"""Local permission revocation coordinator (core domain logic).

Model
-----
* Object algebra: every object carries a ``generation``; rebuilding the object
  bumps it and clears that object's grants, dependencies and applied perms.
* Target permissions at time T = (baseline UNION leases valid at T UNION valid
  attribute references), then pruned by the permission dependency DAG:
  a permission whose prerequisite is not itself retained cannot be retained.
* A lease is valid at T iff ``expires_at > T`` (a lease expiring exactly at T
  is NOT valid at T).
* An attribute reference (obj, attr) grants ``perm`` while the attribute
  exists and ``ref_perm`` is in ``ref_object``'s *base* target (baseline +
  valid leases of the referenced object).  One level, no recursion.
* Every retained permission keeps an itemized list of ALL its sources
  (baseline, each valid lease, each valid attribute reference).

Plan / apply
------------
* ``plan(at)`` computes the target for ``at``, diffs it against the applied
  set and emits ordered steps: revocations first (downstream/dependents before
  upstream/prerequisites), then additions (upstream before downstream).
  A plan adjusts at most MAX_PLAN_OBJECTS objects; overflow is reported via
  ``truncated``/``remaining_objects`` and converges through further plans.
* The plan records its read basis: for every object in the dependency/
  attribute closure of the stepped objects the (generation, grant_version,
  dep_version) triple, plus every lease valid at ``at`` on those objects.
* ``apply(plan_id, now)`` runs in a single IMMEDIATE transaction and
  re-validates the basis at the in-transaction moment: object rebuilt,
  grants/dependencies rewritten, or a relied-upon lease expired/removed
  => the plan is stale.  Stale produces a *stale receipt* and zero business
  changes.  Changes to objects outside the read set do not block apply.

Idempotency
-----------
``request_id`` is unique per tenant.  Replaying the same id with the same
parameters returns the persisted first response verbatim; replaying with
different parameters is rejected with REQUEST_ID_CONFLICT.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from heapq import heappop, heappush

from . import db as dbmod
from .errors import (
    AppError,
    ATTR_REF_NOT_FOUND,
    DEPENDENCY_CYCLE,
    DEPENDENCY_NOT_FOUND,
    DUPLICATE_DEPENDENCY,
    DUPLICATE_LEASE,
    DUPLICATE_OBJECT,
    LEASE_NOT_FOUND,
    OBJECT_NOT_FOUND,
    PLAN_NOT_FOUND,
    PLAN_NOT_PENDING,
    REQUEST_ID_CONFLICT,
    VALIDATION_ERROR,
)

MAX_PLAN_OBJECTS = 50


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def canon(obj):
    """Canonical JSON used for hashing and for stored responses."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj):
    return hashlib.sha256(canon(obj).encode("utf-8")).hexdigest()


def parse_time(value):
    """Accept a logical tick (int) or an ISO-8601 string; return int seconds."""
    if isinstance(value, bool):
        raise AppError(VALIDATION_ERROR, "time must be an int or ISO-8601 string")
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    try:
        return int(s)
    except ValueError:
        pass
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise AppError(VALIDATION_ERROR, f"unparseable time value: {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _require_str(name, value):
    if not isinstance(value, str) or not value:
        raise AppError(VALIDATION_ERROR, f"{name} must be a non-empty string")
    return value


# ---------------------------------------------------------------------------
# coordinator
# ---------------------------------------------------------------------------

class Coordinator:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        conn = dbmod.connect(self.db_path)
        try:
            dbmod.init_db(conn)
        finally:
            conn.close()

    # -- connection / transaction ------------------------------------------
    def _connect(self):
        return dbmod.connect(self.db_path)

    @contextmanager
    def _tx(self):
        """A single IMMEDIATE write transaction (takes the write lock up front
        so validation and mutation are atomic against other processes)."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
        finally:
            conn.close()

    # -- id sequences --------------------------------------------------------
    @staticmethod
    def _next_id(conn, tenant, kind, prefix):
        key = f"{tenant}:{kind}_seq"
        conn.execute("INSERT INTO meta(key, value) VALUES(?, 0) ON CONFLICT(key) DO NOTHING", (key,))
        conn.execute("UPDATE meta SET value = value + 1 WHERE key = ?", (key,))
        value = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()["value"]
        return f"{prefix}{value:06d}"

    # -- idempotency ledger ---------------------------------------------------
    @staticmethod
    def _idem_lookup(conn, tenant, request_id, op, params):
        """Return (stored_response, params_hash).  Raises on conflicting reuse."""
        if not request_id:
            return None, None
        _require_str("request_id", request_id)
        params_hash = _hash({"op": op, "params": params})
        row = conn.execute(
            "SELECT op, params_hash, response FROM requests WHERE tenant = ? AND request_id = ?",
            (tenant, request_id),
        ).fetchone()
        if row is None:
            return None, params_hash
        if row["op"] != op or row["params_hash"] != params_hash:
            raise AppError(
                REQUEST_ID_CONFLICT,
                f"request_id {request_id!r} already used with different parameters",
            )
        return json.loads(row["response"]), params_hash

    @staticmethod
    def _idem_store(conn, tenant, request_id, op, params_hash, response):
        if not request_id:
            return
        conn.execute(
            "INSERT INTO requests(tenant, request_id, op, params_hash, response) VALUES(?,?,?,?,?)",
            (tenant, request_id, op, params_hash, canon(response)),
        )

    # -- object state mutations ------------------------------------------------
    def _require_object(self, conn, tenant, object_id):
        row = conn.execute(
            "SELECT * FROM objects WHERE tenant = ? AND object_id = ?", (tenant, object_id)
        ).fetchone()
        if row is None:
            raise AppError(OBJECT_NOT_FOUND, f"object {object_id!r} does not exist")
        return row

    @staticmethod
    def _bump(conn, tenant, object_id, column):
        conn.execute(
            f"UPDATE objects SET {column} = {column} + 1 WHERE tenant = ? AND object_id = ?",
            (tenant, object_id),
        )

    def create_object(self, tenant, object_id):
        _require_str("object", object_id)
        with self._tx() as conn:
            exists = conn.execute(
                "SELECT 1 FROM objects WHERE tenant = ? AND object_id = ?", (tenant, object_id)
            ).fetchone()
            if exists:
                raise AppError(DUPLICATE_OBJECT, f"object {object_id!r} already exists")
            conn.execute(
                "INSERT INTO objects VALUES(?,?,?,?,?)", (tenant, object_id, 1, 0, 0)
            )
            return {"object": object_id, "generation": 1}

    def rebuild_object(self, tenant, object_id):
        """Rebuild = new object algebra generation; all of the object's own
        grants, dependencies and applied permissions are discarded."""
        _require_str("object", object_id)
        with self._tx() as conn:
            row = self._require_object(conn, tenant, object_id)
            generation = row["generation"] + 1
            for table in ("baseline", "leases", "attr_refs", "dependencies", "current_perms"):
                conn.execute(
                    f"DELETE FROM {table} WHERE tenant = ? AND object_id = ?", (tenant, object_id)
                )
            conn.execute(
                "UPDATE objects SET generation = ?, grant_version = 0, dep_version = 0 "
                "WHERE tenant = ? AND object_id = ?",
                (generation, tenant, object_id),
            )
            return {"object": object_id, "generation": generation}

    def set_baseline(self, tenant, object_id, perms):
        if not isinstance(perms, (list, tuple)) or not all(isinstance(p, str) and p for p in perms):
            raise AppError(VALIDATION_ERROR, "perms must be a list of non-empty strings")
        perms = sorted(set(perms))
        with self._tx() as conn:
            self._require_object(conn, tenant, object_id)
            conn.execute(
                "DELETE FROM baseline WHERE tenant = ? AND object_id = ?", (tenant, object_id)
            )
            for perm in perms:
                conn.execute(
                    "INSERT INTO baseline VALUES(?,?,?)", (tenant, object_id, perm)
                )
            self._bump(conn, tenant, object_id, "grant_version")
            return {"object": object_id, "perms": perms}

    def add_lease(self, tenant, lease_id, object_id, perm, expires_at):
        _require_str("lease_id", lease_id)
        _require_str("perm", perm)
        expires_at = parse_time(expires_at)
        with self._tx() as conn:
            self._require_object(conn, tenant, object_id)
            dup = conn.execute(
                "SELECT 1 FROM leases WHERE tenant = ? AND lease_id = ?", (tenant, lease_id)
            ).fetchone()
            if dup:
                raise AppError(DUPLICATE_LEASE, f"lease {lease_id!r} already exists")
            conn.execute(
                "INSERT INTO leases VALUES(?,?,?,?,?)",
                (tenant, lease_id, object_id, perm, expires_at),
            )
            self._bump(conn, tenant, object_id, "grant_version")
            return {
                "lease_id": lease_id,
                "object": object_id,
                "perm": perm,
                "expires_at": expires_at,
            }

    def remove_lease(self, tenant, lease_id):
        _require_str("lease_id", lease_id)
        with self._tx() as conn:
            row = conn.execute(
                "SELECT object_id FROM leases WHERE tenant = ? AND lease_id = ?",
                (tenant, lease_id),
            ).fetchone()
            if row is None:
                raise AppError(LEASE_NOT_FOUND, f"lease {lease_id!r} does not exist")
            conn.execute(
                "DELETE FROM leases WHERE tenant = ? AND lease_id = ?", (tenant, lease_id)
            )
            self._bump(conn, tenant, row["object_id"], "grant_version")
            return {"removed": lease_id}

    def set_attr_ref(self, tenant, object_id, attr, perm, ref_object, ref_perm):
        _require_str("attr", attr)
        _require_str("perm", perm)
        _require_str("ref_perm", ref_perm)
        with self._tx() as conn:
            self._require_object(conn, tenant, object_id)
            self._require_object(conn, tenant, ref_object)
            conn.execute(
                "INSERT OR REPLACE INTO attr_refs VALUES(?,?,?,?,?,?)",
                (tenant, object_id, attr, perm, ref_object, ref_perm),
            )
            self._bump(conn, tenant, object_id, "grant_version")
            return {
                "object": object_id,
                "attr": attr,
                "perm": perm,
                "ref_object": ref_object,
                "ref_perm": ref_perm,
            }

    def remove_attr_ref(self, tenant, object_id, attr):
        with self._tx() as conn:
            row = conn.execute(
                "SELECT 1 FROM attr_refs WHERE tenant = ? AND object_id = ? AND attr = ?",
                (tenant, object_id, attr),
            ).fetchone()
            if row is None:
                raise AppError(
                    ATTR_REF_NOT_FOUND, f"attribute {attr!r} not found on {object_id!r}"
                )
            conn.execute(
                "DELETE FROM attr_refs WHERE tenant = ? AND object_id = ? AND attr = ?",
                (tenant, object_id, attr),
            )
            self._bump(conn, tenant, object_id, "grant_version")
            return {"removed": attr}

    # -- dependencies ---------------------------------------------------------
    def add_dependency(self, tenant, object_id, perm, req_object, req_perm):
        _require_str("perm", perm)
        _require_str("req_perm", req_perm)
        with self._tx() as conn:
            self._require_object(conn, tenant, object_id)
            self._require_object(conn, tenant, req_object)
            dup = conn.execute(
                "SELECT 1 FROM dependencies WHERE tenant = ? AND object_id = ? AND perm = ? "
                "AND req_object = ? AND req_perm = ?",
                (tenant, object_id, perm, req_object, req_perm),
            ).fetchone()
            if dup:
                raise AppError(DUPLICATE_DEPENDENCY, "dependency already exists")
            if self._creates_cycle(conn, tenant, (object_id, perm), (req_object, req_perm)):
                raise AppError(
                    DEPENDENCY_CYCLE,
                    f"dependency ({object_id},{perm}) -> ({req_object},{req_perm}) "
                    "would close a cycle; dependencies must form a DAG",
                )
            conn.execute(
                "INSERT INTO dependencies VALUES(?,?,?,?,?)",
                (tenant, object_id, perm, req_object, req_perm),
            )
            self._bump(conn, tenant, object_id, "dep_version")
            return {
                "object": object_id,
                "perm": perm,
                "req_object": req_object,
                "req_perm": req_perm,
            }

    def remove_dependency(self, tenant, object_id, perm, req_object, req_perm):
        with self._tx() as conn:
            row = conn.execute(
                "SELECT 1 FROM dependencies WHERE tenant = ? AND object_id = ? AND perm = ? "
                "AND req_object = ? AND req_perm = ?",
                (tenant, object_id, perm, req_object, req_perm),
            ).fetchone()
            if row is None:
                raise AppError(DEPENDENCY_NOT_FOUND, "dependency does not exist")
            conn.execute(
                "DELETE FROM dependencies WHERE tenant = ? AND object_id = ? AND perm = ? "
                "AND req_object = ? AND req_perm = ?",
                (tenant, object_id, perm, req_object, req_perm),
            )
            self._bump(conn, tenant, object_id, "dep_version")
            return {"removed": [object_id, perm, req_object, req_perm]}

    @staticmethod
    def _creates_cycle(conn, tenant, src, dst):
        """Would adding edge src->dst (src requires dst) close a cycle?"""
        if src == dst:
            return True
        adj = defaultdict(set)
        for r in conn.execute(
            "SELECT object_id, perm, req_object, req_perm FROM dependencies WHERE tenant = ?",
            (tenant,),
        ):
            adj[(r["object_id"], r["perm"])].add((r["req_object"], r["req_perm"]))
        seen, stack = set(), [dst]
        while stack:
            node = stack.pop()
            if node == src:
                return True
            if node in seen:
                continue
            seen.add(node)
            stack.extend(adj.get(node, ()))
        return False

    # -- target computation -----------------------------------------------------
    def _compute(self, conn, tenant, at):
        """Compute target permissions at time ``at`` with itemized sources.

        Returns a dict with: objects, leases, deps, current, sources, dropped,
        prereqs, topo, kept, explanations.
        """
        objects = {
            r["object_id"]: dict(r)
            for r in conn.execute("SELECT * FROM objects WHERE tenant = ?", (tenant,))
        }
        baseline = defaultdict(set)
        for r in conn.execute("SELECT object_id, perm FROM baseline WHERE tenant = ?", (tenant,)):
            baseline[r["object_id"]].add(r["perm"])
        leases = [
            dict(r) for r in conn.execute("SELECT * FROM leases WHERE tenant = ?", (tenant,))
        ]
        attrs = [
            dict(r) for r in conn.execute("SELECT * FROM attr_refs WHERE tenant = ?", (tenant,))
        ]
        deps = [
            dict(r)
            for r in conn.execute("SELECT * FROM dependencies WHERE tenant = ?", (tenant,))
        ]
        current = defaultdict(set)
        for r in conn.execute(
            "SELECT object_id, perm FROM current_perms WHERE tenant = ?", (tenant,)
        ):
            current[r["object_id"]].add(r["perm"])

        # base target (baseline + valid leases) per object, used by attr refs
        base = defaultdict(set)
        for obj, perms in baseline.items():
            base[obj] |= perms
        for lease in leases:
            if lease["expires_at"] > at:
                base[lease["object_id"]].add(lease["perm"])

        # itemized grant sources
        sources = defaultdict(lambda: defaultdict(list))   # obj -> perm -> [source, ...]
        dropped = defaultdict(lambda: defaultdict(list))   # obj -> perm -> [invalid source, ...]
        for obj in baseline:
            for perm in baseline[obj]:
                sources[obj][perm].append({"type": "baseline"})
        for lease in leases:
            entry = {
                "type": "lease",
                "lease_id": lease["lease_id"],
                "expires_at": lease["expires_at"],
            }
            if lease["expires_at"] > at:
                sources[lease["object_id"]][lease["perm"]].append(entry)
            else:
                dropped[lease["object_id"]][lease["perm"]].append({**entry, "reason": "expired"})
        for a in attrs:
            entry = {
                "type": "attr_ref",
                "attr": a["attr"],
                "ref_object": a["ref_object"],
                "ref_perm": a["ref_perm"],
            }
            ref_ok = a["ref_object"] in objects and a["ref_perm"] in base.get(
                a["ref_object"], set()
            )
            if ref_ok:
                sources[a["object_id"]][a["perm"]].append(entry)
            else:
                reason = (
                    "ref_object_missing"
                    if a["ref_object"] not in objects
                    else "ref_perm_not_in_ref_base_target"
                )
                dropped[a["object_id"]][a["perm"]].append({**entry, "reason": reason})

        # dependency DAG over (object, perm) nodes
        prereqs = defaultdict(set)
        for d in deps:
            prereqs[(d["object_id"], d["perm"])].add((d["req_object"], d["req_perm"]))

        nodes = set()
        for obj, perms in sources.items():
            for perm in perms:
                nodes.add((obj, perm))
        for node, reqs in prereqs.items():
            nodes.add(node)
            nodes |= reqs

        # deterministic topological order: prerequisites first
        indegree = {n: 0 for n in nodes}
        followers = defaultdict(list)
        for node, reqs in prereqs.items():
            for req in reqs:
                followers[req].append(node)
                indegree[node] += 1
        heap = [n for n in nodes if indegree[n] == 0]
        heap.sort()
        topo = []
        while heap:
            node = heappop(heap)
            topo.append(node)
            for follower in followers.get(node, ()):
                indegree[follower] -= 1
                if indegree[follower] == 0:
                    heappush(heap, follower)

        def is_granted(node):
            return node[1] in sources.get(node[0], {})

        kept = {}
        for node in topo:
            kept[node] = is_granted(node) and all(
                kept.get(req, False) for req in prereqs.get(node, ())
            )
        # nodes not in topo (defensive: cycle) stay untended -> kept False via .get

        # explanations: one entry per (object, perm) ever granted or applied
        keys = set()
        for obj, perms in sources.items():
            keys |= {(obj, p) for p in perms}
        for obj, perms in dropped.items():
            keys |= {(obj, p) for p in perms}
        for obj, perms in current.items():
            keys |= {(obj, p) for p in perms}

        explanations = []
        for obj, perm in sorted(keys):
            srcs = sources.get(obj, {}).get(perm, [])
            drp = dropped.get(obj, {}).get(perm, [])
            missing = [
                {"req_object": r[0], "req_perm": r[1]}
                for r in sorted(prereqs.get((obj, perm), ()))
                if not kept.get(r, False)
            ]
            is_kept = kept.get((obj, perm), False)
            in_current = perm in current.get(obj, set())
            if is_kept and in_current:
                action = "keep"
            elif is_kept:
                action = "add"
            elif in_current:
                action = "revoke"
            else:
                action = "none"
            entry = {
                "object": obj,
                "perm": perm,
                "kept": is_kept,
                "in_current": in_current,
                "action": action,
                "sources": srcs,
            }
            if drp:
                entry["dropped_sources"] = drp
            if srcs and missing:
                entry["missing_prerequisites"] = missing
            explanations.append(entry)

        return {
            "objects": objects,
            "leases": leases,
            "deps": deps,
            "attrs": attrs,
            "current": current,
            "sources": sources,
            "dropped": dropped,
            "prereqs": prereqs,
            "topo": topo,
            "kept": kept,
            "explanations": explanations,
        }

    # -- plan -------------------------------------------------------------------
    def plan(self, tenant, at, request_id=None):
        at = parse_time(at)
        with self._tx() as conn:
            stored, params_hash = self._idem_lookup(conn, tenant, request_id, "plan", {"at": at})
            if stored is not None:
                return stored
            payload = self._build_plan(conn, tenant, at)
            plan_id = self._next_id(conn, tenant, "plan", "P")
            conn.execute(
                "INSERT INTO plans(tenant, plan_id, at_time, status, payload) VALUES(?,?,?,?,?)",
                (tenant, plan_id, at, "pending", canon(payload)),
            )
            response = {"plan_id": plan_id, "status": "pending", **payload}
            self._idem_store(conn, tenant, request_id, "plan", params_hash, response)
            return response

    def _build_plan(self, conn, tenant, at):
        comp = self._compute(conn, tenant, at)
        kept, current, topo = comp["kept"], comp["current"], comp["topo"]

        # revocations: downstream (dependents) before upstream (prerequisites)
        revoke_nodes = [
            n for n in topo if n[1] in current.get(n[0], set()) and not kept.get(n, False)
        ]
        revoke_nodes.reverse()
        # additions: upstream (prerequisites) before downstream (dependents)
        add_nodes = [
            n for n in topo if kept.get(n, False) and n[1] not in current.get(n[0], set())
        ]
        ordered = [("revoke",) + n for n in revoke_nodes] + [("add",) + n for n in add_nodes]

        # a single plan adjusts at most MAX_PLAN_OBJECTS objects
        steps, plan_objects, truncated = [], set(), False
        for action, obj, perm in ordered:
            if obj not in plan_objects:
                if len(plan_objects) >= MAX_PLAN_OBJECTS:
                    truncated = True
                    continue
                plan_objects.add(obj)
            steps.append(
                {"seq": len(steps) + 1, "action": action, "object": obj, "perm": perm}
            )
        all_objects = {obj for _, obj, _ in ordered}
        remaining = len(all_objects - plan_objects)

        # read basis: dependency/attribute closure of the stepped objects
        dep_neighbors = defaultdict(set)
        for d in comp["deps"]:
            dep_neighbors[d["object_id"]].add(d["req_object"])
        for a in comp["attrs"]:
            dep_neighbors[a["object_id"]].add(a["ref_object"])
        closure = set(plan_objects)
        stack = list(plan_objects)
        while stack:
            obj = stack.pop()
            for neighbor in dep_neighbors.get(obj, ()):
                if neighbor not in closure:
                    closure.add(neighbor)
                    stack.append(neighbor)

        objects = comp["objects"]
        read_set = {}
        for obj in sorted(closure):
            row = objects.get(obj)
            if row is None:
                read_set[obj] = {"missing": True}
            else:
                read_set[obj] = {
                    "generation": row["generation"],
                    "grant_version": row["grant_version"],
                    "dep_version": row["dep_version"],
                }
        lease_refs = {
            lease["lease_id"]: lease["expires_at"]
            for lease in comp["leases"]
            if lease["object_id"] in closure and lease["expires_at"] > at
        }

        return {
            "at": at,
            "steps": steps,
            "objects_touched": len(plan_objects),
            "truncated": truncated,
            "remaining_objects": remaining,
            "read_set": read_set,
            "lease_refs": lease_refs,
            "explanations": comp["explanations"],
        }

    # -- apply --------------------------------------------------------------------
    def apply(self, tenant, plan_id, now, request_id=None, _before_commit=None):
        """Apply a pending plan atomically.  ``_before_commit`` is a test hook
        invoked inside the transaction after all writes, before COMMIT."""
        _require_str("plan_id", plan_id)
        now = parse_time(now)
        with self._tx() as conn:
            stored, params_hash = self._idem_lookup(
                conn, tenant, request_id, "apply", {"plan_id": plan_id, "now": now}
            )
            if stored is not None:
                return stored
            row = conn.execute(
                "SELECT * FROM plans WHERE tenant = ? AND plan_id = ?", (tenant, plan_id)
            ).fetchone()
            if row is None:
                raise AppError(PLAN_NOT_FOUND, f"plan {plan_id!r} does not exist")
            if row["status"] != "pending":
                raise AppError(
                    PLAN_NOT_PENDING,
                    f"plan {plan_id!r} is {row['status']}, not pending",
                )
            payload = json.loads(row["payload"])
            stale = self._revalidate(conn, tenant, payload, now)
            receipt_id = self._next_id(conn, tenant, "receipt", "R")

            if stale:
                receipt = {
                    "receipt_id": receipt_id,
                    "plan_id": plan_id,
                    "plan_at": payload["at"],
                    "status": "stale",
                    "now": now,
                    "stale_reasons": stale,
                    "steps_applied": [],
                }
                conn.execute(
                    "UPDATE plans SET status = 'stale' WHERE tenant = ? AND plan_id = ?",
                    (tenant, plan_id),
                )
                self._insert_receipt(conn, tenant, receipt, request_id)
                self._idem_store(conn, tenant, request_id, "apply", params_hash, receipt)
                return receipt

            for step in payload["steps"]:
                if step["action"] == "revoke":
                    conn.execute(
                        "DELETE FROM current_perms WHERE tenant = ? AND object_id = ? AND perm = ?",
                        (tenant, step["object"], step["perm"]),
                    )
                else:
                    conn.execute(
                        "INSERT OR IGNORE INTO current_perms VALUES(?,?,?)",
                        (tenant, step["object"], step["perm"]),
                    )
            conn.execute(
                "UPDATE plans SET status = 'applied' WHERE tenant = ? AND plan_id = ?",
                (tenant, plan_id),
            )
            receipt = {
                "receipt_id": receipt_id,
                "plan_id": plan_id,
                "plan_at": payload["at"],
                "status": "applied",
                "now": now,
                "stale_reasons": [],
                "steps_applied": payload["steps"],
            }
            self._insert_receipt(conn, tenant, receipt, request_id)
            self._idem_store(conn, tenant, request_id, "apply", params_hash, receipt)
            if _before_commit is not None:
                _before_commit()
            return receipt

    @staticmethod
    def _insert_receipt(conn, tenant, receipt, request_id):
        conn.execute(
            "INSERT INTO receipts(tenant, receipt_id, plan_id, request_id, status, now_time, payload)"
            " VALUES(?,?,?,?,?,?,?)",
            (
                tenant,
                receipt["receipt_id"],
                receipt["plan_id"],
                request_id,
                receipt["status"],
                receipt["now"],
                canon(receipt),
            ),
        )

    def _revalidate(self, conn, tenant, payload, now):
        """Re-check the recorded planning basis at the in-transaction moment."""
        stale = []
        objects = {
            r["object_id"]: dict(r)
            for r in conn.execute("SELECT * FROM objects WHERE tenant = ?", (tenant,))
        }
        for obj, recorded in payload["read_set"].items():
            current = objects.get(obj)
            if recorded.get("missing"):
                if current is not None:
                    stale.append({"object": obj, "reason": "object_created_after_plan"})
                continue
            if current is None:
                stale.append({"object": obj, "reason": "object_missing"})
                continue
            if current["generation"] != recorded["generation"]:
                stale.append(
                    {
                        "object": obj,
                        "reason": "object_rebuilt",
                        "planned_generation": recorded["generation"],
                        "current_generation": current["generation"],
                    }
                )
            elif current["grant_version"] != recorded["grant_version"]:
                stale.append(
                    {
                        "object": obj,
                        "reason": "grants_changed",
                        "planned_grant_version": recorded["grant_version"],
                        "current_grant_version": current["grant_version"],
                    }
                )
            elif current["dep_version"] != recorded["dep_version"]:
                stale.append(
                    {
                        "object": obj,
                        "reason": "dependencies_changed",
                        "planned_dep_version": recorded["dep_version"],
                        "current_dep_version": current["dep_version"],
                    }
                )
        for lease_id, expires_at in payload["lease_refs"].items():
            row = conn.execute(
                "SELECT expires_at FROM leases WHERE tenant = ? AND lease_id = ?",
                (tenant, lease_id),
            ).fetchone()
            if row is None:
                stale.append({"lease_id": lease_id, "reason": "lease_removed"})
            elif row["expires_at"] != expires_at:
                stale.append({"lease_id": lease_id, "reason": "lease_modified"})
            elif row["expires_at"] <= now:
                stale.append(
                    {
                        "lease_id": lease_id,
                        "reason": "lease_expired",
                        "expires_at": row["expires_at"],
                        "now": now,
                    }
                )
        return stale

    # -- read-only queries ---------------------------------------------------------
    def current(self, tenant, object_id=None):
        conn = self._connect()
        try:
            if object_id is not None:
                rows = conn.execute(
                    "SELECT perm FROM current_perms WHERE tenant = ? AND object_id = ? "
                    "ORDER BY perm",
                    (tenant, object_id),
                ).fetchall()
                return {"object": object_id, "perms": [r["perm"] for r in rows]}
            result = defaultdict(list)
            for r in conn.execute(
                "SELECT object_id, perm FROM current_perms WHERE tenant = ? "
                "ORDER BY object_id, perm",
                (tenant,),
            ):
                result[r["object_id"]].append(r["perm"])
            return {"objects": {k: v for k, v in sorted(result.items())}}
        finally:
            conn.close()

    def target(self, tenant, at):
        """Compute (but do not persist) the target permission set at ``at``."""
        at = parse_time(at)
        conn = self._connect()
        try:
            comp = self._compute(conn, tenant, at)
        finally:
            conn.close()
        target = {}
        for obj, perms in comp["sources"].items():
            kept_perms = {
                p: srcs for p, srcs in perms.items() if comp["kept"].get((obj, p), False)
            }
            if kept_perms:
                target[obj] = kept_perms
        return {"at": at, "target": target, "explanations": comp["explanations"]}

    def plan_info(self, tenant, plan_id):
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM plans WHERE tenant = ? AND plan_id = ?", (tenant, plan_id)
            ).fetchone()
            if row is None:
                raise AppError(PLAN_NOT_FOUND, f"plan {plan_id!r} does not exist")
            payload = json.loads(row["payload"])
            return {
                "plan_id": plan_id,
                "status": row["status"],
                "objects_touched": payload["objects_touched"],
                **payload,
            }
        finally:
            conn.close()

    def receipts(self, tenant):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT payload FROM receipts WHERE tenant = ? ORDER BY receipt_id", (tenant,)
            ).fetchall()
            return {"receipts": [json.loads(r["payload"]) for r in rows]}
        finally:
            conn.close()
