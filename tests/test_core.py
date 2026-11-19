"""Core domain tests.  All expected values are derived by hand from the spec,
not from the implementation under test."""

import shutil
import tempfile
import unittest

from solution import Coordinator, AppError
from solution.core import MAX_PLAN_OBJECTS

T = "t1"


def sources_of(target_result, obj, perm):
    for entry in target_result["explanations"]:
        if entry["object"] == obj and entry["perm"] == perm:
            return entry
    return None


class CoordinatorTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="permco-")
        self.db = f"{self.tmp}/test.sqlite3"
        self.c = Coordinator(self.db)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestTargetComputation(CoordinatorTestCase):
    def test_baseline_and_lease_union_with_itemized_sources(self):
        self.c.create_object(T, "A")
        self.c.set_baseline(T, "A", ["read"])
        self.c.add_lease(T, "L1", "A", "read", 1000)   # overlaps baseline
        self.c.add_lease(T, "L2", "A", "write", 500)

        result = self.c.target(T, 100)
        # target = baseline UNION valid leases
        self.assertEqual(sorted(result["target"]["A"].keys()), ["read", "write"])
        # 'read' has TWO sources and both must be itemized
        read_entry = sources_of(result, "A", "read")
        self.assertTrue(read_entry["kept"])
        self.assertEqual(len(read_entry["sources"]), 2)
        self.assertEqual(
            {s["type"] for s in read_entry["sources"]}, {"baseline", "lease"}
        )
        lease_src = [s for s in read_entry["sources"] if s["type"] == "lease"][0]
        self.assertEqual(lease_src["lease_id"], "L1")
        self.assertEqual(lease_src["expires_at"], 1000)

    def test_exact_expiry_boundary(self):
        self.c.create_object(T, "A")
        self.c.add_lease(T, "L1", "A", "read", 1000)
        # valid strictly before expiry
        self.assertIn("read", self.c.target(T, 999)["target"].get("A", {}))
        # a lease expiring exactly at T is NOT valid at T
        result = self.c.target(T, 1000)
        self.assertNotIn("A", result["target"])
        entry = sources_of(result, "A", "read")
        self.assertFalse(entry["kept"])
        self.assertEqual(entry["sources"], [])
        self.assertEqual(entry["dropped_sources"][0]["reason"], "expired")
        self.assertEqual(entry["dropped_sources"][0]["lease_id"], "L1")

    def test_persistent_and_temporary_sources_overlap(self):
        # baseline + lease grant the same perm; when the lease expires the
        # permission is retained through the baseline source alone.
        self.c.create_object(T, "A")
        self.c.set_baseline(T, "A", ["read"])
        self.c.add_lease(T, "L1", "A", "read", 1000)
        before = sources_of(self.c.target(T, 500), "A", "read")
        self.assertEqual(len(before["sources"]), 2)
        after = sources_of(self.c.target(T, 1000), "A", "read")
        self.assertTrue(after["kept"])
        self.assertEqual([s["type"] for s in after["sources"]], ["baseline"])
        self.assertEqual(after["dropped_sources"][0]["lease_id"], "L1")

    def test_multiple_sources_itemized(self):
        self.c.create_object(T, "A")
        self.c.create_object(T, "B")
        self.c.set_baseline(T, "B", ["read"])
        self.c.set_baseline(T, "A", ["read"])
        self.c.add_lease(T, "L1", "A", "read", 900)
        self.c.add_lease(T, "L2", "A", "read", 1900)
        self.c.set_attr_ref(T, "A", "via_b", "read", "B", "read")
        entry = sources_of(self.c.target(T, 100), "A", "read")
        self.assertEqual(len(entry["sources"]), 4)  # baseline + 2 leases + attr ref
        self.assertEqual(
            [s["type"] for s in entry["sources"]],
            ["baseline", "lease", "lease", "attr_ref"],
        )

    def test_attr_ref_validity_follows_referenced_object(self):
        self.c.create_object(T, "A")
        self.c.create_object(T, "C")
        self.c.set_baseline(T, "A", ["read"])
        self.c.set_attr_ref(T, "C", "via_a", "read", "A", "read")
        entry = sources_of(self.c.target(T, 0), "C", "read")
        self.assertTrue(entry["kept"])
        self.assertEqual(entry["sources"][0]["type"], "attr_ref")
        # remove the referenced baseline perm -> the attribute source goes invalid
        self.c.set_baseline(T, "A", [])
        entry = sources_of(self.c.target(T, 0), "C", "read")
        self.assertFalse(entry["kept"])
        self.assertEqual(entry["sources"], [])
        self.assertEqual(
            entry["dropped_sources"][0]["reason"], "ref_perm_not_in_ref_base_target"
        )

    def test_dependency_pruning_and_missing_prerequisite_explanation(self):
        self.c.create_object(T, "A")
        self.c.create_object(T, "B")
        self.c.set_baseline(T, "A", ["read"])
        self.c.set_baseline(T, "B", ["read"])
        self.c.add_dependency(T, "B", "read", "A", "read")
        self.assertTrue(sources_of(self.c.target(T, 0), "B", "read")["kept"])
        # prerequisite disappears -> dependent permission cannot be retained
        self.c.set_baseline(T, "A", [])
        entry = sources_of(self.c.target(T, 0), "B", "read")
        self.assertFalse(entry["kept"])
        self.assertEqual(
            entry["missing_prerequisites"], [{"req_object": "A", "req_perm": "read"}]
        )

    def test_dependency_cycle_rejected(self):
        self.c.create_object(T, "A")
        self.c.create_object(T, "B")
        self.c.add_dependency(T, "A", "x", "B", "y")
        with self.assertRaises(AppError) as ctx:
            self.c.add_dependency(T, "B", "y", "A", "x")
        self.assertEqual(ctx.exception.code, "DEPENDENCY_CYCLE")
        with self.assertRaises(AppError) as ctx:
            self.c.add_dependency(T, "A", "x", "A", "x")
        self.assertEqual(ctx.exception.code, "DEPENDENCY_CYCLE")


class TestPlanApplyOrdering(CoordinatorTestCase):
    def _build_chain(self):
        # C.read depends on B.read depends on A.read (cross-object chain)
        for obj in ("A", "B", "C"):
            self.c.create_object(T, obj)
            self.c.set_baseline(T, obj, ["read"])
        self.c.add_dependency(T, "B", "read", "A", "read")
        self.c.add_dependency(T, "C", "read", "B", "read")
        plan = self.c.plan(T, 0)
        self.c.apply(T, plan["plan_id"], 0)
        self.assertEqual(
            self.c.current(T)["objects"],
            {"A": ["read"], "B": ["read"], "C": ["read"]},
        )

    def test_revocation_order_downstream_before_upstream(self):
        self._build_chain()
        for obj in ("A", "B", "C"):
            self.c.set_baseline(T, obj, [])
        plan = self.c.plan(T, 1)
        # cross-object ordering evidence: C (most downstream) revoked first
        self.assertEqual(
            [(s["action"], s["object"], s["perm"]) for s in plan["steps"]],
            [("revoke", "C", "read"), ("revoke", "B", "read"), ("revoke", "A", "read")],
        )
        self.c.apply(T, plan["plan_id"], 1)
        self.assertEqual(self.c.current(T)["objects"], {})

    def test_addition_order_upstream_before_downstream(self):
        for obj in ("A", "B", "C"):
            self.c.create_object(T, obj)
            self.c.set_baseline(T, obj, ["read"])
        self.c.add_dependency(T, "B", "read", "A", "read")
        self.c.add_dependency(T, "C", "read", "B", "read")
        plan = self.c.plan(T, 0)
        self.assertEqual(
            [(s["action"], s["object"], s["perm"]) for s in plan["steps"]],
            [("add", "A", "read"), ("add", "B", "read"), ("add", "C", "read")],
        )

    def test_partial_revocation_keeps_upstream_order(self):
        # only A loses its grant: the whole chain must be revoked downstream-first
        self._build_chain()
        self.c.set_baseline(T, "A", [])
        plan = self.c.plan(T, 1)
        self.assertEqual(
            [(s["action"], s["object"]) for s in plan["steps"]],
            [("revoke", "C"), ("revoke", "B"), ("revoke", "A")],
        )


class TestStaleness(CoordinatorTestCase):
    def _plan_one_add(self):
        self.c.create_object(T, "A")
        self.c.set_baseline(T, "A", ["read"])
        return self.c.plan(T, 0)

    def test_object_rebuild_makes_plan_stale(self):
        plan = self._plan_one_add()
        self.c.rebuild_object(T, "A")
        receipt = self.c.apply(T, plan["plan_id"], 1)
        self.assertEqual(receipt["status"], "stale")
        self.assertEqual(receipt["stale_reasons"][0]["reason"], "object_rebuilt")
        # business state untouched
        self.assertEqual(self.c.current(T)["objects"], {})
        # stale receipt persisted and distinguishable from business state
        receipts = self.c.receipts(T)["receipts"]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["status"], "stale")
        self.assertEqual(receipts[0]["steps_applied"], [])
        self.assertEqual(self.c.plan_info(T, plan["plan_id"])["status"], "stale")

    def test_dependency_rewrite_makes_plan_stale(self):
        self.c.create_object(T, "A")
        self.c.create_object(T, "B")
        self.c.set_baseline(T, "A", ["read"])
        self.c.set_baseline(T, "B", ["read"])
        plan = self.c.plan(T, 0)  # adds A.read and B.read; closure includes both
        self.c.add_dependency(T, "B", "read", "A", "read")  # rewrites B's deps
        receipt = self.c.apply(T, plan["plan_id"], 0)
        self.assertEqual(receipt["status"], "stale")
        self.assertEqual(receipt["stale_reasons"][0]["reason"], "dependencies_changed")
        self.assertEqual(self.c.current(T)["objects"], {})

    def test_grant_change_makes_plan_stale(self):
        plan = self._plan_one_add()
        self.c.add_lease(T, "L1", "A", "write", 1000)
        receipt = self.c.apply(T, plan["plan_id"], 0)
        self.assertEqual(receipt["status"], "stale")
        self.assertEqual(receipt["stale_reasons"][0]["reason"], "grants_changed")

    def test_lease_expired_by_apply_time_makes_plan_stale(self):
        self.c.create_object(T, "A")
        self.c.add_lease(T, "L1", "A", "read", 1000)
        plan = self.c.plan(T, 500)  # relies on L1 (valid at 500)
        # applying at the exact expiry tick: L1 is no longer valid -> stale
        receipt = self.c.apply(T, plan["plan_id"], 1000)
        self.assertEqual(receipt["status"], "stale")
        self.assertEqual(receipt["stale_reasons"][0]["reason"], "lease_expired")
        self.assertEqual(receipt["stale_reasons"][0]["lease_id"], "L1")
        self.assertEqual(self.c.current(T)["objects"], {})

    def test_lease_valid_one_tick_before_expiry_applies(self):
        self.c.create_object(T, "A")
        self.c.add_lease(T, "L1", "A", "read", 1000)
        plan = self.c.plan(T, 500)
        receipt = self.c.apply(T, plan["plan_id"], 999)  # one tick before expiry
        self.assertEqual(receipt["status"], "applied")
        self.assertEqual(self.c.current(T)["objects"], {"A": ["read"]})

    def test_lease_removed_makes_plan_stale(self):
        self.c.create_object(T, "A")
        self.c.add_lease(T, "L1", "A", "read", 1000)
        plan = self.c.plan(T, 500)
        self.c.remove_lease(T, "L1")
        receipt = self.c.apply(T, plan["plan_id"], 600)
        self.assertEqual(receipt["status"], "stale")
        reasons = {r["reason"] for r in receipt["stale_reasons"]}
        self.assertIn("lease_removed", reasons)

    def test_unrelated_object_change_does_not_block_apply(self):
        for obj in ("A", "Z"):
            self.c.create_object(T, obj)
            self.c.set_baseline(T, obj, ["read"])
        plan1 = self.c.plan(T, 0)
        self.c.apply(T, plan1["plan_id"], 0)  # both A.read and Z.read applied
        # plan2 only touches A (revoke A.read); Z is outside its read set
        self.c.set_baseline(T, "A", [])
        plan2 = self.c.plan(T, 1)
        self.assertEqual(plan2["objects_touched"], 1)
        self.assertNotIn("Z", plan2["read_set"])
        # unrelated change on Z between plan and apply
        self.c.set_baseline(T, "Z", ["read", "write"])
        receipt = self.c.apply(T, plan2["plan_id"], 2)
        self.assertEqual(receipt["status"], "applied")
        # A.read revoked; Z untouched by this plan (converges via a later plan)
        self.assertEqual(self.c.current(T)["objects"], {"Z": ["read"]})

    def test_stale_apply_is_atomic_and_plan_consumed(self):
        plan = self._plan_one_add()
        self.c.rebuild_object(T, "A")
        self.c.apply(T, plan["plan_id"], 1)
        with self.assertRaises(AppError) as ctx:
            self.c.apply(T, plan["plan_id"], 2)
        self.assertEqual(ctx.exception.code, "PLAN_NOT_PENDING")
        self.assertEqual(len(self.c.receipts(T)["receipts"]), 1)


class TestIdempotency(CoordinatorTestCase):
    def test_same_request_id_same_params_returns_first_result(self):
        self.c.create_object(T, "A")
        self.c.set_baseline(T, "A", ["read"])
        p1 = self.c.plan(T, 100, request_id="rq-1")
        p2 = self.c.plan(T, 100, request_id="rq-1")
        self.assertEqual(p1, p2)  # persisted first result, verbatim
        r1 = self.c.apply(T, p1["plan_id"], 200, request_id="rq-2")
        r2 = self.c.apply(T, p1["plan_id"], 200, request_id="rq-2")
        self.assertEqual(r1, r2)
        # no duplicate receipts, no duplicate plans
        self.assertEqual(len(self.c.receipts(T)["receipts"]), 1)
        self.assertEqual(self.c.current(T)["objects"], {"A": ["read"]})

    def test_same_request_id_different_params_rejected(self):
        self.c.create_object(T, "A")
        self.c.plan(T, 100, request_id="rq-1")
        with self.assertRaises(AppError) as ctx:
            self.c.plan(T, 200, request_id="rq-1")
        self.assertEqual(ctx.exception.code, "REQUEST_ID_CONFLICT")

    def test_request_ids_are_per_tenant(self):
        for tenant in ("t1", "t2"):
            self.c.create_object(tenant, "A")
        # same request id in two tenants must not clash, even with different params
        p1 = self.c.plan("t1", 100, request_id="rq-shared")
        p2 = self.c.plan("t2", 200, request_id="rq-shared")
        self.assertEqual(p1["at"], 100)
        self.assertEqual(p2["at"], 200)
        # replaying within each tenant returns that tenant's persisted first result
        self.assertEqual(self.c.plan("t1", 100, request_id="rq-shared"), p1)
        self.assertEqual(self.c.plan("t2", 200, request_id="rq-shared"), p2)

    def test_stale_result_is_also_idempotent(self):
        self.c.create_object(T, "A")
        self.c.set_baseline(T, "A", ["read"])
        plan = self.c.plan(T, 0)
        self.c.rebuild_object(T, "A")
        r1 = self.c.apply(T, plan["plan_id"], 1, request_id="rq-stale")
        r2 = self.c.apply(T, plan["plan_id"], 1, request_id="rq-stale")
        self.assertEqual(r1["status"], "stale")
        self.assertEqual(r1, r2)
        self.assertEqual(len(self.c.receipts(T)["receipts"]), 1)


class TestPlanSizeCap(CoordinatorTestCase):
    def test_plan_adjusts_at_most_50_objects(self):
        for i in range(60):
            obj = f"o{i:02d}"
            self.c.create_object(T, obj)
            self.c.set_baseline(T, obj, ["read"])
        plan = self.c.plan(T, 0)
        self.assertEqual(plan["objects_touched"], MAX_PLAN_OBJECTS)
        self.assertTrue(plan["truncated"])
        self.assertEqual(plan["remaining_objects"], 10)
        self.assertEqual(len(plan["steps"]), 50)
        # deterministic: the first 50 objects in sorted order
        self.assertEqual(
            sorted({s["object"] for s in plan["steps"]}),
            [f"o{i:02d}" for i in range(50)],
        )
        self.c.apply(T, plan["plan_id"], 0)
        # second plan converges the remainder
        plan2 = self.c.plan(T, 0)
        self.assertEqual(plan2["objects_touched"], 10)
        self.assertFalse(plan2["truncated"])
        self.assertEqual(plan2["remaining_objects"], 0)
        self.c.apply(T, plan2["plan_id"], 0)
        current = self.c.current(T)["objects"]
        self.assertEqual(len(current), 60)
        self.assertTrue(all(perms == ["read"] for perms in current.values()))


class TestValidation(CoordinatorTestCase):
    def test_unknown_object(self):
        with self.assertRaises(AppError) as ctx:
            self.c.set_baseline(T, "nope", ["read"])
        self.assertEqual(ctx.exception.code, "OBJECT_NOT_FOUND")

    def test_duplicate_object(self):
        self.c.create_object(T, "A")
        with self.assertRaises(AppError) as ctx:
            self.c.create_object(T, "A")
        self.assertEqual(ctx.exception.code, "DUPLICATE_OBJECT")

    def test_unknown_plan(self):
        with self.assertRaises(AppError) as ctx:
            self.c.apply(T, "P999999", 0)
        self.assertEqual(ctx.exception.code, "PLAN_NOT_FOUND")

    def test_iso_time_strings_accepted(self):
        self.c.create_object(T, "A")
        self.c.add_lease(T, "L1", "A", "read", "2026-01-01T00:00:10Z")
        result = self.c.target(T, "2026-01-01T00:00:05Z")
        self.assertIn("read", result["target"].get("A", {}))
        result = self.c.target(T, "2026-01-01T00:00:10Z")
        self.assertNotIn("A", result["target"])


if __name__ == "__main__":
    unittest.main()
