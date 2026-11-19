"""Replay a cases.json file against a fresh SQLite database and write result.json."""

import json
import os

from .core import Coordinator
from .errors import AppError, VALIDATION_ERROR


def dispatch(coord, tenant, case):
    op = case.get("op")
    if op == "create_object":
        return coord.create_object(tenant, case["object"])
    if op == "rebuild_object":
        return coord.rebuild_object(tenant, case["object"])
    if op == "set_baseline":
        return coord.set_baseline(tenant, case["object"], case["perms"])
    if op == "add_lease":
        return coord.add_lease(
            tenant, case["lease_id"], case["object"], case["perm"], case["expires_at"]
        )
    if op == "remove_lease":
        return coord.remove_lease(tenant, case["lease_id"])
    if op == "set_attr_ref":
        return coord.set_attr_ref(
            tenant,
            case["object"],
            case["attr"],
            case["perm"],
            case["ref_object"],
            case["ref_perm"],
        )
    if op == "remove_attr_ref":
        return coord.remove_attr_ref(tenant, case["object"], case["attr"])
    if op == "add_dependency":
        return coord.add_dependency(
            tenant, case["object"], case["perm"], case["req_object"], case["req_perm"]
        )
    if op == "remove_dependency":
        return coord.remove_dependency(
            tenant, case["object"], case["perm"], case["req_object"], case["req_perm"]
        )
    if op == "plan":
        return coord.plan(tenant, case["at"], case.get("request_id"))
    if op == "apply":
        return coord.apply(tenant, case["plan_id"], case["now"], case.get("request_id"))
    if op == "current":
        return coord.current(tenant, case.get("object"))
    if op == "target":
        return coord.target(tenant, case["at"])
    if op == "plan_info":
        return coord.plan_info(tenant, case["plan_id"])
    if op == "receipts":
        return coord.receipts(tenant)
    raise AppError(VALIDATION_ERROR, f"unknown op: {op!r}")


def run_cases(input_path, output_path, db_path=None):
    with open(input_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    default_tenant = data.get("tenant", "default")
    db_path = db_path or data.get("db") or (output_path + ".sqlite3")
    if data.get("fresh", True):
        for suffix in ("", "-journal", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
    coord = Coordinator(db_path)

    results = []
    ok_count = 0
    for case in data.get("cases", []):
        tenant = case.get("tenant", default_tenant)
        entry = {"id": case.get("id"), "op": case.get("op")}
        try:
            entry["ok"] = True
            entry["result"] = dispatch(coord, tenant, case)
            ok_count += 1
        except AppError as exc:
            entry["ok"] = False
            entry["error"] = exc.to_dict()
        results.append(entry)

    output = {
        "db": db_path,
        "tenant": default_tenant,
        "summary": {"total": len(results), "ok": ok_count, "failed": len(results) - ok_count},
        "results": results,
    }
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    return output
