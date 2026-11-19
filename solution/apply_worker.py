"""Standalone apply worker used by the concurrency tests.

Runs ``apply`` in its own process so tests can (a) interleave two processes
deterministically via file-based sync points and (b) really terminate the
process mid-transaction and inspect the uncommitted outcome from a fresh
connection.

Sync protocol (files inside --sync-dir):
  * worker creates ``paused`` after all writes, before COMMIT
    (only with --pause-before-commit), then waits for ``resume`` to appear
  * the parent either writes ``resume`` (let it commit) or terminates the
    process (transaction rolls back)

The final apply response is written to --result-file as JSON.
"""

import argparse
import json
import os
import sys
import time


def _wait_for_file(path, timeout=60.0):
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {path}")
        time.sleep(0.02)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="solution.apply_worker")
    parser.add_argument("--db", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--now", required=True)
    parser.add_argument("--sync-dir", required=True)
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--pause-before-commit", action="store_true")
    args = parser.parse_args(argv)

    # imported here so the worker is self-contained when spawned as __main__
    from solution.core import Coordinator
    from solution.errors import AppError

    os.makedirs(args.sync_dir, exist_ok=True)

    def before_commit():
        if args.pause_before_commit:
            with open(os.path.join(args.sync_dir, "paused"), "w", encoding="utf-8") as fh:
                fh.write(str(os.getpid()))
            _wait_for_file(os.path.join(args.sync_dir, "resume"))

    coord = Coordinator(args.db)
    try:
        result = coord.apply(
            args.tenant,
            args.plan_id,
            args.now,
            request_id=args.request_id,
            _before_commit=before_commit,
        )
        payload = {"ok": True, "result": result}
        code = 0
    except AppError as exc:
        payload = {"ok": False, "error": exc.to_dict()}
        code = 0  # business/protocol errors are still a completed run
    except Exception as exc:  # pragma: no cover - defensive
        payload = {"ok": False, "error": {"code": "WORKER_ERROR", "message": str(exc)}}
        code = 1
    with open(args.result_file, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return code


if __name__ == "__main__":
    sys.exit(main())
