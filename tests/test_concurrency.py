"""Concurrency and crash tests.

Two real subprocesses are coordinated through file-based sync points so the
interleaving is fixed, not racy.  Only child processes started by these tests
are ever terminated.  After a kill, the outcome is judged from the database
state seen through a brand-new connection (i.e. after a "restart").
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

from solution import Coordinator

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
T = "t1"


def wait_for_file(path, timeout=60.0):
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {path}")
        time.sleep(0.02)


def spawn_worker(db, plan_id, request_id, now, sync_dir, result_file, pause):
    cmd = [
        sys.executable, "-m", "solution.apply_worker",
        "--db", db,
        "--tenant", T,
        "--plan-id", plan_id,
        "--request-id", request_id,
        "--now", str(now),
        "--sync-dir", sync_dir,
        "--result-file", result_file,
    ]
    if pause:
        cmd.append("--pause-before-commit")
    return subprocess.Popen(cmd, cwd=REPO_ROOT)


class ConcurrencyTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="permco-conc-")
        self.db = os.path.join(self.tmp, "test.sqlite3")
        self.c = Coordinator(self.db)
        # one object to add, one lease so the plan has real content
        self.c.create_object(T, "A")
        self.c.set_baseline(T, "A", ["read"])
        self.c.add_lease(T, "L1", "A", "write", 100000)
        self.plan = self.c.plan(T, 0)
        self.plan_id = self.plan["plan_id"]
        self.assertEqual(len(self.plan["steps"]), 2)  # add read + add write

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _sync_dir(self, name):
        path = os.path.join(self.tmp, name)
        os.makedirs(path, exist_ok=True)
        return path

    def test_kill_mid_apply_leaves_no_partial_state(self):
        """Terminate the apply process after its writes but before COMMIT.
        After restart: no half-applied plan, no receipt, plan still pending."""
        sync = self._sync_dir("kill")
        result_file = os.path.join(sync, "result.json")
        proc = spawn_worker(self.db, self.plan_id, "rk-1", 0, sync, result_file, pause=True)
        try:
            wait_for_file(os.path.join(sync, "paused"))  # worker is inside the tx
            proc.terminate()  # real termination of our own test child process
            proc.wait(timeout=30)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=30)
        self.assertIsNotNone(proc.returncode)
        self.assertFalse(os.path.exists(result_file))  # worker never finished

        # judge by the database state through a fresh connection ("restart")
        restarted = Coordinator(self.db)
        self.assertEqual(restarted.current(T)["objects"], {})      # no half plan
        self.assertEqual(restarted.receipts(T)["receipts"], [])    # no receipt
        self.assertEqual(restarted.plan_info(T, self.plan_id)["status"], "pending")

        # the same request id can be applied again, exactly once
        r1 = restarted.apply(T, self.plan_id, 0, request_id="rk-1")
        self.assertEqual(r1["status"], "applied")
        r2 = restarted.apply(T, self.plan_id, 0, request_id="rk-1")
        self.assertEqual(r1, r2)  # idempotent replay, no duplicate receipt
        self.assertEqual(len(restarted.receipts(T)["receipts"]), 1)
        self.assertEqual(restarted.current(T)["objects"], {"A": ["read", "write"]})

    def test_two_processes_fixed_interleave_same_request_id(self):
        """Process A pauses inside its transaction holding the write lock;
        process B starts (fixed order via sync point) and blocks on the lock;
        A commits; B must observe A's persisted result instead of re-applying."""
        sync_a = self._sync_dir("procA")
        sync_b = self._sync_dir("procB")
        result_a = os.path.join(sync_a, "result.json")
        result_b = os.path.join(sync_b, "result.json")

        proc_a = spawn_worker(self.db, self.plan_id, "rk-shared", 0, sync_a, result_a, pause=True)
        proc_b = None
        try:
            wait_for_file(os.path.join(sync_a, "paused"))  # A holds the write lock
            proc_b = spawn_worker(
                self.db, self.plan_id, "rk-shared", 0, sync_b, result_b, pause=False
            )
            time.sleep(1.0)  # B is now blocked on the database write lock
            self.assertIsNone(proc_b.poll())  # still waiting, not crashed
            # release A so it commits
            with open(os.path.join(sync_a, "resume"), "w", encoding="utf-8") as fh:
                fh.write("go")
            self.assertEqual(proc_a.wait(timeout=60), 0)
            self.assertEqual(proc_b.wait(timeout=60), 0)
        finally:
            for proc in (proc_a, proc_b):
                if proc is not None and proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=30)

        with open(result_a, encoding="utf-8") as fh:
            out_a = json.load(fh)
        with open(result_b, encoding="utf-8") as fh:
            out_b = json.load(fh)
        self.assertTrue(out_a["ok"])
        self.assertTrue(out_b["ok"])
        # both processes report the SAME persisted receipt
        self.assertEqual(out_a["result"], out_b["result"])
        self.assertEqual(out_a["result"]["status"], "applied")

        restarted = Coordinator(self.db)
        # exactly one receipt, no duplicates
        receipts = restarted.receipts(T)["receipts"]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["receipt_id"], out_a["result"]["receipt_id"])
        # current permissions are exactly the planned ones
        self.assertEqual(restarted.current(T)["objects"], {"A": ["read", "write"]})


if __name__ == "__main__":
    unittest.main()
