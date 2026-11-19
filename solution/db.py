"""SQLite storage layer: schema and connection helpers.

All state lives in one SQLite database file:

  objects        object registry with algebra (generation) and read versions
  baseline       persistent (object, perm) grants
  leases         temporary grants with expiry (valid iff expires_at > t)
  attr_refs      attribute references: object gains perm while the attribute
                 exists and ref_perm is in ref_object's base target
  dependencies   permission DAG edges: (object, perm) requires (req_object, req_perm)
  current_perms  the currently applied permission set (business state)
  plans          generated plans with their recorded read basis
  receipts       plan application receipts (applied AND business-failure stale)
  requests       idempotency ledger: (tenant, request_id) -> first response
  meta           per-tenant id sequences (deterministic plan/receipt ids)
"""

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    tenant        TEXT NOT NULL,
    object_id     TEXT NOT NULL,
    generation    INTEGER NOT NULL,   -- object algebra: bumped by rebuild
    grant_version INTEGER NOT NULL,   -- bumped by baseline/lease/attr_ref changes
    dep_version   INTEGER NOT NULL,   -- bumped by dependency changes on this object
    PRIMARY KEY (tenant, object_id)
);
CREATE TABLE IF NOT EXISTS baseline (
    tenant    TEXT NOT NULL,
    object_id TEXT NOT NULL,
    perm      TEXT NOT NULL,
    PRIMARY KEY (tenant, object_id, perm)
);
CREATE TABLE IF NOT EXISTS leases (
    tenant     TEXT NOT NULL,
    lease_id   TEXT NOT NULL,
    object_id  TEXT NOT NULL,
    perm       TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (tenant, lease_id)
);
CREATE TABLE IF NOT EXISTS attr_refs (
    tenant     TEXT NOT NULL,
    object_id  TEXT NOT NULL,
    attr       TEXT NOT NULL,
    perm       TEXT NOT NULL,
    ref_object TEXT NOT NULL,
    ref_perm   TEXT NOT NULL,
    PRIMARY KEY (tenant, object_id, attr)
);
CREATE TABLE IF NOT EXISTS dependencies (
    tenant     TEXT NOT NULL,
    object_id  TEXT NOT NULL,
    perm       TEXT NOT NULL,
    req_object TEXT NOT NULL,
    req_perm   TEXT NOT NULL,
    PRIMARY KEY (tenant, object_id, perm, req_object, req_perm)
);
CREATE TABLE IF NOT EXISTS current_perms (
    tenant    TEXT NOT NULL,
    object_id TEXT NOT NULL,
    perm      TEXT NOT NULL,
    PRIMARY KEY (tenant, object_id, perm)
);
CREATE TABLE IF NOT EXISTS plans (
    tenant   TEXT NOT NULL,
    plan_id  TEXT NOT NULL,
    at_time  INTEGER NOT NULL,
    status   TEXT NOT NULL,           -- pending | applied | stale
    payload  TEXT NOT NULL,           -- JSON: steps, read_set, lease_refs, explanations
    PRIMARY KEY (tenant, plan_id)
);
CREATE TABLE IF NOT EXISTS receipts (
    tenant     TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    plan_id    TEXT NOT NULL,
    request_id TEXT,
    status     TEXT NOT NULL,         -- applied | stale
    now_time   INTEGER NOT NULL,
    payload    TEXT NOT NULL,
    PRIMARY KEY (tenant, receipt_id)
);
CREATE INDEX IF NOT EXISTS receipts_plan ON receipts (tenant, plan_id);
CREATE TABLE IF NOT EXISTS requests (
    tenant      TEXT NOT NULL,
    request_id  TEXT NOT NULL,
    op          TEXT NOT NULL,
    params_hash TEXT NOT NULL,
    response    TEXT NOT NULL,        -- persisted first result, returned verbatim on replay
    PRIMARY KEY (tenant, request_id)
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


def connect(db_path):
    """Open a connection in autocommit mode; transactions are explicit.

    timeout=30 gives a busy timeout so a second process blocks (instead of
    failing) while another process holds the write lock mid-transaction.
    """
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn):
    conn.executescript(SCHEMA)
