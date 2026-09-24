"""Durable, idempotent asynchronous audit jobs.

Field engineers reviewing large calibration subnets submit an audit once
under a stable ``operation_id`` and then poll for the verdict, so a dropped
connection never re-triggers the same expensive solve:

* ``POST /api/audit/jobs`` validates the payload exactly like the
  synchronous endpoint, persists a job row and hands it to the background
  worker, which runs the same native solver core;
* ``GET  /api/audit/jobs/<operation_id>`` reports ``queued``, ``running``,
  ``succeeded`` or ``failed``; a succeeded job carries the exact document
  the synchronous endpoint would have returned.

Idempotency
-----------
The pair ``(operation_id, payload)`` identifies a job. Re-submitting the
same operation id with the same audit payload -- compared as canonical JSON
of the ``nodes``/``edges``/``endpoints`` fields, so object key order and
insignificant whitespace are ignored while array order and values are
significant -- returns the existing job. This holds across concurrent
submissions, client retries after lost responses, and service restarts.
Re-using the id with a *different* payload is rejected with
``409 OPERATION_ID_CONFLICT`` and never overwrites the stored job.

Crash recovery
--------------
Jobs live in a SQLite database (WAL journal, FULL synchronous) and every
state transition is a single transaction: the status and its result/error
document are committed together, so a half-written edge set or a partial
adjacency list can never be published. If the process dies before the
result is committed, the job is still ``queued``/``running``; on startup
every ``running`` job is reset to ``queued`` and recomputed from its
recoverable persisted payload. Terminal states (``succeeded``/``failed``)
are never resurrected, so a stale failure can never be mistaken for a
fresh success.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import sqlite3
import threading
import time
import traceback
from typing import Any

from .errors import TopologyError, ValidationError
from .solver import solve_payload

JOB_STATUSES = ("queued", "running", "succeeded", "failed")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    operation_id TEXT PRIMARY KEY,
    payload_hash TEXT NOT NULL,
    payload     TEXT NOT NULL,
    status      TEXT NOT NULL
                CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    result      TEXT,
    error       TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
"""

_SELECT = (
    "SELECT operation_id, payload_hash, payload, status, result, error,"
    " created_at, updated_at FROM jobs"
)


def canonical_payload(payload: dict[str, Any]) -> str:
    """Canonical JSON of the audit payload (``nodes``/``edges``/``endpoints``).

    Object key order and whitespace are insignificant; array order and
    values are significant. Extra request fields, which the solver ignores,
    do not take part in the identity of a job.
    """
    audit = {key: payload.get(key) for key in ("nodes", "edges", "endpoints")}
    return json.dumps(
        audit, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def payload_digest(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def job_document(job: dict[str, Any]) -> dict[str, Any]:
    """Public view of a job row (submit response and poll response)."""
    doc: dict[str, Any] = {
        "operation_id": job["operation_id"],
        "status": job["status"],
    }
    if "result" in job:
        doc["result"] = job["result"]
    if "error" in job:
        doc["error"] = job["error"]
    return doc


class JobStore:
    """SQLite-backed durable job table.

    A single connection guarded by a re-entrant lock serialises every
    operation, which keeps read-modify-write sequences (create-or-get,
    claim, finish) race-free between the HTTP handler threads and the
    worker thread.
    """

    def __init__(self, db_path: str) -> None:
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        # WAL + FULL synchronous: every commit is durable and atomic, so a
        # crash can never leave a half-written row behind.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    @staticmethod
    def _row_to_job(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        (operation_id, digest, payload, status, result, error,
         created_at, updated_at) = row
        job: dict[str, Any] = {
            "operation_id": operation_id,
            "payload_hash": digest,
            "payload": payload,
            "status": status,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        if result is not None:
            job["result"] = json.loads(result)
        if error is not None:
            job["error"] = json.loads(error)
        return job

    def _get_locked(self, operation_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            _SELECT + " WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        return self._row_to_job(row)

    def get(self, operation_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._get_locked(operation_id)

    def create_or_get(
        self, operation_id: str, canonical: str, digest: str
    ) -> tuple[dict[str, Any], bool]:
        """Insert a fresh ``queued`` job or return the existing one.

        Returns ``(job, created)``. The existence check and the insert run
        in one transaction under the store lock; the primary key plus an
        ``IntegrityError`` fallback guard against any residual race.
        """
        now = time.time()
        with self._lock, self._conn:
            existing = self._get_locked(operation_id)
            if existing is not None:
                return existing, False
            try:
                self._conn.execute(
                    "INSERT INTO jobs"
                    " (operation_id, payload_hash, payload, status,"
                    "  created_at, updated_at)"
                    " VALUES (?, ?, ?, 'queued', ?, ?)",
                    (operation_id, digest, canonical, now, now),
                )
            except sqlite3.IntegrityError:
                existing = self._get_locked(operation_id)
                if existing is None:  # pragma: no cover - defensive
                    raise
                return existing, False
            job = self._get_locked(operation_id)
            assert job is not None  # inserted in the same transaction
            return job, True

    def claim(self, operation_id: str) -> dict[str, Any] | None:
        """Atomically move ``queued -> running``; ``None`` if not queued."""
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE jobs SET status = 'running', updated_at = ?"
                " WHERE operation_id = ? AND status = 'queued'",
                (now, operation_id),
            )
            if cur.rowcount != 1:
                return None
            return self._get_locked(operation_id)

    def finish(
        self,
        operation_id: str,
        status: str,
        *,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        """Commit a terminal state and its document in one transaction.

        The status flip and the result/error payload are a single UPDATE,
        so observers can never see ``succeeded`` without the complete edge
        set and adjacency list, nor ``failed`` without its error document.
        """
        if status not in ("succeeded", "failed"):
            raise ValueError(f"not a terminal status: {status!r}")
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE jobs SET status = ?, result = ?, error = ?,"
                " updated_at = ?"
                " WHERE operation_id = ? AND status = 'running'",
                (
                    status,
                    json.dumps(result, separators=(",", ":"))
                    if result is not None else None,
                    json.dumps(error, separators=(",", ":"))
                    if error is not None else None,
                    now,
                    operation_id,
                ),
            )

    def reset_running(self) -> int:
        """Crash recovery: re-queue jobs interrupted mid-computation.

        A job found in ``running`` at startup means the process died before
        its result transaction committed; it is recomputed from the
        persisted payload. Terminal states are left untouched.
        """
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE jobs SET status = 'queued', updated_at = ?"
                " WHERE status = 'running'",
                (now,),
            )
            return cur.rowcount

    def queued_ids(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT operation_id FROM jobs"
                " WHERE status = 'queued' ORDER BY created_at"
            ).fetchall()
            return [row[0] for row in rows]


class JobWorker:
    """Single background thread executing queued jobs sequentially.

    Jobs are executed one at a time so a burst of submissions queues up
    (observable as the ``queued`` status) instead of racing for CPU. The
    in-flight set deduplicates queue entries, and ``claim`` makes the
    ``queued -> running`` transition atomic, so a job can never be solved
    twice even if it is enqueued more than once.
    """

    def __init__(self, store: JobStore) -> None:
        self._store = store
        self._queue: queue.Queue[str] = queue.Queue()
        self._inflight: set[str] = set()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Recover from any unclean shutdown, then start the worker loop."""
        recovered = self._store.reset_running()
        if recovered:
            print(f"[jobs] recovered {recovered} interrupted job(s)")
        for operation_id in self._store.queued_ids():
            self.submit(operation_id)
        self._thread = threading.Thread(
            target=self._loop, name="job-worker", daemon=True
        )
        self._thread.start()

    def submit(self, operation_id: str) -> None:
        with self._lock:
            if operation_id in self._inflight:
                return
            self._inflight.add(operation_id)
        self._queue.put(operation_id)

    def _loop(self) -> None:
        while True:
            operation_id = self._queue.get()
            try:
                self._execute(operation_id)
            finally:
                with self._lock:
                    self._inflight.discard(operation_id)

    def _execute(self, operation_id: str) -> None:
        job = self._store.claim(operation_id)
        if job is None:
            return  # already claimed or terminal; nothing to do
        try:
            payload = json.loads(job["payload"])
            result = solve_payload(payload)
        except (ValidationError, TopologyError) as exc:
            # The payload was fully validated at submit time, so reaching
            # this is defensive only -- still record it as a proper failure
            # rather than ever dropping the job.
            self._store.finish(operation_id, "failed", error=exc.to_dict())
        except Exception:  # noqa: BLE001 - solver crash, timeout, invariant
            traceback.print_exc()
            self._store.finish(
                operation_id,
                "failed",
                error={
                    "code": "INTERNAL_ERROR",
                    "message": "solver failed while computing the subnet",
                },
            )
        else:
            self._store.finish(operation_id, "succeeded", result=result)
