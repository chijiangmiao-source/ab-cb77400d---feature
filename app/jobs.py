"""Durable asynchronous audit jobs with idempotent submission.

Field engineers submit a calibration subnet review once and poll for the
conclusion; a dropped connection must never cause the same expensive
computation to be launched twice. This module provides:

* ``JobStore`` -- SQLite-backed durable job records. Submission is
  idempotent on the client-supplied operation id: the same id with an
  identical payload always addresses the same job (across concurrent
  submits, retries after a lost response, and service restarts), while
  reusing the id with a *different* payload is rejected with
  ``JobConflict`` and never overwrites the original record.
* ``JobRunner`` -- a small thread pool that executes queued jobs by
  invoking the existing solver kernel (one short-lived core process per
  audit, exactly like the synchronous endpoint).

Crash contract: a job row is created (status ``queued``) only after the
payload has been fully validated; the result is published by a single
atomic ``UPDATE`` that flips ``running`` -> ``succeeded`` together with the
complete result document. If the process dies anywhere before that update
commits, the row is still ``queued``/``running`` and on restart
``JobRunner.recover()`` re-queues exactly those rows and recomputes them
from the persisted payload. Readers therefore only ever observe a complete
state -- never half an edge set or a partial adjacency list -- and a
terminal (succeeded/failed) job is never recomputed, so an old failure can
never be mistaken for a new success.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any

from .audit import solve_payload
from .errors import TopologyError, ValidationError

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
TERMINAL_STATES = (STATUS_SUCCEEDED, STATUS_FAILED)

#: Client-supplied operation ids are restricted to unreserved URI characters
#: so they can be embedded in the poll path without percent-encoding.
OPERATION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,127}")

DEFAULT_JOB_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "jobs.sqlite3",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    operation_id  TEXT PRIMARY KEY,
    payload_hash  TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN
                      ('queued', 'running', 'succeeded', 'failed')),
    result_json   TEXT,
    error_json    TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
)
"""


class JobConflict(Exception):
    """The operation id is already bound to a different payload (HTTP 409)."""

    def __init__(self, operation_id: str) -> None:
        super().__init__(operation_id)
        self.operation_id = operation_id


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def canonical_payload(payload: Any) -> str:
    """Serialise a validated payload in a stable, comparable form.

    Object key order is normalised; array order is significant (a reordered
    edge list is a *different* submission for idempotency purposes, even
    though it would compute the same optimum).
    """
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def payload_digest(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def validate_operation_id(value: Any) -> str:
    """Validate the client-supplied operation id (HTTP 400 class)."""
    if not isinstance(value, str):
        raise ValidationError(
            "INVALID_OPERATION_ID",
            "operation_id must be a string",
            "/operation_id",
        )
    if value == "":
        raise ValidationError(
            "EMPTY_ID", "operation_id must not be empty", "/operation_id"
        )
    try:
        value.encode("ascii")
    except UnicodeEncodeError:
        raise ValidationError(
            "NON_ASCII_ID",
            f"operation_id must be ASCII: {value!r}",
            "/operation_id",
        ) from None
    if OPERATION_ID_RE.fullmatch(value) is None:
        raise ValidationError(
            "INVALID_OPERATION_ID",
            "operation_id must start with an ASCII letter or digit and "
            "contain only letters, digits, '.', '_', '~', '-' "
            "(max 128 chars)",
            "/operation_id",
        )
    return value


def job_document(record: sqlite3.Row) -> dict[str, Any]:
    """Public representation of a job row.

    Queued/running jobs expose no result fields; terminal jobs expose the
    complete ``result`` (identical to the synchronous audit response) or the
    stable ``error`` document -- never a mixture.
    """
    doc: dict[str, Any] = {
        "operation_id": record["operation_id"],
        "status": record["status"],
    }
    if record["status"] == STATUS_SUCCEEDED:
        doc["result"] = json.loads(record["result_json"])
    elif record["status"] == STATUS_FAILED:
        doc["error"] = json.loads(record["error_json"])
    return doc


class JobStore:
    """Durable job records; safe for concurrent use across threads."""

    def __init__(self, path: str) -> None:
        self._path = str(path)
        parent = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(parent, exist_ok=True)
        db = self._connect()
        try:
            db.executescript(_SCHEMA)
        finally:
            db.close()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self._path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        # WAL lets readers proceed while the single writer commits; FULL
        # sync keeps committed job states durable across a power loss.
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    # -- submission ----------------------------------------------------

    def submit(
        self, operation_id: str, canonical: str, digest: str
    ) -> tuple[sqlite3.Row, bool]:
        """Insert a queued job, or return the existing record for this id.

        Returns ``(record, created)``. Raises :class:`JobConflict` when the
        id exists with a different payload hash; the original row is left
        untouched. The insert-or-read sequence runs under an immediate
        transaction so concurrent first submissions of the same id resolve
        to exactly one creator.
        """
        now = _utcnow()
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "INSERT INTO jobs (operation_id, payload_hash,"
                    " payload_json, status, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (operation_id, digest, canonical, STATUS_QUEUED, now, now),
                )
            except sqlite3.IntegrityError:
                db.execute("ROLLBACK")
            else:
                db.execute("COMMIT")
                return self._fetch(db, operation_id), True
        finally:
            db.close()
        record = self.get(operation_id)
        if record is None:  # pragma: no cover - cannot happen after conflict
            raise RuntimeError("job store invariant: conflicting row missing")
        if record["payload_hash"] != digest:
            raise JobConflict(operation_id)
        return record, False

    # -- queries -------------------------------------------------------

    def get(self, operation_id: str) -> sqlite3.Row | None:
        db = self._connect()
        try:
            return self._fetch(db, operation_id)
        finally:
            db.close()

    @staticmethod
    def _fetch(db: sqlite3.Connection, operation_id: str) -> sqlite3.Row | None:
        return db.execute(
            "SELECT * FROM jobs WHERE operation_id = ?", (operation_id,)
        ).fetchone()

    # -- worker state machine ------------------------------------------

    def mark_running(self, operation_id: str) -> bool:
        """Transition queued -> running; False if the job moved on already."""
        db = self._connect()
        try:
            cur = db.execute(
                "UPDATE jobs SET status = ?, attempts = attempts + 1,"
                " updated_at = ? WHERE operation_id = ? AND status = ?",
                (STATUS_RUNNING, _utcnow(), operation_id, STATUS_QUEUED),
            )
            return cur.rowcount == 1
        finally:
            db.close()

    def complete(self, operation_id: str, result_json: str) -> bool:
        """Publish the full result atomically (running -> succeeded)."""
        db = self._connect()
        try:
            cur = db.execute(
                "UPDATE jobs SET status = ?, result_json = ?, updated_at = ?"
                " WHERE operation_id = ? AND status = ?",
                (STATUS_SUCCEEDED, result_json, _utcnow(),
                 operation_id, STATUS_RUNNING),
            )
            return cur.rowcount == 1
        finally:
            db.close()

    def fail(self, operation_id: str, error_json: str) -> bool:
        """Publish the stable error document atomically (running -> failed)."""
        db = self._connect()
        try:
            cur = db.execute(
                "UPDATE jobs SET status = ?, error_json = ?, updated_at = ?"
                " WHERE operation_id = ? AND status = ?",
                (STATUS_FAILED, error_json, _utcnow(),
                 operation_id, STATUS_RUNNING),
            )
            return cur.rowcount == 1
        finally:
            db.close()

    def requeue_interrupted(self) -> list[str]:
        """Crash recovery: running -> queued; return every queued job id.

        Terminal rows (succeeded/failed) are final and are never touched,
        so a recorded failure can never be recomputed into a success.
        """
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE jobs SET status = ?, updated_at = ? WHERE status = ?",
                (STATUS_QUEUED, _utcnow(), STATUS_RUNNING),
            )
            rows = db.execute(
                "SELECT operation_id FROM jobs WHERE status = ?"
                " ORDER BY created_at",
                (STATUS_QUEUED,),
            ).fetchall()
            db.execute("COMMIT")
            return [row["operation_id"] for row in rows]
        finally:
            db.close()


class JobRunner:
    """Thread pool executing queued jobs through the existing solver."""

    def __init__(
        self, store: JobStore, workers: int = 2, start_delay: float = 0.0
    ) -> None:
        self._store = store
        # Test/demo hook (AUDIT_JOB_DELAY_SECONDS): artificial delay while a
        # job is in `running`, giving crash-recovery acceptance a reliable
        # kill window. Zero in normal operation.
        self._start_delay = start_delay
        self._queue: queue.Queue[str] = queue.Queue()
        for i in range(workers):
            thread = threading.Thread(
                target=self._work, daemon=True, name=f"job-worker-{i}"
            )
            thread.start()

    def submit(self, operation_id: str) -> None:
        self._queue.put(operation_id)

    def recover(self) -> int:
        """Re-queue jobs left recoverable by a previous process; see module
        docstring for the crash contract. Returns the number re-queued."""
        pending = self._store.requeue_interrupted()
        for operation_id in pending:
            self._queue.put(operation_id)
        return len(pending)

    def _work(self) -> None:
        while True:
            operation_id = self._queue.get()
            try:
                self._run_one(operation_id)
            except Exception:  # noqa: BLE001 - a worker must never die
                traceback.print_exc()
            finally:
                self._queue.task_done()

    def _run_one(self, operation_id: str) -> None:
        if not self._store.mark_running(operation_id):
            return  # already terminal or claimed; nothing to do
        try:
            record = self._store.get(operation_id)
            payload = json.loads(record["payload_json"])
            if self._start_delay > 0:
                time.sleep(self._start_delay)
            result = solve_payload(payload)
        except (ValidationError, TopologyError) as exc:
            self._store.fail(operation_id, _dump_error(exc.to_dict()))
        except Exception:  # noqa: BLE001 - never leak internals to clients
            traceback.print_exc()
            self._store.fail(
                operation_id,
                _dump_error(
                    {
                        "code": "INTERNAL_ERROR",
                        "message": "solver failed while computing the subnet",
                    }
                ),
            )
        else:
            self._store.complete(
                operation_id,
                json.dumps(result, separators=(",", ":"), ensure_ascii=True),
            )


def _dump_error(doc: dict[str, Any]) -> str:
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=True)
