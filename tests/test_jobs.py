"""Unit tests for the durable job store (app/jobs.py).

Covers the idempotency key behaviour, the atomic state machine and crash
recovery semantics directly at the storage layer.

Run with: ``python3 tests/harness.py test_solver test_jobs`` from the repo
root (verify.py runs both).
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.jobs import (  # noqa: E402
    JobStore,
    canonical_payload,
    job_document,
    payload_digest,
)


def make_store(tmp: str) -> JobStore:
    return JobStore(os.path.join(tmp, "jobs.sqlite3"))


PAYLOAD = {
    "nodes": ["A", "B"],
    "edges": [{"id": "e1", "source": "A", "target": "B", "cost": 3}],
    "endpoints": ["A", "B"],
}


class TestCanonicalPayload:
    def test_key_order_insensitive(self):
        a = canonical_payload(PAYLOAD)
        b = canonical_payload({
            "edges": PAYLOAD["edges"],
            "endpoints": PAYLOAD["endpoints"],
            "nodes": PAYLOAD["nodes"],
        })
        assert a == b

    def test_array_order_significant(self):
        payload = {
            "nodes": ["A", "B", "C"],
            "edges": [
                {"id": "e1", "source": "A", "target": "B", "cost": 1},
                {"id": "e2", "source": "B", "target": "C", "cost": 1},
            ],
            "endpoints": ["A", "C"],
        }
        shuffled = dict(payload)
        shuffled["edges"] = list(reversed(payload["edges"]))
        assert canonical_payload(payload) != canonical_payload(shuffled)

    def test_extra_fields_ignored(self):
        extra = dict(PAYLOAD, comment="field engineer note")
        assert canonical_payload(extra) == canonical_payload(PAYLOAD)

    def test_digest_stable(self):
        assert payload_digest(canonical_payload(PAYLOAD)) == payload_digest(
            canonical_payload(PAYLOAD)
        )


class TestJobStore:
    def setup_method(self, _name):
        # TemporaryDirectory cleans itself up via its finalizer; the harness
        # has no teardown hook.
        self._tmp = tempfile.TemporaryDirectory()
        self.store = make_store(self._tmp.name)
        self.canonical = canonical_payload(PAYLOAD)
        self.digest = payload_digest(self.canonical)

    def test_create_then_get_returns_same_job(self):
        job, created = self.store.create_or_get("op-1", self.canonical,
                                                self.digest)
        assert created is True
        assert job["status"] == "queued"
        again, created = self.store.create_or_get("op-1", self.canonical,
                                                  self.digest)
        assert created is False
        assert again["operation_id"] == "op-1"
        assert again["payload_hash"] == self.digest

    def test_conflicting_payload_never_overwrites(self):
        self.store.create_or_get("op-1", self.canonical, self.digest)
        other, created = self.store.create_or_get("op-1", "{}", "0" * 64)
        assert created is False
        assert other["payload_hash"] == self.digest  # original intact

    def test_claim_is_atomic_and_single(self):
        self.store.create_or_get("op-1", self.canonical, self.digest)
        job = self.store.claim("op-1")
        assert job is not None and job["status"] == "running"
        assert self.store.claim("op-1") is None  # no double execution

    def test_finish_commits_status_and_document_together(self):
        self.store.create_or_get("op-1", self.canonical, self.digest)
        self.store.claim("op-1")
        result = {"cost": 3, "edge_set": ["e1"], "edges": [], "adjacency": {}}
        self.store.finish("op-1", "succeeded", result=result)
        job = self.store.get("op-1")
        assert job["status"] == "succeeded"
        assert job["result"] == result
        assert "error" not in job

    def test_failed_job_keeps_error_document(self):
        self.store.create_or_get("op-1", self.canonical, self.digest)
        self.store.claim("op-1")
        error = {"code": "INTERNAL_ERROR", "message": "boom"}
        self.store.finish("op-1", "failed", error=error)
        job = self.store.get("op-1")
        assert job["status"] == "failed"
        assert job["error"] == error
        assert "result" not in job

    def test_reset_running_recovers_only_interrupted_jobs(self):
        for op in ("op-queued", "op-running", "op-succeeded", "op-failed"):
            self.store.create_or_get(op, self.canonical, self.digest)
        self.store.claim("op-running")
        self.store.claim("op-succeeded")
        self.store.finish("op-succeeded", "succeeded", result={"cost": 3})
        self.store.claim("op-failed")
        self.store.finish("op-failed", "failed", error={"code": "X"})

        recovered = self.store.reset_running()
        assert recovered == 1
        assert self.store.get("op-running")["status"] == "queued"
        assert self.store.get("op-queued")["status"] == "queued"
        # Terminal states are never resurrected.
        assert self.store.get("op-succeeded")["status"] == "succeeded"
        assert self.store.get("op-failed")["status"] == "failed"
        assert set(self.store.queued_ids()) == {"op-queued", "op-running"}

    def test_jobs_survive_reopen(self):
        self.store.create_or_get("op-1", self.canonical, self.digest)
        self.store.claim("op-1")
        self.store.finish("op-1", "succeeded", result={"cost": 3})
        # Simulate a restart: a brand new store over the same database file.
        reopened = make_store(self._tmp.name)
        job = reopened.get("op-1")
        assert job is not None
        assert job["status"] == "succeeded"
        assert job["result"] == {"cost": 3}

    def test_unknown_operation_id(self):
        assert self.store.get("nope") is None


class TestJobDocument:
    def test_public_view_hides_internals(self):
        job = {
            "operation_id": "op-1",
            "status": "succeeded",
            "payload_hash": "secret",
            "payload": "{}",
            "created_at": 1.0,
            "updated_at": 2.0,
            "result": {"cost": 3},
        }
        doc = job_document(job)
        assert doc == {
            "operation_id": "op-1",
            "status": "succeeded",
            "result": {"cost": 3},
        }
