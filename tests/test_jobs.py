"""Unit tests for the durable async job store and the job runner.

Run with: ``python3 tests/harness.py test_jobs`` from the repo root.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import assert_raises  # noqa: E402

from app.audit import solve_payload  # noqa: E402
from app.errors import ValidationError  # noqa: E402
from app.jobs import (  # noqa: E402
    JobConflict,
    JobRunner,
    JobStore,
    canonical_payload,
    job_document,
    payload_digest,
    validate_operation_id,
)

CORE = Path(os.environ.get(
    "STEINER_CORE", Path(__file__).resolve().parents[1] / "core" / "steiner"
))
assert CORE.exists(), f"native core missing: {CORE}"


def edge(id_, s, t, c):
    return {"id": id_, "source": s, "target": t, "cost": c}


PAYLOAD = {
    "nodes": ["a", "b", "c", "h"],
    "edges": [
        edge("ha", "h", "a", 1),
        edge("hb", "h", "b", 1),
        edge("hc", "h", "c", 1),
        edge("ab", "a", "b", 3),
    ],
    "endpoints": ["a", "b", "c"],
}

SPLIT_PAYLOAD = {
    "nodes": ["a", "b", "c", "d"],
    "edges": [edge("e1", "a", "b", 1), edge("e2", "c", "d", 1)],
    "endpoints": ["a", "c"],
}


def make_store(tmp):
    return JobStore(str(Path(tmp) / "jobs.sqlite3"))


def submit(store, op, payload):
    canonical = canonical_payload(payload)
    return store.submit(op, canonical, payload_digest(canonical))


def wait_final(store, op, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        record = store.get(op)
        if record["status"] in ("succeeded", "failed"):
            return record
        time.sleep(0.05)
    raise AssertionError(f"job {op} did not reach a terminal state")


# --------------------------------------------------------------------------
# Canonical payload / operation id validation
# --------------------------------------------------------------------------

class TestCanonicalPayload:
    def test_key_order_invariant(self):
        a = canonical_payload({"x": 1, "y": [1, 2], "z": {"b": 2, "a": 1}})
        b = canonical_payload({"z": {"a": 1, "b": 2}, "y": [1, 2], "x": 1})
        assert a == b
        assert payload_digest(a) == payload_digest(b)

    def test_array_order_is_significant(self):
        a = canonical_payload({"edges": [1, 2]})
        b = canonical_payload({"edges": [2, 1]})
        assert payload_digest(a) != payload_digest(b)

    def test_validate_operation_id(self):
        assert validate_operation_id("op-1.X_y~z") == "op-1.X_y~z"
        for bad, code in [
            (None, "INVALID_OPERATION_ID"),
            (42, "INVALID_OPERATION_ID"),
            ("", "EMPTY_ID"),
            ("é", "NON_ASCII_ID"),
            ("has space", "INVALID_OPERATION_ID"),
            ("a/b", "INVALID_OPERATION_ID"),
            ("-leading", "INVALID_OPERATION_ID"),
            ("x" * 129, "INVALID_OPERATION_ID"),
        ]:
            with assert_raises(ValidationError) as ctx:
                validate_operation_id(bad)
            assert ctx.value.code == code, (bad, ctx.value.code)
            assert ctx.value.pointer == "/operation_id"


# --------------------------------------------------------------------------
# JobStore: idempotency, conflict, state machine, recovery
# --------------------------------------------------------------------------

class TestJobStore:
    def setup_method(self, _name):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = make_store(self._tmp.name)

    def teardown_method(self, _name):
        self._tmp.cleanup()

    def test_submit_creates_queued_job(self):
        record, created = submit(self.store, "op-1", PAYLOAD)
        assert created is True
        assert record["status"] == "queued"
        assert record["operation_id"] == "op-1"

    def test_identical_resubmit_returns_same_job(self):
        first, created1 = submit(self.store, "op-1", PAYLOAD)
        second, created2 = submit(self.store, "op-1", PAYLOAD)
        assert created1 is True and created2 is False
        assert second["created_at"] == first["created_at"]
        assert second["status"] == first["status"]

    def test_resubmit_key_reordered_payload_is_identical(self):
        reordered = {
            "endpoints": PAYLOAD["endpoints"],
            "edges": PAYLOAD["edges"],
            "nodes": PAYLOAD["nodes"],
        }
        submit(self.store, "op-1", PAYLOAD)
        _, created = submit(self.store, "op-1", reordered)
        assert created is False

    def test_conflicting_payload_rejected_and_original_kept(self):
        submit(self.store, "op-1", PAYLOAD)
        other = dict(PAYLOAD)
        other["edges"] = PAYLOAD["edges"][:3]
        with assert_raises(JobConflict):
            submit(self.store, "op-1", other)
        record = self.store.get("op-1")
        assert record["payload_json"] == canonical_payload(PAYLOAD)
        assert record["status"] == "queued"

    def test_concurrent_identical_submit_creates_exactly_one(self):
        results = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            results.append(submit(self.store, "op-c", PAYLOAD)[1])

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert results.count(True) == 1
        assert results.count(False) == 7

    def test_concurrent_conflicting_submit_has_one_winner(self):
        outcomes = []
        barrier = threading.Barrier(2)
        other = dict(PAYLOAD)
        other["edges"] = PAYLOAD["edges"][:3]

        def worker(payload):
            barrier.wait()
            try:
                submit(self.store, "op-x", payload)
                outcomes.append("created")
            except JobConflict:
                outcomes.append("conflict")

        threads = [
            threading.Thread(target=worker, args=(PAYLOAD,)),
            threading.Thread(target=worker, args=(other,)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(outcomes) == ["conflict", "created"]

    def test_state_machine_transitions(self):
        submit(self.store, "op-1", PAYLOAD)
        assert self.store.mark_running("op-1") is True
        # Second claim fails: already running.
        assert self.store.mark_running("op-1") is False
        result_json = json.dumps({"cost": 3}, separators=(",", ":"))
        assert self.store.complete("op-1", result_json) is True
        # Terminal states are final.
        assert self.store.mark_running("op-1") is False
        assert self.store.fail("op-1", "{}") is False
        record = self.store.get("op-1")
        assert record["status"] == "succeeded"
        assert record["result_json"] == result_json

    def test_fail_records_error_document(self):
        submit(self.store, "op-1", PAYLOAD)
        self.store.mark_running("op-1")
        error = json.dumps({"code": "INTERNAL_ERROR"}, separators=(",", ":"))
        assert self.store.fail("op-1", error) is True
        # A failure is terminal: it can never flip to success.
        assert self.store.complete("op-1", "{}") is False
        record = self.store.get("op-1")
        assert record["status"] == "failed"
        assert record["error_json"] == error

    def test_requeue_interrupted_only_touches_running(self):
        submit(self.store, "op-queued", PAYLOAD)
        submit(self.store, "op-running", PAYLOAD)
        self.store.mark_running("op-running")
        submit(self.store, "op-done", PAYLOAD)
        self.store.mark_running("op-done")
        self.store.complete("op-done", json.dumps({"cost": 1}))
        submit(self.store, "op-failed", PAYLOAD)
        self.store.mark_running("op-failed")
        self.store.fail("op-failed", json.dumps({"code": "X"}))

        pending = self.store.requeue_interrupted()
        assert set(pending) == {"op-queued", "op-running"}
        assert self.store.get("op-running")["status"] == "queued"
        assert self.store.get("op-done")["status"] == "succeeded"
        assert self.store.get("op-failed")["status"] == "failed"

    def test_records_survive_reopen(self):
        submit(self.store, "op-1", PAYLOAD)
        self.store.mark_running("op-1")
        self.store.complete("op-1", json.dumps({"cost": 3}))
        reopened = make_store(self._tmp.name)
        record = reopened.get("op-1")
        assert record["status"] == "succeeded"
        _, created = submit(reopened, "op-1", PAYLOAD)
        assert created is False

    def test_job_document_shape(self):
        submit(self.store, "op-1", PAYLOAD)
        doc = job_document(self.store.get("op-1"))
        assert doc == {"operation_id": "op-1", "status": "queued"}
        self.store.mark_running("op-1")
        self.store.complete("op-1", json.dumps({"cost": 3, "edge_set": []}))
        doc = job_document(self.store.get("op-1"))
        assert doc["status"] == "succeeded"
        assert doc["result"] == {"cost": 3, "edge_set": []}
        assert "error" not in doc


# --------------------------------------------------------------------------
# JobRunner: end-to-end execution, failure, crash recovery
# --------------------------------------------------------------------------

class TestJobRunner:
    def setup_method(self, _name):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = make_store(self._tmp.name)
        self.runner = JobRunner(self.store, workers=2)

    def teardown_method(self, _name):
        self._tmp.cleanup()

    def enqueue(self, op, payload):
        submit(self.store, op, payload)
        self.runner.submit(op)

    def test_end_to_end_success_matches_sync_result(self):
        self.enqueue("op-ok", PAYLOAD)
        record = wait_final(self.store, "op-ok")
        assert record["status"] == "succeeded"
        doc = job_document(record)
        assert doc["result"] == solve_payload(PAYLOAD)
        assert doc["result"]["cost"] == 3
        assert doc["result"]["edge_set"] == ["ha", "hb", "hc"]

    def test_topology_error_becomes_failed_job(self):
        self.enqueue("op-split", SPLIT_PAYLOAD)
        doc = job_document(wait_final(self.store, "op-split"))
        assert doc["status"] == "failed"
        assert doc["error"]["code"] == "ENDPOINTS_UNCONNECTED"
        assert doc["error"]["components"] == [["a"], ["c"]]
        assert "result" not in doc

    def test_invalid_stored_payload_fails_cleanly(self):
        # Defence in depth: a row that fails re-validation (e.g. written by
        # an older version) becomes a failed job, never a crash.
        self.enqueue("op-bad", {"nodes": ["a"], "edges": [], "endpoints": []})
        doc = job_document(wait_final(self.store, "op-bad"))
        assert doc["status"] == "failed"
        assert doc["error"]["code"] == "NODE_COUNT_OUT_OF_RANGE"

    def test_recovery_recomputes_interrupted_job(self):
        # Simulate a crash: job persisted, claimed, but result never written.
        submit(self.store, "op-crash", PAYLOAD)
        assert self.store.mark_running("op-crash") is True

        # A fresh runner over the same store (as after a process restart).
        recovered = self.runner.recover()
        assert recovered == 1
        doc = job_document(wait_final(self.store, "op-crash"))
        assert doc["status"] == "succeeded"
        assert doc["result"] == solve_payload(PAYLOAD)
        assert self.store.get("op-crash")["attempts"] == 2

    def test_recovery_never_touches_terminal_jobs(self):
        self.enqueue("op-final", SPLIT_PAYLOAD)
        doc = job_document(wait_final(self.store, "op-final"))
        assert doc["status"] == "failed"
        error_json = self.store.get("op-final")["error_json"]
        self.runner.recover()
        time.sleep(0.3)
        record = self.store.get("op-final")
        assert record["status"] == "failed"
        assert record["error_json"] == error_json
        assert record["attempts"] == 1
