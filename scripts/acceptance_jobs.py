#!/usr/bin/env python3
"""End-to-end acceptance for the asynchronous audit job API on Compose.

Drives a real deployment (stdlib only, no third-party packages):

  1. builds and starts the ``api`` service via ``docker compose``;
  2. submits async jobs, checks idempotent replay (including concurrent
     duplicates) and the stable 409 conflict on payload mismatch;
  3. simulates a crash: hard-kills the container while a job is running,
     restarts the service and polls the job to its conclusion -- the
     recovered result must be complete and identical to the synchronous
     ``POST /api/audit`` response, and no poll may ever observe a partial
     edge set or adjacency list;
  4. confirms completed jobs and conflict rejections survive the restart;
  5. re-checks that the synchronous endpoint's semantics and error formats
     are unchanged.

Usage (from the repository root):

    python3 scripts/acceptance_jobs.py

Environment:
    AUDIT_HOST_PORT   host port mapped to the api container (default 8080)
    COMPOSE           compose command to use (default "docker compose")
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
HOST_PORT = os.environ.get("AUDIT_HOST_PORT", "8080")
BASE_URL = f"http://127.0.0.1:{HOST_PORT}"
COMPOSE = os.environ.get("COMPOSE", "docker compose").split()

_failures: list[str] = []


def check(condition: bool, label: str, detail: str = "") -> None:
    if condition:
        print(f"  pass  {label}")
    else:
        _failures.append(f"{label} {detail}".strip())
        print(f"  FAIL  {label} {detail}")


# --------------------------------------------------------------------------
# Compose orchestration
# --------------------------------------------------------------------------

def compose(*args: str, job_delay: str | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["AUDIT_HOST_PORT"] = HOST_PORT
    if job_delay is not None:
        env["AUDIT_JOB_DELAY_SECONDS"] = job_delay
    return subprocess.run(
        [*COMPOSE, *args], cwd=ROOT, env=env,
        capture_output=True, text=True, check=False,
    )


def up_api(job_delay: str) -> None:
    """(Re)create the api service with the given worker delay and wait."""
    proc = compose("up", "-d", "--build", "api", job_delay=job_delay)
    if proc.returncode != 0:
        raise RuntimeError(
            f"compose up failed:\n{proc.stdout}\n{proc.stderr}"
        )
    wait_for_health()


def wait_for_health(timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            status, body = request("GET", "/healthz")
            if status == 200 and isinstance(body, dict) \
                    and body.get("status") == "ok":
                return
        except Exception as exc:  # noqa: BLE001 - service may still start
            last = exc
        time.sleep(0.5)
    raise RuntimeError(f"api did not become healthy: {last}")


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

def request(method: str, path: str, body: Any = None) -> tuple[int, Any]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(
        BASE_URL + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload, status = resp.read(), resp.status
    except urllib.error.HTTPError as exc:
        payload, status = exc.read(), exc.code
    try:
        return status, json.loads(payload.decode("utf-8"))
    except Exception:
        return status, payload


def poll_job(op_id: str, timeout: float = 120.0) -> dict[str, Any]:
    """Poll a job to its terminal state, tolerating container restarts.

    Every successful response is validated: intermediate states must be
    bare (no result/error), and a succeeded result must be complete --
    a partial edge set or adjacency list is a hard failure.
    """
    deadline = time.time() + timeout
    last: Any = None
    while time.time() < deadline:
        try:
            status, body = request("GET", f"/api/jobs/{op_id}")
        except Exception:  # noqa: BLE001 - container may be mid-restart
            time.sleep(0.5)
            continue
        last = body
        if status != 200 or not isinstance(body, dict):
            time.sleep(0.5)
            continue
        state = body.get("status")
        if state in ("queued", "running"):
            check("result" not in body and "error" not in body,
                  f"{op_id}: intermediate state is bare", str(body))
            time.sleep(0.5)
            continue
        if state in ("succeeded", "failed"):
            return body
        check(False, f"{op_id}: unknown job state", str(body))
        time.sleep(0.5)
    raise RuntimeError(f"job {op_id} did not finish in time; last={last}")


def check_result_complete(result: Any, label: str) -> None:
    """A published result must be the complete audit document."""
    ok = (
        isinstance(result, dict)
        and set(result.keys()) == {"cost", "edge_set", "edges", "adjacency"}
        and isinstance(result["edge_set"], list)
        and len(result["edge_set"]) == len(result["edges"])
        and sum(e["cost"] for e in result["edges"]) == result["cost"]
    )
    check(ok, label, str(result)[:400])


# --------------------------------------------------------------------------
# Instance generators (deterministic)
# --------------------------------------------------------------------------

def large_payload(seed: int = 20260924) -> dict[str, Any]:
    """A realistically large review: 56 nodes, 200 edges, 9 endpoints."""
    rng = random.Random(seed)
    n, m, k = 56, 200, 9
    nodes = [f"N{i:02d}" for i in range(n)]
    edges: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()

    def add(u: int, v: int) -> None:
        seen.add((min(u, v), max(u, v)))
        edges.append({
            "id": f"e{len(edges):03d}",
            "source": nodes[u],
            "target": nodes[v],
            "cost": rng.randint(1, 99),
        })

    for v in range(1, n):  # spanning tree keeps the instance connected
        add(rng.randrange(v), v)
    while len(edges) < m:
        u, v = rng.randrange(n), rng.randrange(n)
        if u != v and (min(u, v), max(u, v)) not in seen:
            add(u, v)
    return {
        "nodes": nodes,
        "edges": edges,
        "endpoints": rng.sample(nodes, k),
    }


def edge(id_, s, t, c):
    return {"id": id_, "source": s, "target": t, "cost": c}


SMALL_PAYLOAD = {
    "nodes": ["A", "B", "C", "D", "R"],
    "edges": [
        edge("e1", "A", "R", 5),
        edge("e2", "B", "R", 5),
        edge("e3", "C", "R", 5),
        edge("e4", "D", "R", 5),
        edge("direct-ab", "A", "B", 9),
        edge("relay-cd", "C", "D", 1),
    ],
    "endpoints": ["A", "B", "C", "D"],
}

SPLIT_PAYLOAD = {
    "nodes": ["A", "B", "C", "D"],
    "edges": [edge("e1", "A", "B", 1), edge("e2", "C", "D", 1)],
    "endpoints": ["A", "C"],
}


def unique_op(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


# --------------------------------------------------------------------------
# Acceptance phases
# --------------------------------------------------------------------------

def phase_async_happy_path() -> tuple[str, dict[str, Any]]:
    print("[1] submit, idempotent replay, concurrency, conflict")
    status, sync_body = request("POST", "/api/audit", SMALL_PAYLOAD)
    check(status == 200, "sync baseline 200", f"got {status}")

    op = unique_op("acc-main")
    status, body = request("POST", "/api/jobs",
                           {"operation_id": op, "payload": SMALL_PAYLOAD})
    check(status == 202, "submit -> 202", f"got {status}: {body}")
    check(isinstance(body, dict) and body.get("operation_id") == op
          and body.get("status") in ("queued", "running"),
          "submit returns the queued job", str(body))

    # Retry after a 'lost response': same id + identical payload -> same job.
    status, replay = request("POST", "/api/jobs",
                             {"operation_id": op, "payload": SMALL_PAYLOAD})
    check(status in (200, 202) and replay.get("operation_id") == op,
          "retry after lost response -> same job", str(replay))

    # Concurrent duplicates: exactly one job, every response agrees.
    responses: list[tuple[int, Any]] = []
    barrier = threading.Barrier(8)

    def dup():
        barrier.wait()
        responses.append(request(
            "POST", "/api/jobs", {"operation_id": op, "payload": SMALL_PAYLOAD}))

    threads = [threading.Thread(target=dup) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check(all(s in (200, 202) for s, _ in responses),
          "concurrent duplicates all accepted",
          str([s for s, _ in responses]))
    check(all(isinstance(b, dict) and b.get("operation_id") == op
              for _, b in responses),
          "concurrent duplicates reference the same job")

    job = poll_job(op)
    check(job.get("status") == "succeeded", "job succeeded", str(job))
    check(job.get("result") == sync_body,
          "async result identical to sync response")
    check_result_complete(job.get("result"), "published result is complete")

    # Id reuse with a different payload: stable 409, original untouched.
    changed = dict(SMALL_PAYLOAD)
    changed["edges"] = SMALL_PAYLOAD["edges"][:-1]
    for attempt in range(2):
        status, conflict = request(
            "POST", "/api/jobs", {"operation_id": op, "payload": changed})
        check(status == 409
              and conflict.get("code") == "OPERATION_CONFLICT",
              f"conflict 409 (attempt {attempt + 1})", f"got {status}")
    _, job = poll_job(op)
    check(job.get("result") == sync_body, "original job not overwritten")
    return op, sync_body


def phase_failed_job() -> None:
    print("[2] failed job state")
    status, sync_err = request("POST", "/api/audit", SPLIT_PAYLOAD)
    check(status == 422 and sync_err.get("code") == "ENDPOINTS_UNCONNECTED",
          "sync baseline 422", f"got {status}")
    op = unique_op("acc-fail")
    status, _ = request("POST", "/api/jobs",
                        {"operation_id": op, "payload": SPLIT_PAYLOAD})
    check(status == 202, "infeasible payload accepted", f"got {status}")
    job = poll_job(op)
    check(job.get("status") == "failed", "job failed", str(job))
    check(job.get("error") == sync_err,
          "failed error identical to sync 422 body", str(job.get("error")))
    check("result" not in job, "no result on failure", str(job))


def phase_crash_recovery() -> str:
    print("[3] crash mid-computation -> restart -> recovered conclusion")
    payload = large_payload()
    up_api(job_delay="3")  # widen the running window for a reliable kill
    op = unique_op("acc-crash")
    status, body = request("POST", "/api/jobs",
                           {"operation_id": op, "payload": payload})
    check(status == 202, "large job submitted", f"got {status}: {body}")

    time.sleep(1.0)  # the job is now running (inside the artificial delay)
    proc = compose("kill", "-s", "SIGKILL", "api")
    check(proc.returncode == 0, "hard kill (SIGKILL) the api container",
          proc.stderr.strip())

    up_api(job_delay="0")  # restart without the test hook
    job = poll_job(op, timeout=180.0)
    check(job.get("status") == "succeeded",
          "interrupted job recomputed to success", str(job)[:400])

    status, sync_body = request("POST", "/api/audit", payload)
    check(status == 200, "sync baseline for large instance", f"got {status}")
    check(job.get("result") == sync_body,
          "recovered result identical to sync response")
    check_result_complete(job.get("result"),
                          "recovered result is complete (no partial state)")
    return op


def phase_persistence(known: list[tuple[str, str]]) -> None:
    print("[4] completed jobs and conflicts survive the restart")
    for op, note in known:
        status, job = request("GET", f"/api/jobs/{op}")
        check(status == 200 and job.get("status") in ("succeeded", "failed"),
              f"{note}: job still terminal after restart",
              f"got {status}: {str(job)[:200]}")
        check(isinstance(job.get("result") or job.get("error"), dict),
              f"{note}: terminal document intact", str(job)[:200])
    # The conflict rejection is durable, too.
    changed = dict(SMALL_PAYLOAD)
    changed["edges"] = SMALL_PAYLOAD["edges"][:-1]
    status, conflict = request(
        "POST", "/api/jobs", {"operation_id": known[0][0], "payload": changed})
    check(status == 409 and conflict.get("code") == "OPERATION_CONFLICT",
          "conflict still rejected after restart", f"got {status}")


def phase_sync_endpoint() -> None:
    print("[5] synchronous endpoint semantics unchanged")
    status, body = request("POST", "/api/audit", SMALL_PAYLOAD)
    check(status == 200 and body.get("cost") == 16
          and set(body.get("edge_set", [])) == {"e1", "e2", "e3", "relay-cd"},
          "sync solve unchanged", str(body)[:200])

    status, body = request("POST", "/api/audit", {
        "nodes": ["A", "B"],
        "edges": [edge("e1", "A", "B", 0)],
        "endpoints": ["A", "B"],
    })
    check(status == 400 and body.get("code") == "NON_POSITIVE_COST"
          and body.get("pointer") == "/edges/0/cost",
          "sync 400 format unchanged", str(body))

    status, body = request("POST", "/api/audit", SPLIT_PAYLOAD)
    check(status == 422 and body.get("code") == "ENDPOINTS_UNCONNECTED"
          and body.get("components") == [["A"], ["C"]],
          "sync 422 format unchanged", str(body))

    status, _ = request("GET", "/api/audit")
    check(status == 405, "GET /api/audit -> 405", f"got {status}")
    status, _ = request("GET", "/no-such-path")
    check(status == 404, "unknown path -> 404", f"got {status}")


def main() -> int:
    print(f"acceptance target: {BASE_URL} (compose project at {ROOT})")
    print("[0] build and start the api service")
    try:
        up_api(job_delay="0")
    except FileNotFoundError:
        print("ERROR: `docker compose` is not available on this machine;")
        print("run this acceptance on a host with Docker installed.")
        return 2

    op_main, _ = phase_async_happy_path()
    phase_failed_job()
    op_crash = phase_crash_recovery()
    phase_persistence([(op_main, "main"), (op_crash, "crashed")])
    phase_sync_endpoint()

    print("-" * 68)
    if _failures:
        print(f"ACCEPTANCE FAILED: {len(_failures)} check(s)")
        for f in _failures:
            print("  -", f)
        return 1
    print("ACCEPTANCE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
