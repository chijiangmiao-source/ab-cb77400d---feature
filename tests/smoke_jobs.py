"""Smoke tests for the asynchronous, idempotent job endpoints.

Zero third-party dependencies (urllib only). Covers:
  * submit -> poll lifecycle: queued/running -> succeeded, and the result
    document is byte-identical to the synchronous POST /api/audit response;
  * idempotent replay: same operation id + same payload (even with a
    different JSON key order) returns the same job -- sequentially,
    concurrently, and after a simulated service restart;
  * conflict: reusing the operation id with a different payload is rejected
    with 409 and never overwrites the stored job;
  * validation-before-persistence: invalid payloads create no job;
  * crash recovery: SIGKILL the api mid-flight (via the Docker socket when
    running under Compose); the restarted service recomputes the job from
    its recoverable state and concludes successfully -- never publishing a
    partial edge set or mistaking a stale failure for a success.

When the Docker socket is unavailable (local runs outside Compose), the
restart-dependent checks are skipped, not failed.

Exits non-zero on the first failed check, printing a readable report.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any

BASE_URL = os.environ.get("AUDIT_BASE_URL", "http://127.0.0.1:8080")
DOCKER_SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")

_failures: list[str] = []


def check(condition: bool, label: str, detail: str = "") -> None:
    if condition:
        print(f"  pass  {label}")
    else:
        _failures.append(f"{label} {detail}".strip())
        print(f"  FAIL  {label} {detail}")


def request(method: str, path: str, body: Any = None,
            raw: bytes | None = None) -> tuple[int, Any]:
    url = BASE_URL + path
    if raw is not None:
        data = raw
        headers = {"Content-Type": "application/json"}
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"}
    else:
        data = None
        headers = {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        status = exc.code
    try:
        return status, json.loads(payload.decode("utf-8"))
    except Exception:
        return status, payload


def wait_for_health(timeout: float = 45.0) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            status, body = request("GET", "/healthz")
            if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
                return
        except Exception as exc:  # noqa: BLE001 - service may still start
            last = exc
        time.sleep(0.4)
    raise RuntimeError(f"service did not become healthy: {last}")


def edge(id_, s, t, c):
    return {"id": id_, "source": s, "target": t, "cost": c}


def sample_payload() -> dict[str, Any]:
    """A solvable instance with a non-trivial canonical witness."""
    return {
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


def poll_job(operation_id: str, timeout: float = 90.0) -> tuple[dict, list[str]]:
    """Poll until a terminal status; return (job document, seen statuses)."""
    deadline = time.time() + timeout
    seen: list[str] = []
    while time.time() < deadline:
        status, body = request("GET", f"/api/audit/jobs/{operation_id}")
        if status == 200 and isinstance(body, dict):
            seen.append(body.get("status"))
            if body.get("status") in ("succeeded", "failed"):
                return body, seen
        time.sleep(0.3)
    raise RuntimeError(
        f"job {operation_id} did not reach a terminal state; seen={seen}"
    )


def check_result_complete(result: Any, label: str) -> None:
    """A published result must be the complete audit document -- never a
    partial edge set or a partial adjacency list."""
    ok = (
        isinstance(result, dict)
        and isinstance(result.get("cost"), int)
        and isinstance(result.get("edge_set"), list)
        and isinstance(result.get("edges"), list)
        and isinstance(result.get("adjacency"), dict)
    )
    check(ok, f"{label}: result document complete", str(result)[:200])
    if not ok:
        return
    ids_from_edges = sorted(e["id"] for e in result["edges"])
    check(
        result["edge_set"] == ids_from_edges,
        f"{label}: edge_set consistent with edges",
        f"{result['edge_set']} vs {ids_from_edges}",
    )
    check(
        sum(e["cost"] for e in result["edges"]) == result["cost"],
        f"{label}: edge costs sum to cost",
    )
    # Adjacency induced exactly by the chosen edges: symmetric and complete.
    symmetric = all(
        any(a["edge"] == e["id"] and a["to"] == other
            for a in result["adjacency"].get(self_, []))
        for e in result["edges"]
        for self_, other in (
            (e["source"], e["target"]),
            (e["target"], e["source"]),
        )
    )
    check(symmetric, f"{label}: adjacency induced and symmetric")


# --- Docker API over the unix socket (for the simulated restart) ----------

class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str) -> None:
        super().__init__("localhost")
        self._socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self._socket_path)


def _docker_api(method: str, path: str) -> tuple[int, bytes]:
    conn = _UnixHTTPConnection(DOCKER_SOCKET)
    try:
        conn.request(method, path)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def docker_available() -> bool:
    if not os.path.exists(DOCKER_SOCKET):
        return False
    try:
        status, _ = _docker_api("GET", "/_ping")
        return status == 200
    except OSError:
        return False


def find_api_container() -> str | None:
    filters = urllib.parse.quote(
        json.dumps({"label": ["com.docker.compose.service=api"]})
    )
    status, body = _docker_api("GET", f"/containers/json?filters={filters}")
    if status != 200:
        return None
    containers = json.loads(body)
    return containers[0]["Id"] if containers else None


def kill_api_container() -> bool:
    """SIGKILL the api container, simulating a mid-computation crash.

    Compose's ``restart: unless-stopped`` policy brings it back; the named
    volume keeps the job database.
    """
    cid = find_api_container()
    if not cid:
        return False
    status, _ = _docker_api("POST", f"/containers/{cid}/kill?signal=SIGKILL")
    return status == 204


def main() -> int:
    print(f"job smoke target: {BASE_URL}")
    wait_for_health()
    run = uuid.uuid4().hex[:8]

    # --- 1. submit -> poll -> result identical to the synchronous audit ---
    print("[1] submit and poll lifecycle")
    payload = sample_payload()
    op1 = f"op-{run}-basic"
    status, body = request(
        "POST", "/api/audit/jobs", {"operation_id": op1, **payload}
    )
    check(status == 202, "submit -> 202 Accepted", f"got {status}: {body}")
    check(
        isinstance(body, dict)
        and body.get("operation_id") == op1
        and body.get("status") in ("queued", "running"),
        "submit returns trackable job identity",
        str(body),
    )

    sync_status, sync_body = request("POST", "/api/audit", payload)
    check(sync_status == 200, "synchronous audit still works",
          f"got {sync_status}")

    job, seen = poll_job(op1)
    check(job["status"] == "succeeded", "job succeeded", str(job)[:200])
    check(
        all(s in ("queued", "running", "succeeded") for s in seen),
        "only documented statuses observed while polling",
        str(seen),
    )
    check(
        job.get("result") == sync_body,
        "job result identical to synchronous audit",
        f"{json.dumps(job.get('result'), sort_keys=True)[:160]} vs "
        f"{json.dumps(sync_body, sort_keys=True)[:160]}",
    )
    check_result_complete(job.get("result"), "job[1]")

    # --- 2. idempotent replay (same id + same payload) --------------------
    print("[2] idempotent replay returns the same job")
    # Same payload, different JSON key order on the wire.
    reordered = {
        "edges": payload["edges"],
        "operation_id": op1,
        "endpoints": payload["endpoints"],
        "nodes": payload["nodes"],
    }
    status, body = request("POST", "/api/audit/jobs", reordered)
    check(status == 200, "replay -> 200 (existing job)", f"got {status}: {body}")
    check(
        isinstance(body, dict)
        and body.get("operation_id") == op1
        and body.get("status") == "succeeded"
        and body.get("result") == sync_body,
        "replay points at the same finished job",
        str(body)[:200],
    )

    # --- 3. concurrent duplicate submissions ------------------------------
    print("[3] concurrent duplicate submissions")
    op3 = f"op-{run}-race"
    results: list[tuple[int, Any]] = []
    lock = threading.Lock()

    def submit() -> None:
        outcome = request(
            "POST", "/api/audit/jobs", {"operation_id": op3, **payload}
        )
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    codes = [s for s, _ in results]
    check(codes.count(202) == 1, "exactly one submission created the job",
          str(codes))
    check(codes.count(200) == len(threads) - 1,
          "the rest replayed the same job", str(codes))
    check(
        all(b.get("operation_id") == op3 for _, b in results
            if isinstance(b, dict)),
        "every response names the same operation id",
    )
    job3, _ = poll_job(op3)
    check(
        job3["status"] == "succeeded" and job3.get("result") == sync_body,
        "concurrent job concludes with the same result",
        str(job3)[:200],
    )

    # --- 4. conflict: id reused with a different payload ------------------
    print("[4] operation id conflict")
    different = {
        "nodes": ["A", "B"],
        "edges": [edge("only", "A", "B", 7)],
        "endpoints": ["A", "B"],
    }
    status, body = request(
        "POST", "/api/audit/jobs", {"operation_id": op1, **different}
    )
    check(status == 409, "id reuse with different payload -> 409",
          f"got {status}: {body}")
    check(
        isinstance(body, dict) and body.get("code") == "OPERATION_ID_CONFLICT",
        "stable conflict error code",
        str(body),
    )
    # Array order is significant: same edges in a different order is a
    # different payload and must be rejected just as stably.
    shuffled = dict(payload)
    shuffled["edges"] = list(reversed(payload["edges"]))
    status, body = request(
        "POST", "/api/audit/jobs", {"operation_id": op1, **shuffled}
    )
    check(status == 409, "reordered edge array also conflicts",
          f"got {status}: {body}")
    # The stored job was never overwritten.
    status, body = request("GET", f"/api/audit/jobs/{op1}")
    check(
        status == 200
        and body.get("status") == "succeeded"
        and body.get("result") == sync_body,
        "original job untouched by conflicting submissions",
        str(body)[:200],
    )

    # --- 5. validation happens before persistence -------------------------
    print("[5] validation before persistence")
    op5 = f"op-{run}-invalid"
    status, body = request("POST", "/api/audit/jobs", {
        "operation_id": op5,
        "nodes": ["A", "B"],
        "edges": [edge("e1", "A", "B", -3)],
        "endpoints": ["A", "B"],
    })
    check(status == 400 and body.get("code") == "NON_POSITIVE_COST",
          "invalid payload -> same 400 as sync endpoint", str(body))
    check(body.get("pointer") == "/edges/0/cost", "locatable pointer kept",
          str(body))
    status, body = request("GET", f"/api/audit/jobs/{op5}")
    check(status == 404 and body.get("code") == "JOB_NOT_FOUND",
          "invalid submission persisted no job", f"got {status}: {body}")

    status, body = request("POST", "/api/audit/jobs", {
        "operation_id": f"op-{run}-unconnected",
        "nodes": ["A", "B", "C"],
        "edges": [edge("e1", "A", "B", 1)],
        "endpoints": ["A", "C"],
    })
    check(status == 422 and body.get("code") == "DANGLING_ENDPOINT",
          "topology errors -> same 422 as sync endpoint", str(body))

    status, body = request("POST", "/api/audit/jobs", payload)
    check(status == 400 and body.get("code") == "MISSING_FIELD",
          "missing operation_id -> 400", str(body))
    status, body = request("POST", "/api/audit/jobs",
                           {"operation_id": "bad id!", **payload})
    check(status == 400 and body.get("code") == "INVALID_OPERATION_ID",
          "illegal operation_id -> 400", str(body))

    # --- 6. crash recovery: SIGKILL mid-flight, then poll the verdict -----
    print("[6] crash recovery (simulated restart)")
    if not docker_available():
        print("  skip  docker socket unavailable; restart checks skipped "
              "(run under docker compose to execute them)")
    else:
        op6 = f"op-{run}-restart"
        status, body = request(
            "POST", "/api/audit/jobs", {"operation_id": op6, **payload}
        )
        check(status == 202, "pre-crash submit -> 202", f"got {status}: {body}")

        check(kill_api_container(), "api container killed (SIGKILL)")
        wait_for_health()
        print("  pass  api healthy again after restart")

        job6, seen6 = poll_job(op6, timeout=120.0)
        check(
            job6["status"] == "succeeded",
            "interrupted job recomputed to success after restart",
            str(job6)[:200],
        )
        check(
            job6.get("result") == sync_body,
            "post-restart result identical to synchronous audit",
            str(job6)[:200],
        )
        check_result_complete(job6.get("result"), "job[6]")

        # Idempotency survives the restart: replay returns the same job,
        # and the conflict guard still protects the stored record.
        status, body = request(
            "POST", "/api/audit/jobs", {"operation_id": op6, **payload}
        )
        check(
            status == 200
            and body.get("status") == "succeeded"
            and body.get("result") == sync_body,
            "replay after restart returns the same job",
            f"got {status}: {str(body)[:200]}",
        )
        status, body = request(
            "POST", "/api/audit/jobs", {"operation_id": op6, **different}
        )
        check(status == 409, "conflict still rejected after restart",
              f"got {status}: {body}")

    # --- 7. synchronous endpoint unaffected --------------------------------
    print("[7] synchronous endpoint semantics unchanged")
    status, body = request("POST", "/api/audit", payload)
    check(status == 200 and body == sync_body,
          "sync audit deterministic alongside jobs", f"got {status}")
    status, body = request("GET", "/api/audit")
    check(status == 405, "GET /api/audit still 405", f"got {status}")
    status, body = request("GET", "/api/audit/jobs/op-does-not-exist")
    check(status == 404 and body.get("code") == "JOB_NOT_FOUND",
          "unknown job -> 404", f"got {status}: {body}")

    print("-" * 60)
    if _failures:
        print(f"job smoke tests: {len(_failures)} FAILED")
        for f in _failures:
            print("  -", f)
        return 1
    print("job smoke tests: all passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
