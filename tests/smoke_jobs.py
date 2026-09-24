"""HTTP smoke tests for the asynchronous audit job API.

Zero third-party dependencies (urllib only). Covers:
  * submit -> poll lifecycle: queued/running -> succeeded, and the result
    document is identical to the synchronous POST /api/audit response;
  * idempotent replay (same operation id + identical payload -> same job),
    including concurrent duplicate submissions;
  * stable 409 rejection when the operation id is reused with a different
    payload, leaving the original job untouched;
  * failed jobs: topology errors surface as the terminal ``failed`` state
    carrying the same error document the sync endpoint would return;
  * submit-time validation identical to the sync endpoint (400 codes and
    JSON pointers), unknown/invalid operation ids, method routing.

Exits non-zero on the first failed check, printing a readable report.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

BASE_URL = os.environ.get("AUDIT_BASE_URL", "http://127.0.0.1:8080")

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


def wait_for_health(timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            status, body = request("GET", "/healthz")
            if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
                print("healthz ok")
                return
        except Exception as exc:  # noqa: BLE001 - service may still start
            last = exc
        time.sleep(0.4)
    raise RuntimeError(f"service did not become healthy: {last}")


def edge(id_, s, t, c):
    return {"id": id_, "source": s, "target": t, "cost": c}


def poll_job(op_id: str, timeout: float = 30.0) -> tuple[int, Any]:
    """Poll until the job reaches a terminal state; return the last reply."""
    deadline = time.time() + timeout
    status, body = request("GET", f"/api/jobs/{op_id}")
    while time.time() < deadline:
        status, body = request("GET", f"/api/jobs/{op_id}")
        if (
            status == 200
            and isinstance(body, dict)
            and body.get("status") in ("succeeded", "failed")
        ):
            return status, body
        # Non-terminal states must never expose result or error fields.
        check(
            status == 200
            and isinstance(body, dict)
            and body.get("status") in ("queued", "running")
            and "result" not in body
            and "error" not in body,
            f"intermediate state clean ({body.get('status') if isinstance(body, dict) else status})",
            str(body),
        )
        time.sleep(0.2)
    return status, body


def unique_op(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


PAYLOAD = {
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


def main() -> int:
    print(f"async smoke target: {BASE_URL}")
    wait_for_health()

    # --- 1. submit -> poll -> result identical to the sync endpoint ------
    print("[1] async result identical to synchronous audit")
    op = unique_op("smoke-ok")
    status, body = request("POST", "/api/jobs",
                           {"operation_id": op, "payload": PAYLOAD})
    check(status == 202, "submit -> 202 Accepted", f"got {status}: {body}")
    check(
        isinstance(body, dict)
        and body.get("operation_id") == op
        and body.get("status") in ("queued", "running"),
        "submit returns the queued job",
        str(body),
    )
    check("result" not in body and "error" not in body,
          "no result/error fields before completion", str(body))

    status, sync_body = request("POST", "/api/audit", PAYLOAD)
    check(status == 200, "sync baseline 200", f"got {status}")

    status, job = poll_job(op)
    check(status == 200 and job.get("status") == "succeeded",
          "job succeeded", str(job))
    if isinstance(job, dict) and job.get("status") == "succeeded":
        check(job.get("result") == sync_body,
              "async result identical to sync response",
              f"{job.get('result')} != {sync_body}")
        check(set(job.keys()) == {"operation_id", "status", "result"},
              "succeeded document shape", str(sorted(job.keys())))

    # --- 2. idempotent replay ---------------------------------------------
    print("[2] idempotent replay returns the same job")
    status, replay = request("POST", "/api/jobs",
                             {"operation_id": op, "payload": PAYLOAD})
    check(status == 200, "replay -> 200", f"got {status}: {replay}")
    check(
        isinstance(replay, dict)
        and replay.get("operation_id") == op
        and replay.get("status") == "succeeded"
        and replay.get("result") == sync_body,
        "replay returns the original completed job",
        str(replay),
    )

    # --- 3. concurrent duplicate submissions ------------------------------
    print("[3] concurrent identical submissions -> one job")
    op_c = unique_op("smoke-conc")
    responses: list[tuple[int, Any]] = []
    barrier = threading.Barrier(6)

    def submit_dup():
        barrier.wait()
        responses.append(request(
            "POST", "/api/jobs", {"operation_id": op_c, "payload": PAYLOAD}))

    threads = [threading.Thread(target=submit_dup) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check(all(s in (200, 202) for s, _ in responses),
          "all concurrent submits accepted",
          str([s for s, _ in responses]))
    check(all(isinstance(b, dict) and b.get("operation_id") == op_c
              for _, b in responses),
          "all responses reference the same operation id")
    status, job = poll_job(op_c)
    check(job.get("status") == "succeeded" and job.get("result") == sync_body,
          "concurrent job converges to the same result", str(job))

    # --- 4. id reuse with a different payload -> stable 409 ---------------
    print("[4] operation id conflict")
    changed = dict(PAYLOAD)
    changed["edges"] = PAYLOAD["edges"][:-1]
    status, conflict = request(
        "POST", "/api/jobs", {"operation_id": op, "payload": changed})
    check(status == 409, "conflict -> 409", f"got {status}: {conflict}")
    check(isinstance(conflict, dict)
          and conflict.get("code") == "OPERATION_CONFLICT"
          and conflict.get("pointer") == "/operation_id",
          "stable conflict error", str(conflict))
    # The original record is untouched and the rejection is stable.
    status, again = request(
        "POST", "/api/jobs", {"operation_id": op, "payload": changed})
    check(status == 409, "conflict is stable on retry", f"got {status}")
    _, job = request("GET", f"/api/jobs/{op}")
    check(job.get("status") == "succeeded" and job.get("result") == sync_body,
          "original job not overwritten", str(job))

    # --- 5. failed job carries the sync error document --------------------
    print("[5] failed job state")
    op_f = unique_op("smoke-fail")
    split_payload = {
        "nodes": ["A", "B", "C", "D"],
        "edges": [edge("e1", "A", "B", 1), edge("e2", "C", "D", 1)],
        "endpoints": ["A", "C"],
    }
    status, sync_err = request("POST", "/api/audit", split_payload)
    check(status == 422 and sync_err.get("code") == "ENDPOINTS_UNCONNECTED",
          "sync baseline 422", f"got {status}: {sync_err}")
    status, _ = request("POST", "/api/jobs",
                        {"operation_id": op_f, "payload": split_payload})
    check(status == 202, "infeasible payload still accepted", f"got {status}")
    _, job = poll_job(op_f)
    check(job.get("status") == "failed", "job failed", str(job))
    check(job.get("error") == sync_err,
          "failed error identical to sync 422 body",
          f"{job.get('error')} != {sync_err}")
    check("result" not in job, "no result on failure", str(job))
    # Replaying the same submission returns the same failed job.
    status, replay = request("POST", "/api/jobs",
                             {"operation_id": op_f, "payload": split_payload})
    check(status == 200 and replay.get("status") == "failed",
          "failed job replay returns the same job", str(replay))

    # --- 6. submit-time validation mirrors the sync endpoint --------------
    print("[6] submit-time validation")
    bad_cost = {
        "nodes": ["A", "B"],
        "edges": [edge("e1", "A", "B", 0)],
        "endpoints": ["A", "B"],
    }
    status, body = request("POST", "/api/jobs",
                           {"operation_id": unique_op("smoke-bad"),
                            "payload": bad_cost})
    check(status == 400 and body.get("code") == "NON_POSITIVE_COST"
          and body.get("pointer") == "/edges/0/cost",
          "invalid payload -> sync-shaped 400", str(body))

    status, body = request("POST", "/api/jobs", {"payload": PAYLOAD})
    check(status == 400 and body.get("code") == "MISSING_FIELD"
          and body.get("pointer") == "/operation_id",
          "missing operation_id -> 400", str(body))

    status, body = request("POST", "/api/jobs",
                           {"operation_id": unique_op("smoke-nopayload")})
    check(status == 400 and body.get("code") == "MISSING_FIELD"
          and body.get("pointer") == "/payload",
          "missing payload -> 400", str(body))

    status, body = request("POST", "/api/jobs",
                           {"operation_id": "bad id/with slashes",
                            "payload": PAYLOAD})
    check(status == 400 and body.get("code") == "INVALID_OPERATION_ID",
          "bad operation id charset -> 400", str(body))

    status, body = request("POST", "/api/jobs", raw=b"{not json")
    check(status == 400 and body.get("code") == "MALFORMED_JSON",
          "malformed JSON -> 400", str(body))

    status, body = request("POST", "/api/jobs",
                           {"operation_id": unique_op("smoke-notobj"),
                            "payload": [1, 2, 3]})
    check(status == 400 and body.get("code") == "INVALID_BODY",
          "non-object payload -> 400", str(body))

    # --- 7. query routing ---------------------------------------------------
    print("[7] query routing")
    status, body = request("GET", f"/api/jobs/{unique_op('smoke-absent')}")
    check(status == 404 and body.get("code") == "JOB_NOT_FOUND",
          "unknown operation id -> 404", str(body))

    status, body = request("GET", "/api/jobs/not%20a%20valid%20id")
    check(status == 404 and body.get("code") == "JOB_NOT_FOUND",
          "malformed operation id -> 404", str(body))

    status, _ = request("GET", "/api/jobs")
    check(status == 405, "GET /api/jobs -> 405", f"got {status}")

    status, _ = request("POST", f"/api/jobs/{unique_op('smoke-method')}")
    check(status == 405, "POST /api/jobs/{id} -> 405", f"got {status}")

    print("-" * 60)
    if _failures:
        print(f"async smoke tests: {len(_failures)} FAILED")
        for f in _failures:
            print("  -", f)
        return 1
    print("async smoke tests: all passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
