"""HTTP API built only on the Python standard library.

Endpoints
---------
* ``GET  /healthz`` -- liveness probe used by Compose.
* ``POST /api/audit`` -- validate an instance, solve the minimum-cost subnet
  joining every calibration endpoint, and return the canonical edge set plus
  the adjacency list it induces (synchronous).
* ``POST /api/audit/jobs`` -- same validation, but the audit is persisted as
  a durable job under a caller-supplied ``operation_id`` and solved
  asynchronously; safe to retry, safe across restarts.
* ``GET  /api/audit/jobs/<operation_id>`` -- poll a job: ``queued``,
  ``running``, ``succeeded`` (with the exact document the synchronous
  endpoint would have returned) or ``failed`` (with a stable error body).

Failures always respond with a complete, stable error document
(``{"code", "message", "pointer"?}``); a failed audit never returns a partial
subnet and never reuses state from a previous request -- every request builds
a fresh problem and solver run. See ``app/jobs.py`` for the idempotency and
crash-recovery guarantees of the asynchronous endpoints.
"""

from __future__ import annotations

import json
import os
import re
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .errors import TopologyError, ValidationError
from .jobs import JobStore, JobWorker, canonical_payload, job_document, payload_digest
from .solver import check_feasibility, solve_payload
from .validation import parse_problem

MAX_BODY_BYTES = 2_000_000

# Operation identifiers appear in the poll URL, so keep them URL-safe.
_OPERATION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,127}\Z")
_JOBS_PREFIX = "/api/audit/jobs"

# _read_json_body sentinel: distinguishes "error already sent" from a
# legitimately decoded JSON null.
_BODY_REJECTED = object()


def _parse_operation_id(payload: dict[str, Any]) -> str:
    if "operation_id" not in payload:
        raise ValidationError(
            "MISSING_FIELD", "operation_id is required", "/operation_id"
        )
    value = payload["operation_id"]
    if not isinstance(value, str) or not _OPERATION_ID_RE.match(value):
        raise ValidationError(
            "INVALID_OPERATION_ID",
            "operation_id must be 1-128 characters from [A-Za-z0-9._~-] "
            "starting with a letter or digit",
            "/operation_id",
        )
    return value


class AuditHandler(BaseHTTPRequestHandler):
    server_version = "CalibrationAudit/1.0"

    # --- helpers -------------------------------------------------------

    def _send_json(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_error(
        self, status: int, code: str, message: str, pointer: str = ""
    ) -> None:
        body: dict[str, Any] = {"code": code, "message": message}
        if pointer:
            body["pointer"] = pointer
        self._send_json(status, body)

    def _read_json_body(self) -> Any:
        """Read and decode the request body, or send the error and return
        the ``_BODY_REJECTED`` sentinel."""
        length_hdr = self.headers.get("Content-Length")
        try:
            length = int(length_hdr) if length_hdr is not None else -1
        except ValueError:
            self._send_error(
                400, "INVALID_CONTENT_LENGTH", "Content-Length must be an integer"
            )
            return _BODY_REJECTED
        if length < 0:
            self._send_error(411, "LENGTH_REQUIRED", "Content-Length is required")
            return _BODY_REJECTED
        if length > MAX_BODY_BYTES:
            self._send_error(
                413,
                "PAYLOAD_TOO_LARGE",
                f"request body exceeds {MAX_BODY_BYTES} bytes",
            )
            return _BODY_REJECTED

        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except UnicodeDecodeError:
            self._send_error(400, "MALFORMED_JSON", "request body must be UTF-8 JSON")
            return _BODY_REJECTED
        except json.JSONDecodeError as exc:
            self._send_error(
                400,
                "MALFORMED_JSON",
                f"request body is not valid JSON: {exc.msg}",
            )
            return _BODY_REJECTED

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep stderr concise; verification relies on exit codes, not logs.
        # log_request passes (requestline, status, size) as format args.
        status = args[1] if len(args) > 1 else "-"
        print(f'[audit] {self.address_string()} "{self.requestline}" {status}')

    # --- routes --------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            self._send_json(200, {"status": "ok"})
            return
        if path == "/api/audit":
            self._send_error(
                405,
                "METHOD_NOT_ALLOWED",
                "POST /api/audit with a JSON body",
            )
            return
        if path == _JOBS_PREFIX:
            self._send_error(
                405,
                "METHOD_NOT_ALLOWED",
                "POST /api/audit/jobs with a JSON body; "
                "GET /api/audit/jobs/<operation_id> to poll",
            )
            return
        if path.startswith(_JOBS_PREFIX + "/"):
            self._handle_get_job(path[len(_JOBS_PREFIX) + 1:])
            return
        self._send_error(404, "NOT_FOUND", f"unknown path: {self.path}")

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/api/audit":
            payload = self._read_json_body()
            if payload is _BODY_REJECTED:
                return
            self._handle_sync_audit(payload)
            return
        if path == _JOBS_PREFIX:
            payload = self._read_json_body()
            if payload is _BODY_REJECTED:
                return
            self._handle_submit_job(payload)
            return
        self._send_error(404, "NOT_FOUND", f"unknown path: {self.path}")

    # --- handlers ------------------------------------------------------

    def _handle_sync_audit(self, payload: Any) -> None:
        try:
            result = solve_payload(payload)
        except ValidationError as exc:
            self._send_json(400, exc.to_dict())
            return
        except TopologyError as exc:
            self._send_json(422, exc.to_dict())
            return
        except Exception:
            # Log internally but never leak internals or a partial subnet.
            import traceback

            traceback.print_exc()
            self._send_json(
                500,
                {
                    "code": "INTERNAL_ERROR",
                    "message": "solver failed while computing the subnet",
                },
            )
            return

        self._send_json(200, result)

    def _handle_submit_job(self, payload: Any) -> None:
        store: JobStore | None = getattr(self.server, "job_store", None)
        worker: JobWorker | None = getattr(self.server, "job_worker", None)
        if store is None or worker is None:
            self._send_error(
                503, "JOBS_UNAVAILABLE", "job store is not configured"
            )
            return

        # Fully validate BEFORE anything is persisted: the operation id
        # shape, then the audit payload with exactly the same checks (and
        # error documents) as the synchronous endpoint.
        try:
            if not isinstance(payload, dict):
                raise ValidationError(
                    "INVALID_BODY", "request body must be a JSON object"
                )
            operation_id = _parse_operation_id(payload)
            problem = parse_problem(payload)
            check_feasibility(problem)
        except ValidationError as exc:
            self._send_json(400, exc.to_dict())
            return
        except TopologyError as exc:
            self._send_json(422, exc.to_dict())
            return

        canonical = canonical_payload(payload)
        digest = payload_digest(canonical)
        job, created = store.create_or_get(operation_id, canonical, digest)
        if job["payload_hash"] != digest:
            # Id reused with a different payload: reject stably and leave
            # the stored job untouched.
            self._send_error(
                409,
                "OPERATION_ID_CONFLICT",
                "operation_id was already submitted with a different "
                "payload; the original job is unchanged",
                "/operation_id",
            )
            return
        if created:
            worker.submit(operation_id)
        self._send_json(202 if created else 200, job_document(job))

    def _handle_get_job(self, raw_operation_id: str) -> None:
        store: JobStore | None = getattr(self.server, "job_store", None)
        if store is None:
            self._send_error(
                503, "JOBS_UNAVAILABLE", "job store is not configured"
            )
            return
        operation_id = urllib.parse.unquote(raw_operation_id)
        job = (
            store.get(operation_id)
            if operation_id and "/" not in operation_id
            else None
        )
        if job is None:
            self._send_error(
                404,
                "JOB_NOT_FOUND",
                f"no job for operation id {operation_id!r}",
            )
            return
        self._send_json(200, job_document(job))


def build_server(
    host: str,
    port: int,
    store: JobStore | None = None,
    worker: JobWorker | None = None,
) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), AuditHandler)
    httpd.job_store = store
    httpd.job_worker = worker
    return httpd


def _default_data_dir() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, "data")


def main() -> None:
    host = os.environ.get("AUDIT_HOST", "0.0.0.0")
    port = int(os.environ.get("AUDIT_PORT", "8080"))
    data_dir = os.environ.get("AUDIT_DATA_DIR", _default_data_dir())
    store = JobStore(os.path.join(data_dir, "jobs.sqlite3"))
    worker = JobWorker(store)
    worker.start()
    httpd = build_server(host, port, store, worker)
    print(f"[audit] listening on {host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
