"""HTTP API built only on the Python standard library.

Endpoints
---------
* ``GET  /healthz``                  -- liveness probe used by Compose.
* ``POST /api/audit``                -- validate an instance, solve the
  minimum-cost subnet joining every calibration endpoint, and return the
  canonical edge set plus the adjacency list it induces (synchronous).
* ``POST /api/jobs``                 -- submit the same audit asynchronously:
  ``{"operation_id": ..., "payload": {...}}``; fully validates the payload,
  durably persists a queued job and returns it. Idempotent on
  ``operation_id``: replaying an identical submission returns the existing
  job, a different payload is rejected with 409.
* ``GET  /api/jobs/{operation_id}``  -- poll the job state: ``queued``,
  ``running``, ``succeeded`` (with the audit result) or ``failed`` (with
  the stable error document).

Failures always respond with a complete, stable error document
(``{"code", "message", "pointer"?}``); a failed audit never returns a partial
subnet and never reuses state from a previous request -- every request builds
a fresh problem and solver run.
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .audit import solve_payload
from .errors import TopologyError, ValidationError
from .jobs import (
    DEFAULT_JOB_DB,
    OPERATION_ID_RE,
    JobConflict,
    JobRunner,
    JobStore,
    canonical_payload,
    job_document,
    payload_digest,
    validate_operation_id,
)
from .validation import parse_problem

MAX_BODY_BYTES = 2_000_000

JOBS_PREFIX = "/api/jobs/"


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

    def _read_json_body(self) -> tuple[bool, Any]:
        """Read and parse the request body.

        Returns ``(True, payload)``; on failure the error response has
        already been sent and the result is ``(False, None)``.
        """
        length_hdr = self.headers.get("Content-Length")
        try:
            length = int(length_hdr) if length_hdr is not None else -1
        except ValueError:
            self._send_error(
                400, "INVALID_CONTENT_LENGTH", "Content-Length must be an integer"
            )
            return False, None
        if length < 0:
            self._send_error(411, "LENGTH_REQUIRED", "Content-Length is required")
            return False, None
        if length > MAX_BODY_BYTES:
            self._send_error(
                413,
                "PAYLOAD_TOO_LARGE",
                f"request body exceeds {MAX_BODY_BYTES} bytes",
            )
            return False, None

        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except UnicodeDecodeError:
            self._send_error(400, "MALFORMED_JSON", "request body must be UTF-8 JSON")
            return False, None
        except json.JSONDecodeError as exc:
            self._send_error(
                400,
                "MALFORMED_JSON",
                f"request body is not valid JSON: {exc.msg}",
            )
            return False, None
        return True, payload

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
        if path == "/api/jobs":
            self._send_error(
                405,
                "METHOD_NOT_ALLOWED",
                "POST /api/jobs with a JSON body",
            )
            return
        if path.startswith(JOBS_PREFIX):
            self._handle_job_query(path[len(JOBS_PREFIX):])
            return
        self._send_error(404, "NOT_FOUND", f"unknown path: {self.path}")

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/api/audit":
            self._handle_audit()
            return
        if path == "/api/jobs":
            self._handle_job_submit()
            return
        if path.startswith(JOBS_PREFIX):
            self._send_error(
                405,
                "METHOD_NOT_ALLOWED",
                "GET /api/jobs/{operation_id} to poll a job",
            )
            return
        self._send_error(404, "NOT_FOUND", f"unknown path: {self.path}")

    # --- synchronous audit ----------------------------------------------

    def _handle_audit(self) -> None:
        ok, payload = self._read_json_body()
        if not ok:
            return
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

    # --- asynchronous jobs ------------------------------------------------

    def _handle_job_submit(self) -> None:
        ok, body = self._read_json_body()
        if not ok:
            return
        try:
            if not isinstance(body, dict):
                raise ValidationError(
                    "INVALID_BODY", "request body must be a JSON object"
                )
            if "operation_id" not in body:
                raise ValidationError(
                    "MISSING_FIELD", "operation_id is required", "/operation_id"
                )
            operation_id = validate_operation_id(body["operation_id"])
            if "payload" not in body:
                raise ValidationError(
                    "MISSING_FIELD", "payload is required", "/payload"
                )
            payload = body["payload"]
            # Fully validate the payload BEFORE anything is persisted; the
            # error documents are identical to the synchronous endpoint.
            parse_problem(payload)

            canonical = canonical_payload(payload)
            record, created = self.server.job_store.submit(
                operation_id, canonical, payload_digest(canonical)
            )
            if created:
                self.server.job_runner.submit(operation_id)
                self._send_json(202, job_document(record))
            else:
                self._send_json(200, job_document(record))
        except ValidationError as exc:
            self._send_json(400, exc.to_dict())
        except JobConflict as exc:
            self._send_json(
                409,
                {
                    "code": "OPERATION_CONFLICT",
                    "message": "operation_id "
                    f"{exc.operation_id!r} is already bound to a different "
                    "payload; the original job is unchanged",
                    "pointer": "/operation_id",
                },
            )
        except Exception:
            # Log internally but never leak internals or a partial subnet.
            import traceback

            traceback.print_exc()
            self._send_json(
                500,
                {
                    "code": "INTERNAL_ERROR",
                    "message": "failed to register the audit job",
                },
            )

    def _handle_job_query(self, operation_id: str) -> None:
        try:
            record = None
            if OPERATION_ID_RE.fullmatch(operation_id) is not None:
                record = self.server.job_store.get(operation_id)
            if record is None:
                self._send_error(
                    404,
                    "JOB_NOT_FOUND",
                    f"unknown operation id: {operation_id}",
                )
                return
            self._send_json(200, job_document(record))
        except Exception:
            import traceback

            traceback.print_exc()
            self._send_json(
                500,
                {
                    "code": "INTERNAL_ERROR",
                    "message": "failed to read the audit job",
                },
            )


class AuditHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    # Field engineers may retry in bursts after a dropped connection; keep
    # the listen backlog comfortably larger than the socketserver default.
    request_queue_size = 64


def build_server(
    host: str,
    port: int,
    job_store: JobStore | None = None,
    job_runner: JobRunner | None = None,
) -> ThreadingHTTPServer:
    httpd = AuditHTTPServer((host, port), AuditHandler)
    httpd.job_store = job_store
    httpd.job_runner = job_runner
    return httpd


def _job_delay_from_env() -> float:
    raw = os.environ.get("AUDIT_JOB_DELAY_SECONDS", "0")
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        print(f"[audit] ignoring invalid AUDIT_JOB_DELAY_SECONDS={raw!r}")
        return 0.0


def main() -> None:
    host = os.environ.get("AUDIT_HOST", "0.0.0.0")
    port = int(os.environ.get("AUDIT_PORT", "8080"))
    store = JobStore(os.environ.get("AUDIT_JOB_DB", DEFAULT_JOB_DB))
    runner = JobRunner(store, start_delay=_job_delay_from_env())
    recovered = runner.recover()
    if recovered:
        print(f"[audit] recovered {recovered} interrupted job(s)")
    httpd = build_server(host, port, store, runner)
    print(f"[audit] listening on {host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
