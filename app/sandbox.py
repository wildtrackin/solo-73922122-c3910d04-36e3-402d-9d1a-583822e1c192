"""API-side client for the out-of-process runner supervisor.

The API worker never spawns plugin runners itself.  It sends each validated
request to the supervisor (``app/supervisor.py``) over a filesystem-scoped
Unix domain socket and polls for the result.  This module preserves the old
in-process ``execute_plugin`` contract used by ``app/main.py`` -- including the
exact error codes -- while the process lifecycle lives outside the API.

Concurrency model: every in-flight request owns its own short-lived socket
connection (a small pool guarded by a semaphore), so requests never share a
buffer or a lock and cannot block one another.  The supervisor accepts many
simultaneous connections and keys every run by a random id.

Operational consequences:

* if the API is stopped with SIGSTOP, polling pauses but the supervisor keeps
  enforcing the 2 second runner wall clock; when the API resumes, the finished
  run is reported as a timeout and no partial output is returned;
* if the API is SIGKILLed, the supervisor -- which owns every runner -- kills
  and reaps them and removes their staging directories on its own;
* a fresh API process starts with an empty client, so it can never address a
  previous instance's run ids or files.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import socket

from .config import CONTROL_SOCKET_PATH, PLUGIN_TIMEOUT_SECONDS
from .errors import ApiError
from .runresult import KILLED_BY_SUPERVISOR, failure_for_returncode

POLL_INTERVAL_SECONDS = 0.025
# The supervisor guarantees a decision within the plugin timeout; the margin
# only covers scheduling and a final authoritative re-read.  If no answer
# arrives, the control plane is treated as unavailable rather than hanging.
POLL_DEADLINE_MARGIN_SECONDS = 10.0
# Bound the number of concurrently open control connections; each is cheap and
# the supervisor's listen backlog plus pids_limit cap the other side anyway.
MAX_CONTROL_CONNECTIONS = 64
CONNECT_TIMEOUT_SECONDS = 5.0
IO_TIMEOUT_SECONDS = 10.0

# Terminal states produced by the supervisor (mirrors app/supervisor.py).
STATE_EXITED = "exited"
STATE_TIMEOUT = "timeout"
STATE_OUTPUT_TOO_LARGE = "output_too_large"
STATE_CANCELLED = "cancelled"
STATE_API_DEAD = "api_dead"
STATE_SHUTDOWN = "shutdown"
STATE_SPAWN_FAILED = "spawn_failed"


class SupervisorUnavailable(ApiError):
    def __init__(self, detail: str) -> None:
        super().__init__(
            503,
            "sandbox_unavailable",
            f"The sandbox supervisor is unavailable: {detail}.",
        )


class ControlConnection:
    """One blocking, single-request-pipeline Unix socket connection."""

    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self._sock: socket.socket | None = None
        self._buffer = bytearray()

    def __enter__(self) -> "ControlConnection":
        self.connect()
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT_SECONDS)
        try:
            sock.connect(self.socket_path)
        except OSError as exc:
            sock.close()
            raise SupervisorUnavailable(
                f"cannot connect to {self.socket_path} ({exc.__class__.__name__})"
            ) from exc
        sock.settimeout(IO_TIMEOUT_SECONDS)
        self._sock = sock
        self._buffer = bytearray()

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None
        self._buffer = bytearray()

    def request(self, payload: dict) -> dict:
        if self._sock is None:
            self.connect()
        frame = json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"
        try:
            self._sock.sendall(frame)
        except OSError as exc:
            self.close()
            raise SupervisorUnavailable(
                f"control write failed ({exc.__class__.__name__})"
            ) from exc

        while b"\n" not in self._buffer:
            try:
                chunk = self._sock.recv(65536)
            except socket.timeout as exc:
                self.close()
                raise SupervisorUnavailable("control read timed out") from exc
            except OSError as exc:
                self.close()
                raise SupervisorUnavailable(
                    f"control read failed ({exc.__class__.__name__})"
                ) from exc
            if not chunk:
                self.close()
                raise SupervisorUnavailable("control connection closed")
            self._buffer.extend(chunk)

        line, _, rest = self._buffer.partition(b"\n")
        del self._buffer[: len(line) + 1]
        try:
            reply = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            self.close()
            raise SupervisorUnavailable("malformed control frame") from exc
        if not isinstance(reply, dict):
            self.close()
            raise SupervisorUnavailable("malformed control frame")
        if reply.get("type") == "error":
            self.close()
            raise SupervisorUnavailable(
                f"supervisor rejected frame: {reply.get('error')}"
            )
        return reply


class SupervisorClient:
    """Factory for per-request control connections, concurrency-safe."""

    def __init__(self, socket_path: str = CONTROL_SOCKET_PATH) -> None:
        self.socket_path = socket_path
        self._slots = asyncio.Semaphore(MAX_CONTROL_CONNECTIONS)

    def _submit(self, conn: ControlConnection, module_bytes: bytes,
                log_bytes: bytes) -> str:
        reply = conn.request(
            {
                "type": "submit",
                "module_b64": base64.b64encode(module_bytes).decode("ascii"),
                "log_b64": base64.b64encode(log_bytes).decode("ascii"),
            }
        )
        if reply.get("type") != "submitted" or "id" not in reply:
            raise SupervisorUnavailable("unexpected submit acknowledgement")
        return str(reply["id"])

    @staticmethod
    def _cancel(conn: ControlConnection, run_id: str) -> None:
        try:
            conn.request({"type": "cancel", "id": run_id})
        except SupervisorUnavailable:
            # The supervisor's own timeout/death sweep is the backstop; a lost
            # cancel must not mask the client-disconnect error.
            pass

    async def execute(
        self, module_bytes: bytes, log_bytes: bytes, request
    ) -> bytes:
        # The semaphore only bounds open fds; it does not serialise requests.
        async with self._slots:
            conn = ControlConnection(self.socket_path)
            try:
                run_id = await asyncio.to_thread(
                    self._submit, conn, module_bytes, log_bytes
                )
                hard_deadline = (
                    asyncio.get_running_loop().time()
                    + PLUGIN_TIMEOUT_SECONDS
                    + POLL_DEADLINE_MARGIN_SECONDS
                )
                while True:
                    frame = await asyncio.to_thread(
                        conn.request, {"type": "poll", "id": run_id}
                    )
                    if frame.get("state") != "unknown" and frame.get("done"):
                        return self._interpret(frame)

                    try:
                        if await request.is_disconnected():
                            await asyncio.to_thread(self._cancel, conn, run_id)
                            raise ApiError(
                                499,
                                "client_disconnected",
                                "The client disconnected before the plugin "
                                "completed.",
                            )
                    except ApiError:
                        raise
                    except Exception:
                        # A probe failure must not disable the timeout backstop.
                        pass

                    if asyncio.get_running_loop().time() > hard_deadline:
                        # One authoritative reading first: after SIGSTOP/SIGCONT
                        # the supervisor has usually finalised the run, and that
                        # result (e.g. a 504 timeout) must win over the backstop.
                        final = await asyncio.to_thread(
                            conn.request, {"type": "poll", "id": run_id}
                        )
                        if final.get("state") != "unknown" and final.get("done"):
                            return self._interpret(final)
                        await asyncio.to_thread(self._cancel, conn, run_id)
                        raise SupervisorUnavailable(
                            "no result within the wait budget"
                        )

                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
            finally:
                conn.close()

    def _interpret(self, frame: dict) -> bytes:
        state = frame.get("state")
        if state == STATE_EXITED and frame.get("returncode") == 0:
            try:
                return base64.b64decode(frame.get("stdout_b64", ""), validate=True)
            except (ValueError, TypeError) as exc:
                raise ApiError(
                    500,
                    "sandbox_internal_error",
                    "The sandbox returned an undecodable result.",
                ) from exc

        if state == STATE_TIMEOUT:
            raise ApiError(
                504,
                "plugin_timeout",
                f"Plugin execution exceeded the {PLUGIN_TIMEOUT_SECONDS:g} second limit.",
            )
        if state == STATE_OUTPUT_TOO_LARGE:
            raise ApiError(
                413,
                "plugin_output_too_large",
                "Plugin stdout and stderr exceeded the combined output limit.",
            )
        if state == STATE_CANCELLED:
            # Only the client-disconnect path requests cancellation; that path
            # raises 499 itself, so reaching here means a supervisor decision.
            raise ApiError(
                503,
                "sandbox_interrupted",
                "The plugin run was terminated before it completed.",
            )
        if state in (STATE_API_DEAD, STATE_SHUTDOWN):
            raise ApiError(
                503,
                "sandbox_interrupted",
                "The sandbox supervisor terminated the run while the API "
                "worker was unavailable; no partial output is returned.",
            )
        if state == STATE_SPAWN_FAILED:
            raise ApiError(
                500,
                "sandbox_internal_error",
                "The sandbox could not start the plugin runner.",
            )

        returncode = frame.get("returncode")
        if isinstance(returncode, int) and returncode != KILLED_BY_SUPERVISOR:
            raise failure_for_returncode(returncode)
        raise ApiError(
            502,
            "sandbox_process_failed",
            "The sandbox runner ended unexpectedly.",
        )

    # Compatibility helper used by the test fixtures.
    def _close(self) -> None:
        # Per-request connections are short-lived; nothing pooled to reset.
        return None


# A single client per API process.  A restarted worker therefore starts with
# no knowledge of any previous worker's runs.
_client = SupervisorClient()


async def execute_plugin(module_bytes: bytes, log_bytes: bytes, request) -> bytes:
    """Run one plugin via the supervisor and return its complete stdout."""
    if not os.path.exists(CONTROL_SOCKET_PATH):
        raise SupervisorUnavailable("control socket is missing")
    return await _client.execute(module_bytes, log_bytes, request)
