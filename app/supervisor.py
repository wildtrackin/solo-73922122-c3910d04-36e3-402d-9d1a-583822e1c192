#!/usr/bin/env python3
"""Out-of-process supervisor for the WASI plugin runners and the API worker.

This process is PID 1 of the container.  The design starts from one fact:
a runner whose parent is the API worker is orphaned the moment the worker is
SIGKILLed, and a SIGSTOPed worker cannot enforce any timeout itself.  So the
API never starts runners -- *this* process does, and every runner is its child.

Responsibilities:

* start (and restart) the single uvicorn API worker as a child process;
* accept run requests from the API over a filesystem-scoped Unix socket and
  spawn one fresh runner interpreter per request in its own process group,
  with a per-run staging directory on tmpfs;
* enforce the 2 second wall-clock limit off the monotonic clock -- this keeps
  working while the API is stopped with SIGSTOP, because this process is not
  stopped;
* on API death (SIGKILL, crash, ...) SIGKILL and reap every in-flight runner
  and remove every staging directory within one second, without waiting for
  the API to come back;
* restart the API afterwards.  Runs are keyed by random ids and staging
  directories by unique names, so a restarted API can neither signal an old
  runner nor touch a new request's files.

Only the Python standard library is used: the supervisor must keep working
even if third-party packages are unavailable, and it has no event loop of its
own that a stopped API could freeze.
"""

from __future__ import annotations

import argparse
import base64
import errno
import json
import os
import select
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .config import (
    CONTROL_SOCKET_PATH,
    MAX_CONTROL_FRAME_BYTES,
    MAX_PLUGIN_OUTPUT_BYTES,
    PLUGIN_TIMEOUT_SECONDS,
    SANDBOX_GID,
    SANDBOX_UID,
    STAGING_PREFIX,
    STAGING_ROOT,
    SUPERVISOR_TICK_SECONDS,
)
from .runresult import KILLED_BY_SUPERVISOR

RUNNER_PATH = Path(__file__).resolve().with_name("runner.py")

# Run lifecycle states.  A run only counts as finished once Run.finalized is
# set (process reaped, drain threads joined, staging directory removed).
STATE_PENDING = "pending"
STATE_RUNNING = "running"
STATE_EXITED = "exited"
STATE_TIMEOUT = "timeout"
STATE_OUTPUT_TOO_LARGE = "output_too_large"
STATE_CANCELLED = "cancelled"
STATE_API_DEAD = "api_dead"
STATE_SHUTDOWN = "shutdown"
STATE_SPAWN_FAILED = "spawn_failed"

TERMINAL_STATES = {
    STATE_EXITED,
    STATE_TIMEOUT,
    STATE_OUTPUT_TOO_LARGE,
    STATE_CANCELLED,
    STATE_API_DEAD,
    STATE_SHUTDOWN,
    STATE_SPAWN_FAILED,
}

# API worker process states.
API_RUNNING = "running"
API_DEAD = "dead"

READ_CHUNK = 4096
API_RESTART_DELAY_SECONDS = 0.1
SHUTDOWN_GRACE_SECONDS = 5.0
DRAIN_JOIN_SECONDS = 2.0
# Finalised runs are retained briefly so a racing poll still gets the result,
# then pruned.  This bounds the supervisor's memory regardless of traffic.
FINALISED_RETENTION_SECONDS = 30.0
FINALISED_HARD_CAP = 1024


def _log(message: str) -> None:
    """Structured-ish log line on stdout; the container keeps this visible."""
    sys.stdout.write(
        f"[supervisor {time.strftime('%H:%M:%S')} pid={os.getpid()}] {message}\n"
    )
    sys.stdout.flush()


@dataclass
class Run:
    """One plugin execution and everything needed to clean it up."""

    run_id: str
    staging_dir: str
    module_path: str
    log_path: str
    created_at: float
    deadline: float
    state: str = STATE_PENDING
    finalized: bool = False
    pid: int | None = None
    process: subprocess.Popen | None = None
    returncode: int | None = None
    stdout: bytearray = field(default_factory=bytearray)
    stderr: bytearray = field(default_factory=bytearray)
    total_output: int = 0
    overflow: bool = False
    finished_at: float | None = None
    error: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    _drain_threads: list[threading.Thread] = field(default_factory=list)
    _pipe_fds: list[int] = field(default_factory=list)


class Supervisor:
    def __init__(
        self,
        socket_path: str = CONTROL_SOCKET_PATH,
        staging_root: str = STAGING_ROOT,
        runner_path: Path = RUNNER_PATH,
        manage_api: bool = True,
        api_command: list[str] | None = None,
    ) -> None:
        self.socket_path = socket_path
        self.staging_root = staging_root
        self.runner_path = runner_path
        self.manage_api = manage_api
        self.api_command = api_command or self._default_api_command()

        self.runs: dict[str, Run] = {}
        # pid -> run_id for every runner that has been spawned and not reaped.
        self.pid_to_run: dict[int, str] = {}

        self.api_pid: int | None = None
        self.api_state = API_DEAD
        self.api_epoch = 0
        self.next_api_start = 0.0

        self.shutting_down = False
        self.shutdown_deadline: float | None = None
        self._self_pipe_r, self._self_pipe_w = os.pipe()
        self._listen: socket.socket | None = None
        # control socket -> buffered bytes not yet parsed into frames
        self.connections: dict[socket.socket, bytearray] = {}

        self._can_drop_privs = os.geteuid() == 0
        if not self._can_drop_privs:
            _log(
                "not running as root; API and runners keep the current "
                "credentials (test environment only)"
            )

    # ------------------------------------------------------------------ setup
    @staticmethod
    def _default_api_command() -> list[str]:
        # -B: never write bytecode.  Isolation comes from the explicit minimal
        # environment below and the privilege drop, not from interpreter flags.
        return [
            sys.executable,
            "-B",
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ]

    def _prepare_dirs(self) -> None:
        os.makedirs(self.staging_root, exist_ok=True)
        os.makedirs(os.path.dirname(self.socket_path), exist_ok=True)
        # A stale socket from a previous instance would make bind() fail.
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        self._sweep_staging()
        if self._can_drop_privs:
            os.chown(os.path.dirname(self.socket_path), SANDBOX_UID, SANDBOX_GID)
            os.chown(self.staging_root, SANDBOX_UID, SANDBOX_GID)
            os.chmod(os.path.dirname(self.socket_path), 0o700)
            os.chmod(self.staging_root, 0o700)

    def _sweep_staging(self) -> None:
        """Remove staging directories left behind by an earlier instance."""
        try:
            entries = os.listdir(self.staging_root)
        except FileNotFoundError:
            return
        for name in entries:
            if not name.startswith(STAGING_PREFIX):
                continue
            path = os.path.join(self.staging_root, name)
            shutil.rmtree(path, ignore_errors=True)
            _log(f"swept stale staging directory {path}")

    def _install_signals(self) -> None:
        def sigchld_handler(_signum, _frame):
            # Nudge the self-pipe so select() wakes and reap logic runs.  All
            # child bookkeeping stays in the main loop / main thread.
            try:
                os.write(self._self_pipe_w, b"c")
            except OSError:
                pass

        # SIGTERM/SIGINT are installed by main() (or an embedding test) so the
        # shutdown policy is not hard-wired into the loop itself.
        signal.signal(signal.SIGCHLD, sigchld_handler)
        try:
            signal.siginterrupt(signal.SIGCHLD, False)
        except OSError:
            pass

    def _open_listen_socket(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.bind(self.socket_path)
        os.chmod(self.socket_path, 0o600)
        if self._can_drop_privs:
            os.chown(self.socket_path, SANDBOX_UID, SANDBOX_GID)
        sock.listen(16)
        self._listen = sock

    # --------------------------------------------------------------- API worker
    def _spawn_api(self) -> None:
        self.api_epoch += 1
        epoch = self.api_epoch
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
            # -I ignores cwd, so the app package is located explicitly.
            "PYTHONPATH": os.getcwd(),
            "WASM_SANDBOX_RUNTIME_ROOT": str(Path(self.socket_path).parent),
            "PYTHONUNBUFFERED": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        try:
            proc = subprocess.Popen(
                self.api_command,
                cwd=os.getcwd(),
                env=env,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
                preexec_fn=self._drop_privileges if self._can_drop_privs else None,
            )
        except OSError as exc:
            _log(f"failed to spawn API worker: {exc}")
            self.api_state = API_DEAD
            self.next_api_start = time.monotonic() + 1.0
            return
        self.api_pid = proc.pid
        self.api_state = API_RUNNING
        _log(f"API worker started (epoch {epoch}, pid {proc.pid})")

    def _drop_privileges(self) -> None:
        # Runs inside the child between fork and exec.
        os.setgroups([])
        os.setgid(SANDBOX_GID)
        os.setuid(SANDBOX_UID)

    def _reap_api_if_done(self) -> None:
        if self.api_pid is None:
            return
        try:
            waited_pid, _status = os.waitpid(self.api_pid, os.WNOHANG)
        except ChildProcessError:
            waited_pid = self.api_pid
        if waited_pid == 0:
            return
        dead_pid = self.api_pid
        self.api_pid = None
        if self.shutting_down:
            self.api_state = API_DEAD
            return
        self.api_state = API_DEAD
        self.next_api_start = time.monotonic() + API_RESTART_DELAY_SECONDS
        _log(f"API worker pid {dead_pid} exited; purging in-flight runs")
        self._handle_api_death()

    def _handle_api_death(self) -> None:
        """SIGKILL every in-flight runner; staging is removed on reap.

        Runs that are still between registration and fork (no pid yet) are
        finalised directly, so a fault during runner startup cannot leak.
        """
        for run in list(self.runs.values()):
            if run.finalized:
                continue
            self._kill_run(run, STATE_API_DEAD)

    # -------------------------------------------------------------------- runs
    def submit_run(self, module_bytes: bytes, log_bytes: bytes) -> Run:
        run_id = uuid.uuid4().hex
        staging_dir = os.path.join(
            self.staging_root, f"{STAGING_PREFIX}{run_id}"
        )
        os.makedirs(staging_dir, mode=0o700)
        if self._can_drop_privs:
            os.chown(staging_dir, SANDBOX_UID, SANDBOX_GID)
        module_path = os.path.join(staging_dir, "plugin.wasm")
        log_path = os.path.join(staging_dir, "input.json")
        with open(module_path, "wb") as handle:
            handle.write(module_bytes)
        with open(log_path, "wb") as handle:
            handle.write(log_bytes)
        os.chmod(module_path, 0o600)
        os.chmod(log_path, 0o600)

        now = time.monotonic()
        run = Run(
            run_id=run_id,
            staging_dir=staging_dir,
            module_path=module_path,
            log_path=log_path,
            created_at=now,
            deadline=now + PLUGIN_TIMEOUT_SECONDS,
        )
        # Register before fork: an API death landing while Popen is in flight
        # still finds the run and can finalise (or kill) it.
        self.runs[run_id] = run
        try:
            self._spawn_runner(run)
        except OSError as exc:
            run.error = f"runner could not be started: {exc.__class__.__name__}"
            self._finalize_run(run, STATE_SPAWN_FAILED)
        if run.finalized:
            # An API-death sweep or output-limit event finalised the run while
            # Popen was in flight; leave that outcome untouched.
            _log(f"newly spawned run {run_id[:8]} was already finalised")
        return run

    def _spawn_runner(self, run: Run) -> None:
        if run.finalized:
            # An API-death sweep finalised the run while Popen was in flight.
            # Never fork a runner for a run that must already be gone.
            _log(
                f"run {run.run_id[:8]} was finalised before fork; "
                "runner not started"
            )
            return
        proc = subprocess.Popen(
            [
                sys.executable,
                "-B",
                "-I",
                str(self.runner_path),
                run.module_path,
                run.log_path,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/",
            env={},
            close_fds=True,
            start_new_session=True,
            preexec_fn=self._drop_privileges if self._can_drop_privs else None,
        )
        run.process = proc
        run.pid = proc.pid
        run.state = STATE_RUNNING
        self.pid_to_run[proc.pid] = run.run_id

        stdout_fd = proc.stdout.fileno()
        stderr_fd = proc.stderr.fileno()
        run._pipe_fds = [stdout_fd, stderr_fd]
        t1 = threading.Thread(
            target=self._drain_output,
            args=(run, stdout_fd, run.stdout),
            name=f"drain-out-{run.run_id[:8]}",
            daemon=True,
        )
        t2 = threading.Thread(
            target=self._drain_output,
            args=(run, stderr_fd, run.stderr),
            name=f"drain-err-{run.run_id[:8]}",
            daemon=True,
        )
        run._drain_threads = [t1, t2]
        t1.start()
        t2.start()
        _log(f"runner pid {proc.pid} started for run {run.run_id[:8]}")

    def _drain_output(
        self, run: Run, fd: int, sink: bytearray
    ) -> None:
        while True:
            try:
                chunk = os.read(fd, READ_CHUNK)
            except OSError:
                return
            if not chunk:
                return
            overflowed = False
            with run.lock:
                if run.overflow or run.finalized:
                    # A limit was hit or the run ended; keep the pipe drained
                    # but retain none of the partial output.
                    continue
                run.total_output += len(chunk)
                if run.total_output > MAX_PLUGIN_OUTPUT_BYTES:
                    run.overflow = True
                    overflowed = True
                else:
                    sink.extend(chunk)
            if overflowed:
                _log(f"run {run.run_id[:8]} exceeded output cap; killing")
                self._kill_run(run, STATE_OUTPUT_TOO_LARGE)

    @staticmethod
    def _kill_pg(pid: int) -> bool:
        """SIGKILL the whole process group; fall back to the single pid.

        A restricted environment may deny group signals while allowing a
        direct one.  Both attempts ignore "already gone" so the call stays
        idempotent during reap races.
        """
        for target in (-pid, pid):
            try:
                os.kill(target, signal.SIGKILL)
                return True
            except ProcessLookupError:
                return True
            except PermissionError:
                continue
            except OSError as exc:
                if exc.errno == errno.ESRCH:
                    return True
                continue
        return False

    def _kill_run(self, run: Run, reason: str) -> None:
        """SIGKILL the runner process group. Idempotent and pidless-safe."""
        if run.finalized:
            return
        if run.pid is None:
            # Fault before/during fork: nothing to signal, finalise at once.
            self._finalize_run(run, reason)
            return
        killed = self._kill_pg(run.pid)
        if not killed:
            # Last resort: let the periodic reap observe the process.  The
            # run is still marked for the requested terminal state.
            _log(
                f"could not signal runner pid {run.pid} for run "
                f"{run.run_id[:8]}; relying on exit/reap"
            )
        # Remember the intended outcome; finalisation (reap + staging removal)
        # happens when the child is waited for.
        if reason == STATE_RUNNING:
            reason = STATE_CANCELLED
        run.state = reason

    def cancel_run(self, run_id: str) -> bool:
        run = self.runs.get(run_id)
        if run is None or run.finalized:
            return False
        self._kill_run(run, STATE_CANCELLED)
        return True

    def _enforce_timeouts(self, now: float) -> None:
        # Monotonic deadline: still fires while the API is SIGSTOPed.
        for run in list(self.runs.values()):
            if run.finalized or run.pid is None:
                continue
            if now >= run.deadline:
                _log(f"run {run.run_id[:8]} hit {PLUGIN_TIMEOUT_SECONDS:g}s deadline")
                self._kill_run(run, STATE_TIMEOUT)

    def _prune_finalised(self, now: float) -> None:
        """Drop old finalised runs; their files were already removed."""
        if len(self.runs) <= FINALISED_HARD_CAP:
            stale = [
                run_id
                for run_id, run in self.runs.items()
                if run.finalized
                and run.finished_at is not None
                and now - run.finished_at > FINALISED_RETENTION_SECONDS
            ]
        else:
            # Under a burst, evict the oldest finalised runs first.
            ordered = sorted(
                (r for r in self.runs.values() if r.finalized),
                key=lambda r: r.finished_at or 0.0,
            )
            excess = len(self.runs) - FINALISED_HARD_CAP
            stale = [r.run_id for r in ordered[:excess]]
        for run_id in stale:
            self.runs.pop(run_id, None)

    def _reap_runners(self) -> None:
        for pid in list(self.pid_to_run.keys()):
            try:
                waited_pid, status = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                # Already reaped (SIGCHLD raced the periodic sweep).
                self.pid_to_run.pop(pid, None)
                continue
            if waited_pid == 0:
                continue
            self.pid_to_run.pop(pid, None)
            run = next(
                (r for r in self.runs.values() if r.pid == pid), None
            )
            if run is None:
                continue
            if os.WIFSIGNALED(status):
                returncode = KILLED_BY_SUPERVISOR
            elif os.WIFEXITED(status):
                returncode = os.WEXITSTATUS(status)
            else:
                returncode = KILLED_BY_SUPERVISOR
            self._finalize_run(run, run.state, returncode)

    def _finalize_run(
        self,
        run: Run,
        state: str,
        returncode: int | None = None,
    ) -> None:
        if run.finalized:
            return
        if state in (STATE_RUNNING, STATE_PENDING):
            state = STATE_EXITED
        run.finalized = True
        run.state = state
        if returncode is not None:
            run.returncode = returncode
        run.finished_at = time.monotonic()

        # Close the pipe fds so the drain threads receive EOF.
        for fd in run._pipe_fds:
            try:
                os.close(fd)
            except OSError:
                pass
        for thread in run._drain_threads:
            thread.join(DRAIN_JOIN_SECONDS)

        # Staging removal is synchronous and unconditional: a killed run never
        # leaves its uploaded module or log behind.
        shutil.rmtree(run.staging_dir, ignore_errors=True)
        _log(
            f"run {run.run_id[:8]} finalised state={state} "
            f"rc={run.returncode} staging_removed={not os.path.exists(run.staging_dir)}"
        )

    # ------------------------------------------------------------- control API
    def _accept_connection(self) -> None:
        assert self._listen is not None
        try:
            conn, _ = self._listen.accept()
        except (BlockingIOError, InterruptedError):
            return
        conn.setblocking(False)
        self.connections[conn] = bytearray()

    def _read_connection(self, conn: socket.socket) -> None:
        buffer = self.connections[conn]
        try:
            chunk = conn.recv(65536)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            self._close_connection(conn)
            return
        if not chunk:
            self._close_connection(conn)
            return
        if len(buffer) + len(chunk) > MAX_CONTROL_FRAME_BYTES:
            self._send(conn, {"type": "error", "error": "frame_too_large"})
            self._close_connection(conn)
            return
        buffer.extend(chunk)
        while b"\n" in buffer:
            line, _, rest = buffer.partition(b"\n")
            del buffer[: len(line) + 1]
            self._dispatch_frame(conn, line)

    def _dispatch_frame(self, conn: socket.socket, raw_frame: bytes) -> None:
        try:
            frame = json.loads(raw_frame.decode("utf-8"))
            if not isinstance(frame, dict) or "type" not in frame:
                raise ValueError("frame must be a JSON object with a type")
        except (UnicodeDecodeError, ValueError, RecursionError):
            self._send(conn, {"type": "error", "error": "bad_frame"})
            return

        kind = frame["type"]
        try:
            if kind == "submit":
                self._handle_submit(conn, frame)
            elif kind == "poll":
                self._handle_poll(conn, frame)
            elif kind == "cancel":
                self._handle_cancel(conn, frame)
            elif kind == "ping":
                self._send(conn, {"type": "pong"})
            else:
                self._send(conn, {"type": "error", "error": "unknown_type"})
        except Exception as exc:  # noqa: BLE001 - report, never crash the loop
            _log(f"control frame {kind} failed: {exc.__class__.__name__}: {exc}")
            self._send(conn, {"type": "error", "error": "internal_error"})

    def _handle_submit(self, conn: socket.socket, frame: dict) -> None:
        try:
            module_bytes = base64.b64decode(frame["module_b64"], validate=True)
            log_bytes = base64.b64decode(frame["log_b64"], validate=True)
        except (KeyError, ValueError):
            self._send(conn, {"type": "error", "error": "bad_submit"})
            return
        run = self.submit_run(module_bytes, log_bytes)
        self._send(conn, {"type": "submitted", "id": run.run_id})

    def _handle_poll(self, conn: socket.socket, frame: dict) -> None:
        run = self.runs.get(frame.get("id", ""))
        if run is None:
            self._send(conn, {"type": "result", "id": frame.get("id", ""),
                              "state": "unknown"})
            return
        payload = {
            "type": "result",
            "id": run.run_id,
            "state": run.state,
            "done": run.finalized,
        }
        if run.finalized:
            payload["returncode"] = run.returncode
            payload["stdout_b64"] = base64.b64encode(bytes(run.stdout)).decode(
                "ascii"
            )
            payload["stderr_b64"] = base64.b64encode(bytes(run.stderr)).decode(
                "ascii"
            )
            if run.error:
                payload["error"] = run.error
        self._send(conn, payload)

    def _handle_cancel(self, conn: socket.socket, frame: dict) -> None:
        ok = self.cancel_run(frame.get("id", ""))
        self._send(conn, {"type": "cancelled", "id": frame.get("id", ""),
                          "ok": ok})

    @staticmethod
    def _send(conn: socket.socket, payload: dict) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"
        try:
            conn.sendall(data)
        except OSError:
            pass

    def _close_connection(self, conn: socket.socket) -> None:
        self.connections.pop(conn, None)
        try:
            conn.close()
        except OSError:
            pass
        # Note: closing the API's control connection does not kill its runs.
        # API death is detected through waitpid(), which cannot false-positive
        # on a transient socket problem.

    # ---------------------------------------------------------------- main loop
    def run(self) -> int:
        self._prepare_dirs()
        self._install_signals()
        self._open_listen_socket()
        if self.manage_api:
            self._spawn_api()
        _log(
            f"supervising control={self.socket_path} staging={self.staging_root} "
            f"manage_api={self.manage_api}"
        )

        while True:
            try:
                self._reap_api_if_done()
                self._reap_runners()
                now = time.monotonic()
                self._enforce_timeouts(now)
                self._prune_finalised(now)

                if (
                    self.manage_api
                    and self.api_state == API_DEAD
                    and not self.shutting_down
                    and time.monotonic() >= self.next_api_start
                ):
                    self._spawn_api()

                if self.shutting_down:
                    # Escalate the API stop from SIGTERM to SIGKILL once the
                    # grace window has elapsed.
                    if (
                        self.api_pid is not None
                        and self.shutdown_deadline is not None
                        and time.monotonic() >= self.shutdown_deadline
                    ):
                        self._kill_pg(self.api_pid)
                    if self._all_quiescent():
                        return self._shutdown()

                readable, _, _ = select.select(
                    [self._self_pipe_r, self._listen, *self.connections.keys()],
                    [],
                    [],
                    SUPERVISOR_TICK_SECONDS,
                )
                for ready in readable:
                    if ready is self._self_pipe_r:
                        try:
                            os.read(self._self_pipe_r, 256)
                        except OSError:
                            pass
                        if self.shutting_down:
                            pass  # handled at the top of the next iteration
                    elif ready is self._listen:
                        self._accept_connection()
                    else:
                        self._read_connection(ready)
            except KeyboardInterrupt:
                self._begin_shutdown()

    def _all_quiescent(self) -> bool:
        if self.manage_api and self.api_pid is not None:
            return False
        return all(r.finalized for r in self.runs.values())

    def _begin_shutdown(self) -> None:
        if self.shutting_down:
            return
        self.shutting_down = True
        self.shutdown_deadline = time.monotonic() + SHUTDOWN_GRACE_SECONDS
        _log("shutdown requested; stopping API and all runners")
        if self.api_pid is not None:
            try:
                os.killpg(self.api_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError:
                try:
                    os.kill(self.api_pid, signal.SIGTERM)
                except OSError:
                    pass
            except OSError:
                pass
        for run in list(self.runs.values()):
            if not run.finalized:
                self._kill_run(run, STATE_SHUTDOWN)

    def _shutdown(self) -> int:
        # Final grace: SIGKILL the API if SIGTERM did not take.
        if self.api_pid is not None:
            self._kill_pg(self.api_pid)
            try:
                os.waitpid(self.api_pid, 0)
            except ChildProcessError:
                pass
        for conn in list(self.connections):
            self._close_connection(conn)
        if self._listen is not None:
            self._listen.close()
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        for fd in (self._self_pipe_r, self._self_pipe_w):
            try:
                os.close(fd)
            except OSError:
                pass
        _log("shutdown complete")
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="WASM sandbox supervisor")
    parser.add_argument("--no-api", action="store_true",
                        help="only serve the control socket; do not start the API worker (tests)")
    parser.add_argument(
        "--api-cmd",
        default=None,
        help="override the API worker command, formatted as an argv list in "
        "one JSON string (tests substitute a lightweight stand-in)",
    )
    parser.add_argument("--socket", default=CONTROL_SOCKET_PATH)
    parser.add_argument("--staging", default=STAGING_ROOT)
    parser.add_argument("--runner", type=Path, default=RUNNER_PATH)
    args = parser.parse_args(argv)

    api_command = None
    if args.api_cmd is not None:
        parsed = json.loads(args.api_cmd)
        if not isinstance(parsed, list) or not all(
            isinstance(part, str) for part in parsed
        ):
            parser.error("--api-cmd must be a JSON array of strings")
        api_command = parsed

    supervisor = Supervisor(
        socket_path=args.socket,
        staging_root=args.staging,
        runner_path=args.runner,
        manage_api=not args.no_api,
        api_command=api_command,
    )

    def shutdown_on_term(_signum, _frame):
        # Safe to call directly: it only flips a flag and signals children,
        # then lets the main loop perform all reaping and cleanup.
        supervisor._begin_shutdown()  # noqa: SLF001

    signal.signal(signal.SIGTERM, shutdown_on_term)
    signal.signal(signal.SIGINT, shutdown_on_term)
    return supervisor.run()


if __name__ == "__main__":
    raise SystemExit(main())
