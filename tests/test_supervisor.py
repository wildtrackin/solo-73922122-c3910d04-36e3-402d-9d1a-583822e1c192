"""Lifecycle tests for the out-of-process supervisor.

These tests do not need wasmtime: a tiny stdlib script stands in for
``app/runner.py``.  They exercise the guarantees the API-level tests cannot:

* the 2 second wall clock fires while *nothing* polls the supervisor
  (equivalent to the API being SIGSTOPed);
* an API-child death purges every in-flight runner and its staging directory
  inside one second, including a run whose runner is still starting up;
* post-restart submissions are isolated from the previous epoch's files;
* concurrent runs are cleaned up independently.
"""

from __future__ import annotations

import base64
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _signals_available() -> bool:
    """Whether this host lets a process signal its own children.

    Some CI sandboxes install a seccomp filter that returns EPERM for every
    kill(2); the real container image (default docker profile, root PID 1)
    permits them.  Signal-driven tests skip instead of failing there.
    """
    probe = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(5)"]
    )
    try:
        os.kill(probe.pid, signal.SIGKILL)
        probe.wait(timeout=5)
        return True
    except (OSError, subprocess.TimeoutExpired):
        probe.kill()
        return False
    finally:
        try:
            probe.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            pass


SIGNALS_AVAILABLE = _signals_available()
skip_without_signals = pytest.mark.skipif(
    not SIGNALS_AVAILABLE,
    reason="host seccomp profile denies kill(2); run inside the container",
)


# --------------------------------------------------------------------- helpers
def _write_fake_runner(directory: Path, behavior: str) -> Path:
    """Create a runner script that mimics app/runner.py's argv contract."""
    script = directory / f"runner_{behavior}.py"
    script.write_text(
        textwrap.dedent(
            f"""
            import json, os, sys, time

            behavior = {behavior!r}
            module_path, log_path = sys.argv[1], sys.argv[2]

            # Marker lets tests observe that staging existed during the run.
            with open(os.path.join(os.path.dirname(module_path), "marker"), "w") as f:
                f.write(str(os.getpid()))
            if behavior == "ok":
                sys.stdout.write(json.dumps({{"ok": True}}))
                sys.stdout.flush()
                sys.exit(0)
            if behavior == "slow":
                while True:
                    time.sleep(0.05)
            if behavior == "self_timeout":
                # Exits on its own just past the 2s deadline; lets hosts that
                # deny kill(2) still verify deadline bookkeeping and cleanup.
                time.sleep(2.2)
                sys.exit(0)
            if behavior == "spawn_delay":
                time.sleep(2.0)
                sys.exit(0)
            if behavior == "noisy":
                while True:
                    sys.stdout.write("x" * 4096)
                    sys.stdout.flush()
            if behavior == "exit22":
                sys.exit(22)
            sys.exit(26)
            """
        )
    )
    os.chmod(script, 0o755)
    return script


def _write_fake_api(directory: Path) -> Path:
    script = directory / "fake_api.py"
    script.write_text("import time\nwhile True:\n    time.sleep(0.1)\n")
    return script


class ControlClient:
    def __init__(self, socket_path: str) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(15)
        self.sock.connect(socket_path)
        self.buffer = b""

    def _frame(self, payload: dict) -> dict:
        self.sock.sendall(
            (json.dumps(payload, separators=(",", ":")) + "\n").encode()
        )
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(65536)
            assert chunk, "supervisor closed the control connection"
            self.buffer += chunk
        line, _, self.buffer = self.buffer.partition(b"\n")
        return json.loads(line.decode())

    def submit(self, module: bytes = b"m", log: bytes = b"{}") -> str:
        reply = self._frame(
            {
                "type": "submit",
                "module_b64": base64.b64encode(module).decode(),
                "log_b64": base64.b64encode(log).decode(),
            }
        )
        assert reply["type"] == "submitted", reply
        return reply["id"]

    def poll(self, run_id: str) -> dict:
        return self._frame({"type": "poll", "id": run_id})

    def wait_terminal(self, run_id: str, timeout: float = 15.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self.poll(run_id)
            if frame.get("done"):
                return frame
            time.sleep(0.02)
        raise AssertionError(f"run {run_id} never finished")

    def close(self) -> None:
        self.sock.close()


def _start_supervisor(
    tmp_path: Path,
    runner_behavior: str,
    *,
    manage_api: bool,
    name: str,
    api_script: Path | None = None,
):
    runtime = tmp_path / f"runtime-{name}"
    runtime.mkdir(parents=True, exist_ok=True)
    socket_path = str(runtime / "control.sock")
    staging = runtime / "runs"
    runner = _write_fake_runner(tmp_path, runner_behavior)

    env = dict(os.environ)
    env["WASM_SANDBOX_RUNTIME_ROOT"] = str(runtime)
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["WASM_SANDBOX_UID"] = str(os.geteuid())
    env["WASM_SANDBOX_GID"] = str(os.getegid())

    cmd = [
        sys.executable,
        "-B",
        "-m",
        "app.supervisor",
        "--socket",
        socket_path,
        "--staging",
        str(staging),
        "--runner",
        str(runner),
    ]
    if manage_api:
        cmd += [
            "--api-cmd",
            json.dumps([sys.executable, "-B", str(api_script)]),
        ]
    else:
        cmd.append("--no-api")

    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not os.path.exists(socket_path):
        if proc.poll() is not None:
            raise AssertionError(
                f"supervisor exited early: {proc.stdout.read()}"
            )
        time.sleep(0.02)
    return proc, ControlClient(socket_path), staging, socket_path


def _stop(proc: subprocess.Popen) -> None:
    """Terminate a supervisor, tolerating hosts that deny kill(2)."""
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        try:
            proc.kill()
            proc.wait(timeout=5)
        except OSError:
            # Seccomp denied every signal; the process is reaped by the host
            # when the test session ends.
            pass


@pytest.fixture
def no_api_harness(tmp_path):
    proc, client, staging, socket_path = _start_supervisor(
        tmp_path, "ok", manage_api=False, name="basic"
    )
    try:
        yield client, staging
    finally:
        client.close()
        _stop(proc)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _child_pids(parent_pid: int) -> list[int]:
    pids = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
        except OSError:
            continue
        ppid = int(stat.split()[3])
        if ppid == parent_pid:
            pids.append(int(entry.name))
    return pids


# ----------------------------------------------------------------------- tests
def test_ok_run_returns_stdout_and_removes_staging(no_api_harness):
    client, staging = no_api_harness
    run_id = client.submit()
    result = client.wait_terminal(run_id)
    assert result["state"] == "exited"
    assert result["returncode"] == 0
    assert base64.b64decode(result["stdout_b64"]) == b'{"ok": true}'
    assert list(staging.glob("run-*")) == []


def test_runner_exit_code_is_forwarded(tmp_path):
    proc, client, staging, _ = _start_supervisor(
        tmp_path, "exit22", manage_api=False, name="exit22"
    )
    try:
        result = client.wait_terminal(client.submit())
        assert result["state"] == "exited"
        assert result["returncode"] == 22
        assert list(staging.glob("run-*")) == []
    finally:
        client.close()
        _stop(proc)


@skip_without_signals
def test_timeout_without_any_poller_mimics_sigstop(tmp_path):
    """Nobody polls: the supervisor must still kill at the 2s deadline."""
    proc, client, staging, _ = _start_supervisor(
        tmp_path, "slow", manage_api=False, name="sigstop"
    )
    try:
        start = time.monotonic()
        run_id = client.submit()
        # Do NOT poll -- simulate an API frozen with SIGSTOP.
        time.sleep(2.6)
        result = client.poll(run_id)
        elapsed = time.monotonic() - start
        assert result["done"] is True
        assert result["state"] == "timeout"
        assert result["returncode"] == 137
        # 2s deadline measured from runner start, with a scheduling margin.
        assert 2.0 <= elapsed < 2.4
        assert list(staging.glob("run-*")) == []
    finally:
        client.close()
        _stop(proc)


def test_timeout_bookkeeping_without_host_signals(tmp_path):
    """Signal-restricted host variant: a self-exiting runner past the deadline.

    The supervisor still flags the run as timed out (the monotonic deadline
    fires) and removes the staging directory once the runner exits and is
    reaped.
    """
    if SIGNALS_AVAILABLE:
        pytest.skip("covered by test_timeout_without_any_poller_mimics_sigstop")
    proc, client, staging, _ = _start_supervisor(
        tmp_path, "self_timeout", manage_api=False, name="sigstop-soft"
    )
    try:
        start = time.monotonic()
        run_id = client.submit()
        result = client.wait_terminal(run_id, timeout=8.0)
        elapsed = time.monotonic() - start
        assert result["state"] == "timeout"
        assert elapsed >= 2.0
        assert list(staging.glob("run-*")) == []
    finally:
        client.close()
        _stop(proc)


@skip_without_signals
def test_api_death_kills_inflight_runners_within_one_second(tmp_path):
    """Real API child plus slow runners; SIGKILL the API, verify the purge."""
    api_script = _write_fake_api(tmp_path)
    proc, client, staging, _ = _start_supervisor(
        tmp_path, "slow", manage_api=True, name="apideath", api_script=api_script
    )
    try:
        # Wait for the API child to appear, then find its pid.
        deadline = time.monotonic() + 5.0
        api_pids: list[int] = []
        while time.monotonic() < deadline:
            api_pids = [pid for pid in _child_pids(proc.pid) if pid != proc.pid]
            if api_pids:
                break
            time.sleep(0.02)
        assert len(api_pids) == 1, api_pids
        api_pid = api_pids[0]

        run_ids = [client.submit() for _ in range(3)]
        # Give the slow runners time to create their staging markers.
        marker_deadline = time.monotonic() + 3.0
        while time.monotonic() < marker_deadline:
            markers = list(staging.glob("run-*/marker"))
            if len(markers) == 3:
                break
            time.sleep(0.02)
        runner_pids = [int(marker.read_text()) for marker in markers]
        assert len(runner_pids) == 3
        for pid in runner_pids:
            assert _pid_alive(pid)

        kill_at = time.monotonic()
        os.kill(api_pid, signal.SIGKILL)

        reap_deadline = kill_at + 1.0
        for pid in runner_pids:
            while time.monotonic() < reap_deadline and _pid_alive(pid):
                time.sleep(0.01)
            assert not _pid_alive(pid), f"runner {pid} survived API SIGKILL"
        assert time.monotonic() < reap_deadline + 0.05

        for _ in range(50):
            if not list(staging.glob("run-*")):
                break
            time.sleep(0.02)
        assert list(staging.glob("run-*")) == []

        for run_id in run_ids:
            result = client.poll(run_id)
            assert result["state"] == "api_dead"
            assert result["done"] is True

        # The supervisor restarts the API child automatically.
        restart_deadline = time.monotonic() + 5.0
        new_api = None
        while time.monotonic() < restart_deadline:
            candidates = [
                pid
                for pid in _child_pids(proc.pid)
                if pid not in (proc.pid, api_pid)
            ]
            if candidates:
                new_api = candidates[0]
                break
            time.sleep(0.05)
        assert new_api is not None, "supervisor did not restart the API"

        # A run submitted after the restart is isolated from the old epoch.
        new_run = client.submit()
        result = client.wait_terminal(new_run)
        assert result["state"] == "timeout"  # slow runner -> 2s timeout
        assert list(staging.glob("run-*")) == []
    finally:
        client.close()
        _stop(proc)
        log = proc.stdout.read() if proc.stdout else ""
        if "Traceback" in log:
            pytest.fail(f"supervisor logged a traceback:\n{log}")


@skip_without_signals
def test_fault_during_runner_startup_is_not_lost(tmp_path):
    """API SIGKILL while the runner is still starting up must still purge."""
    api_script = _write_fake_api(tmp_path)
    proc, client, staging, _ = _start_supervisor(
        tmp_path,
        "spawn_delay",
        manage_api=True,
        name="startup",
        api_script=api_script,
    )
    try:
        deadline = time.monotonic() + 5.0
        api_pids: list[int] = []
        while time.monotonic() < deadline:
            api_pids = _child_pids(proc.pid)
            if api_pids:
                break
            time.sleep(0.02)
        assert len(api_pids) == 1

        run_id = client.submit()
        os.kill(api_pids[0], signal.SIGKILL)
        result = client.wait_terminal(run_id, timeout=5.0)
        assert result["state"] == "api_dead"
        assert list(staging.glob("run-*")) == []
    finally:
        client.close()
        _stop(proc)


@skip_without_signals
def test_output_overflow_kills_and_reports(tmp_path):
    proc, client, staging, _ = _start_supervisor(
        tmp_path, "noisy", manage_api=False, name="overflow"
    )
    try:
        result = client.wait_terminal(client.submit())
        assert result["state"] == "output_too_large"
        assert list(staging.glob("run-*")) == []
    finally:
        client.close()
        _stop(proc)


def test_concurrent_runs_are_independent(no_api_harness):
    client, staging = no_api_harness
    ids = [client.submit() for _ in range(4)]
    results = [client.wait_terminal(run_id) for run_id in ids]
    assert all(
        r["state"] == "exited" and r["returncode"] == 0 for r in results
    )
    assert list(staging.glob("run-*")) == []


def test_unknown_run_id_is_reported(no_api_harness):
    client, _staging = no_api_harness
    assert client.poll("does-not-exist")["state"] == "unknown"


def test_stale_staging_is_swept_on_startup(tmp_path):
    runtime = tmp_path / "runtime-sweep"
    staging = runtime / "runs"
    staging.mkdir(parents=True)
    stale = staging / "run-stale1234"
    stale.mkdir()
    (stale / "plugin.wasm").write_text("secret")
    unrelated = staging / "keep-me"
    unrelated.mkdir()

    proc, client, _staging, _ = _start_supervisor(
        tmp_path, "ok", manage_api=False, name="sweep"
    )
    try:
        assert not stale.exists()
        assert unrelated.exists()
    finally:
        client.close()
        _stop(proc)


def test_ping_frame(no_api_harness):
    client, _ = no_api_harness
    client.sock.sendall(b'{"type":"ping"}\n')
    reply = json.loads(client.sock.recv(128).decode())
    assert reply == {"type": "pong"}
