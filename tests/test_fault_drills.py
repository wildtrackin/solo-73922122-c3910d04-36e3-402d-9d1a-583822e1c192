"""End-to-end fault drills against the real process topology.

A full supervisor (the container's PID-1 role) manages a single uvicorn API
worker.  These tests drive the two operational scenarios from the
requirements:

* SIGSTOP the API while a dead-loop plugin is running: the supervisor still
  enforces the 2 second wall clock; when the API is resumed the call returns a
  504 timeout, never partial output;
* SIGKILL the API while plugins are in flight (including one still starting):
  every runner is terminated and reaped and every staging directory removed
  within one second, without waiting for the API; the supervisor then
  restarts the API and new calls succeed.

They need kill(2), so they skip on hosts whose seccomp profile denies signals
(e.g. some CI sandboxes).  Inside the delivered container they run in full.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO = REPO_ROOT / "demo"
RUNNER_PATH = REPO_ROOT / "app" / "runner.py"


def _signals_available() -> bool:
    probe = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(5)"]
    )
    try:
        os.kill(probe.pid, signal.SIGKILL)
        probe.wait(timeout=5)
        return True
    except OSError:
        return False
    finally:
        try:
            probe.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            pass


pytestmark = pytest.mark.skipif(
    not _signals_available(),
    reason="host seccomp profile denies kill(2); run inside the container",
)


def _wait_health(url: str, timeout: float = 20.0) -> None:
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except OSError as exc:
            last = exc
        time.sleep(0.1)
    raise AssertionError(f"API never became healthy: {last}")


def _wait_gone(pid: int, timeout: float) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return True
        time.sleep(0.01)
    return False


def _child_pids(ppid: int) -> list[int]:
    out = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if int((entry / "stat").read_text().split()[3]) == ppid:
                out.append(int(entry.name))
        except OSError:
            continue
    return out


def _runner_pids() -> list[int]:
    """Pids of real runner.py processes, read from /proc cmdlines."""
    pids = []
    target = str(RUNNER_PATH)
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if target.encode() in raw.split(b"\x00"):
            pids.append(int(entry.name))
    return pids


@pytest.fixture
def full_stack(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    socket_path = runtime / "control.sock"
    staging = runtime / "runs"
    port = int(os.environ.get("WASM_SANDBOX_TEST_PORT", "8099"))
    api_cmd = [
        sys.executable,
        "-B",
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]
    env = dict(os.environ)
    env["WASM_SANDBOX_RUNTIME_ROOT"] = str(runtime)
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["WASM_SANDBOX_UID"] = str(os.geteuid())
    env["WASM_SANDBOX_GID"] = str(os.getegid())

    proc = subprocess.Popen(
        [
            sys.executable,
            "-B",
            "-m",
            "app.supervisor",
            "--socket",
            str(socket_path),
            "--staging",
            str(staging),
            "--runner",
            str(RUNNER_PATH),
            "--api-cmd",
            json.dumps(api_cmd),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_health(base_url + "/healthz")
        yield {
            "supervisor": proc,
            "base_url": base_url,
            "staging": staging,
            "runtime": runtime,
        }
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            try:
                proc.kill()
                proc.wait(timeout=5)
            except OSError:
                pass


def _post_classification(base_url: str) -> httpx.Response:
    with (DEMO / "classify.wasm").open("rb") as mf, (
        DEMO / "sample-log.json"
    ).open("rb") as lf:
        return httpx.post(
            base_url + "/api/v1/analyze",
            files={
                "module": ("plugin.wasm", mf, "application/wasm"),
                "log": ("input.json", lf, "application/json"),
            },
            timeout=30.0,
        )


def _post_loop(base_url: str) -> httpx.Response:
    with (DEMO / "loop.wasm").open("rb") as mf, (
        DEMO / "sample-log.json"
    ).open("rb") as lf:
        return httpx.post(
            base_url + "/api/v1/analyze",
            files={
                "module": ("plugin.wasm", mf, "application/wasm"),
                "log": ("input.json", lf, "application/json"),
            },
            timeout=30.0,
        )


def test_healthy_classification_before_faults(full_stack):
    resp = _post_classification(full_stack["base_url"])
    assert resp.status_code == 200
    assert resp.json()["verdict"] == "allow"
    assert list(full_stack["staging"].glob("run-*")) == []


def test_sigstop_api_still_enforces_timeout_and_returns_504(full_stack):
    sup = full_stack["supervisor"]
    base_url = full_stack["base_url"]
    staging: Path = full_stack["staging"]
    api_pid = _child_pids(sup.pid)[0]

    result: dict = {}

    def do_call():
        try:
            result["resp"] = _post_loop(base_url)
        except Exception as exc:  # noqa: BLE001
            result["err"] = exc

    thread = threading.Thread(target=do_call)
    started = time.monotonic()
    thread.start()
    time.sleep(0.5)  # let the request reach the supervisor and start its runner

    os.kill(api_pid, signal.SIGSTOP)
    try:
        # Frozen well beyond the 2s plugin budget; the supervisor keeps going.
        time.sleep(3.0)
        deadline = started + 2.8
        while time.monotonic() < deadline:
            if not list(staging.glob("run-*")):
                break
            time.sleep(0.02)
        assert list(staging.glob("run-*")) == []
    finally:
        os.kill(api_pid, signal.SIGCONT)

    thread.join(timeout=10)
    elapsed = time.monotonic() - started

    assert "err" not in result, result.get("err")
    resp = result["resp"]
    assert resp.status_code == 504
    assert resp.json()["error"]["code"] == "plugin_timeout"
    # No partial plugin output is ever returned.
    assert "verdict" not in resp.text
    assert elapsed >= 2.0

    # Same API process; traffic continues normally after resume.
    assert _child_pids(sup.pid) == [api_pid]
    follow = _post_classification(base_url)
    assert follow.status_code == 200
    assert follow.json()["verdict"] == "allow"


def test_sigkill_api_purges_runners_within_one_second_and_restarts(full_stack):
    sup = full_stack["supervisor"]
    base_url = full_stack["base_url"]
    staging: Path = full_stack["staging"]
    api_pid = _child_pids(sup.pid)[0]

    errors: list = []

    def launch_loop():
        try:
            _post_loop(base_url)
        except Exception as exc:  # noqa: BLE001 - connection reset is expected
            errors.append(exc)

    threads = [threading.Thread(target=launch_loop) for _ in range(3)]
    for t in threads:
        t.start()

    # Wait until three real runner processes exist.
    runner_deadline = time.monotonic() + 5.0
    runner_pids: list[int] = []
    while time.monotonic() < runner_deadline:
        runner_pids = _runner_pids()
        if len(runner_pids) >= 3:
            break
        time.sleep(0.05)
    assert len(runner_pids) >= 3
    runner_pids = runner_pids[:3]

    kill_at = time.monotonic()
    os.kill(api_pid, signal.SIGKILL)

    reap_budget = kill_at + 1.0
    for pid in runner_pids:
        assert _wait_gone(pid, 1.0), f"runner {pid} survived API SIGKILL"
    assert time.monotonic() < reap_budget + 0.05

    # Staging cleared without waiting for any API restart.
    for _ in range(50):
        if not list(staging.glob("run-*")):
            break
        time.sleep(0.02)
    assert list(staging.glob("run-*")) == []

    for t in threads:
        t.join(timeout=10)

    # The supervisor restarts the API automatically; a fresh, isolated call
    # then succeeds.
    _wait_health(base_url + "/healthz")
    new_api_pids = [p for p in _child_pids(sup.pid) if p != api_pid]
    assert len(new_api_pids) == 1
    assert new_api_pids[0] != api_pid

    resp = _post_classification(base_url)
    assert resp.status_code == 200
    assert resp.json()["verdict"] == "allow"
    assert list(staging.glob("run-*")) == []
