"""Shared pytest fixtures.

The API talks to a separate supervisor process over a Unix socket.  Tests run
a real supervisor in ``--no-api`` mode (no uvicorn child), pointed at the real
``app/runner.py``, with all of its state under an isolated temporary root so
test runs never touch the container's default runtime paths.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from app.config import CONTROL_SOCKET_PATH, STAGING_ROOT
from app.sandbox import _client, SupervisorUnavailable

REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER = REPO_ROOT / "app" / "runner.py"


def _wait_for_socket(path: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.settimeout(1.0)
                sock.connect(path)
                sock.sendall(b'{"type":"ping"}\n')
                if b"pong" in sock.recv(128):
                    return
            except OSError:
                pass
            finally:
                sock.close()
        time.sleep(0.05)
    raise RuntimeError(f"supervisor control socket {path} never became ready")


@pytest.fixture(scope="session", autouse=True)
def supervisor_process():
    runtime_root = tempfile.mkdtemp(prefix="wasm-test-runtime-")
    socket_path = os.path.join(runtime_root, "control.sock")
    staging = os.path.join(runtime_root, "runs")

    env = dict(os.environ)
    env["WASM_SANDBOX_RUNTIME_ROOT"] = runtime_root
    env["PYTHONPATH"] = str(REPO_ROOT)
    # No privilege drop in the test environment: the container's sandbox
    # account generally does not exist on a developer machine.
    env["WASM_SANDBOX_UID"] = str(os.geteuid())
    env["WASM_SANDBOX_GID"] = str(os.getegid())

    proc = subprocess.Popen(
        [
            sys.executable,
            "-B",
            "-m",
            "app.supervisor",
            "--no-api",
            "--socket",
            socket_path,
            "--staging",
            staging,
            "--runner",
            str(RUNNER),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_socket(socket_path)
        # Point the API's client at the test supervisor for the whole session.
        _client.socket_path = socket_path
        yield {
            "proc": proc,
            "socket": socket_path,
            "staging": staging,
            "runtime_root": runtime_root,
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        if proc.stdout is not None:
            output = proc.stdout.read()
            if output:
                print("\n--- supervisor log ---\n" + output)


@pytest.fixture(autouse=True)
def _reset_supervisor_client():
    # Each test starts with a fresh control connection so a test that breaks
    # the socket cannot poison the next one.
    _client._close()  # noqa: SLF001
    yield
    _client._close()  # noqa: SLF001


# Re-export so individual tests can assert on the documented default paths.
__all__ = ["supervisor_process", "CONTROL_SOCKET_PATH", "STAGING_ROOT",
           "SupervisorUnavailable"]
