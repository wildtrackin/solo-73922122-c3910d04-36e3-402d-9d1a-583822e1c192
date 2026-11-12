"""In-process unit tests for supervisor kill/reap logic.

The host running these tests may install a seccomp filter that denies
kill(2).  The signal *mechanism* is therefore replaced with a deterministic
fake: ``Supervisor._kill_pg`` is monkeypatched to mark the target pid, and a
fake waitpid reports those pids as SIGKILLed on the next reap sweep.  This
exercises exactly the production code paths for timeout, API death and the
runner-startup race, including the one-second cleanup budget and staging
removal, without sending a single signal.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from app import supervisor as supmod
from app.supervisor import (
    STATE_API_DEAD,
    STATE_OUTPUT_TOO_LARGE,
    STATE_TIMEOUT,
    Supervisor,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


class _FakeWaiter:
    """Replaces os.waitpid: faked kills are reported as signal deaths."""

    def __init__(self) -> None:
        self.killed: set[int] = set()
        self.exited: dict[int, int] = {}
        self.real_waitpid = os.waitpid

    def fake_kill_pg(self, pid: int) -> bool:
        # Simulate the kernel accepting SIGKILL on the group leader.
        self.killed.add(pid)
        return True

    def waitpid(self, pid: int, options: int) -> tuple[int, int]:
        if pid in self.killed:
            self.killed.discard(pid)
            # Signal-death status: WIFSIGNALED is true when (status & 0x7f) != 0
            # and (status & 0xff) != 0x7f; WTERMSIG reads status & 0x7f = 9.
            return pid, 9
        if pid in self.exited:
            # A known-but-not-yet-reaped exit is held back until the test
            # releases it, so a deadline can fire first even though the real
            # child process may already have gone away.
            if options == os.WNOHANG:
                return 0, 0
            code = self.exited.pop(pid)
            return pid, code << 8
        if options == os.WNOHANG:
            return 0, 0
        # Unknown pid: fall back to the real call.
        return self.real_waitpid(pid, options)


@pytest.fixture
def inproc_supervisor(tmp_path, monkeypatch):
    runner = tmp_path / "fake_runner.py"
    runner.write_text("import sys\nsys.exit(0)\n")
    s = Supervisor(
        socket_path=str(tmp_path / "control.sock"),
        staging_root=str(tmp_path / "runs"),
        runner_path=runner,
        manage_api=False,
    )
    waiter = _FakeWaiter()
    monkeypatch.setattr(supmod.os, "waitpid", waiter.waitpid)
    monkeypatch.setattr(s, "_kill_pg", waiter.fake_kill_pg)
    s._prepare_dirs()
    yield s, waiter, tmp_path


def _pump(s: Supervisor, cycles: int = 50, interval: float = 0.005) -> None:
    """Drive the supervisor's periodic maintenance for a short window."""
    for _ in range(cycles):
        s._reap_runners()
        s._enforce_timeouts(time.monotonic())
        if all(r.finalized for r in s.runs.values()):
            return
        time.sleep(interval)


def test_timeout_path_kills_reaps_and_cleans(inproc_supervisor):
    s, waiter, tmp = inproc_supervisor
    run = s.submit_run(b"m", b"{}")
    pid = run.pid
    assert pid is not None
    waiter.exited[pid] = 0  # pretend the runner would exit cleanly later

    # Force the deadline into the past and run maintenance.
    run.deadline = time.monotonic() - 0.01
    t0 = time.monotonic()
    _pump(s)
    # The kill/reap/cleanup decision is made by the maintenance sweep itself,
    # not after the full 2 second budget.
    assert time.monotonic() - t0 < 0.5

    assert run.finalized
    assert run.state == STATE_TIMEOUT
    assert run.returncode == supmod.KILLED_BY_SUPERVISOR
    assert pid not in s.pid_to_run
    assert not Path(run.staging_dir).exists()


def test_api_death_purges_everything_within_one_second(inproc_supervisor):
    s, waiter, tmp = inproc_supervisor
    runs = [s.submit_run(b"m", b"{}") for _ in range(4)]
    pids = {r.pid for r in runs}
    assert all(p is not None for p in pids)
    assert all(Path(r.staging_dir).exists() for r in runs)

    t0 = time.monotonic()
    s._handle_api_death()
    # The kill decision for every run is synchronous.
    assert waiter.killed == pids
    # One reap sweep finalises all of them (well inside the 1s budget).
    s._reap_runners()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0

    for run in runs:
        assert run.finalized
        assert run.state == STATE_API_DEAD
        assert run.returncode == supmod.KILLED_BY_SUPERVISOR
        assert not Path(run.staging_dir).exists()
    assert s.pid_to_run == {}


def test_api_death_during_runner_startup_is_covered(inproc_supervisor, monkeypatch):
    s, waiter, tmp = inproc_supervisor
    captured: dict[str, object] = {}
    real_spawn = s._spawn_runner

    def slow_spawn(run):
        # API dies while Popen is still in flight: no pid exists yet.
        s._handle_api_death()
        captured["finalised_during_spawn"] = run.finalized
        real_spawn(run)

    monkeypatch.setattr(s, "_spawn_runner", slow_spawn)
    run = s.submit_run(b"m", b"{}")

    # The pre-fork finalisation happened first, and the later Popen did not
    # resurrect or leak the run.
    assert captured["finalised_during_spawn"] is True
    assert run.finalized
    assert run.state == STATE_API_DEAD
    assert not Path(run.staging_dir).exists()


def test_output_overflow_path(inproc_supervisor):
    s, waiter, tmp = inproc_supervisor
    run = s.submit_run(b"m", b"{}")
    # Simulate the drain thread observing the cap being crossed.
    run.overflow = True
    s._kill_run(run, STATE_OUTPUT_TOO_LARGE)
    assert waiter.killed == {run.pid}
    s._reap_runners()
    assert run.finalized
    assert run.state == STATE_OUTPUT_TOO_LARGE
    assert not Path(run.staging_dir).exists()


def test_restart_epoch_cannot_touch_old_runs(inproc_supervisor):
    s, waiter, tmp = inproc_supervisor
    old = s.submit_run(b"m", b"{}")
    old_dir = Path(old.staging_dir)
    s._handle_api_death()
    s._reap_runners()
    assert old.finalized and not old_dir.exists()

    # A brand-new supervisor instance (as after an API restart) has an empty
    # run table and a freshly swept staging root.
    runner = tmp / "fake_runner.py"
    s2 = Supervisor(
        socket_path=str(tmp / "control2.sock"),
        staging_root=str(tmp / "runs"),
        runner_path=runner,
        manage_api=False,
    )
    waiter2 = _FakeWaiter()
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(supmod.os, "waitpid", waiter2.waitpid)
    monkeypatch.setattr(s2, "_kill_pg", waiter2.fake_kill_pg)
    try:
        s2._prepare_dirs()
        assert old.run_id not in s2.runs
        new_run = s2.submit_run(b"m2", b"{}")
        assert new_run.run_id != old.run_id
        assert Path(new_run.staging_dir).exists()
        assert new_run.staging_dir != str(old_dir)
    finally:
        monkeypatch.undo()


def test_pidless_run_finalised_without_signals(inproc_supervisor):
    """A run that fails before fork never needs kill(2) and leaves no files."""
    s, waiter, tmp = inproc_supervisor
    run = s.submit_run(b"m", b"{}")
    # Construct a pidless pending run directly, as a failed Popen would.
    from app.supervisor import Run

    pending = Run(
        run_id="pidless1234",
        staging_dir=str(tmp / "runs" / "run-pidless1234"),
        module_path=str(tmp / "runs" / "run-pidless1234" / "plugin.wasm"),
        log_path=str(tmp / "runs" / "run-pidless1234" / "input.json"),
        created_at=time.monotonic(),
        deadline=time.monotonic() + 2,
    )
    os.makedirs(pending.staging_dir)
    open(pending.module_path, "wb").write(b"m")
    s.runs[pending.run_id] = pending
    s._handle_api_death()
    assert pending.finalized
    assert pending.state == STATE_API_DEAD
    assert not Path(pending.staging_dir).exists()
    # And the normal run from submit_run is unaffected by that cleanup.
    assert not run.finalized


def test_main_loop_reaps_and_prunes_finalised_runs(inproc_supervisor, monkeypatch):
    """Drive the real run() maintenance cycle with a stubbed select()."""
    import select as select_mod

    s, waiter, tmp = inproc_supervisor
    s._open_listen_socket()
    run = s.submit_run(b"m", b"{}")
    assert run.pid is not None
    # Deadline already elapsed; the first maintenance pass must time the run
    # out (which fakes the SIGKILL) and the second must reap it.
    run.deadline = time.monotonic() - 0.01

    iterations = {"n": 0}

    def fake_select(rlist, wlist, xlist, timeout):
        iterations["n"] += 1
        if iterations["n"] >= 3:
            s.shutting_down = True
        return [], [], []

    monkeypatch.setattr(select_mod, "select", fake_select)
    monkeypatch.setattr(
        s, "_all_quiescent",
        lambda: all(r.finalized for r in s.runs.values()),
    )
    monkeypatch.setattr(s, "_shutdown", lambda: 0)

    rc = s.run()
    assert rc == 0
    assert run.finalized
    assert run.state == STATE_TIMEOUT
    assert run.returncode == supmod.KILLED_BY_SUPERVISOR
    assert not Path(run.staging_dir).exists()


def test_finalised_runs_are_pruned_after_retention(inproc_supervisor):
    s, waiter, tmp = inproc_supervisor
    run = s.submit_run(b"m", b"{}")
    waiter.killed.add(run.pid)
    s._reap_runners()
    assert run.finalized
    # Age it beyond the retention window.
    run.finished_at = time.monotonic() - (
        supmod.FINALISED_RETENTION_SECONDS + 1
    )
    s._prune_finalised(time.monotonic())
    assert run.run_id not in s.runs
