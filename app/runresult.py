"""Runner exit-code contract shared by the supervisor and the API.

The one-shot WASI runner (``app/runner.py``) exits with small fixed status
codes.  The supervisor forwards the raw code over the control socket and the
API translates it into the client-facing error.  Keeping the table in one
module guarantees the two processes cannot drift apart.
"""

from __future__ import annotations

from .errors import ApiError

# Exit codes emitted by app/runner.py.
EXIT_INVALID_MODULE = 20
EXIT_INSTANTIATE = 21
EXIT_TRAP = 22
EXIT_NONZERO_EXIT = 23
EXIT_NO_START = 24
EXIT_BAD_START = 25
EXIT_INTERNAL = 26

# Pseudo exit code used by the supervisor when the runner was killed by a
# signal (timeout, API death, shutdown, ...).  It never collides with a real
# runner status, all of which are below 30.
KILLED_BY_SUPERVISOR = 137

EXIT_CODES: dict[int, tuple[int, str, str]] = {
    EXIT_INVALID_MODULE: (
        422,
        "invalid_module",
        "The uploaded file is not a valid WebAssembly module.",
    ),
    EXIT_INSTANTIATE: (
        422,
        "plugin_instantiation_failed",
        "The module could not be instantiated or its imports could not be linked.",
    ),
    EXIT_TRAP: (422, "plugin_trap", "The plugin trapped while executing."),
    EXIT_NONZERO_EXIT: (
        422,
        "plugin_nonzero_exit",
        "The plugin exited with a non-zero status.",
    ),
    EXIT_NO_START: (
        422,
        "invalid_module",
        "The module does not export a WASI _start function.",
    ),
    EXIT_BAD_START: (
        422,
        "invalid_module",
        "The module's _start export has an invalid signature.",
    ),
    EXIT_INTERNAL: (500, "sandbox_internal_error", "The sandbox runner failed."),
}


def failure_for_returncode(returncode: int) -> ApiError:
    """Translate a non-zero runner exit into the documented API error."""
    mapping = EXIT_CODES.get(returncode)
    if mapping is None:
        return ApiError(
            502,
            "sandbox_process_failed",
            "The sandbox runner ended unexpectedly.",
        )
    status_code, code, message = mapping
    return ApiError(status_code, code, message)
