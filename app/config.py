"""Limits and local control-plane locations shared by the API and supervisor."""

import os

MAX_MODULE_BYTES = 1 * 1024 * 1024
MAX_LOG_BYTES = 64 * 1024
MAX_PLUGIN_OUTPUT_BYTES = 64 * 1024
PLUGIN_TIMEOUT_SECONDS = 2.0
PLUGIN_MEMORY_BYTES = 64 * 1024 * 1024

# Multipart framing is tiny compared with the payloads, but leave room for
# headers and boundaries so the largest allowed module and log are accepted.
MAX_REQUEST_BYTES = MAX_MODULE_BYTES + MAX_LOG_BYTES + 192 * 1024

# --- local control plane -------------------------------------------------
# The API worker and the runner supervisor are separate processes.  They only
# communicate over this filesystem-scoped Unix domain socket; the socket and
# every per-run staging directory live on the container's tmpfs so that an API
# restart (or replacement) can never reach another instance's files.
RUNTIME_ROOT = os.environ.get("WASM_SANDBOX_RUNTIME_ROOT", "/tmp/wasm-sandbox")
CONTROL_SOCKET_PATH = os.path.join(RUNTIME_ROOT, "control.sock")
STAGING_ROOT = os.path.join(RUNTIME_ROOT, "runs")

# Prefix used when sweeping staging directories left behind by an earlier
# supervisor instance (crash, image rebuild with a tmpfs that survived, ...).
STAGING_PREFIX = "run-"

# How often the supervisor's event loop wakes up.  The 2-second runner wall
# clock and the 1-second post-SIGKILL cleanup budget are both enforced off the
# monotonic clock, so this value only bounds detection latency.
SUPERVISOR_TICK_SECONDS = 0.02

# Upper bound on a single control-plane JSON frame (module + log + framing).
MAX_CONTROL_FRAME_BYTES = MAX_REQUEST_BYTES + 64 * 1024

# The unprivileged user the API and every runner run as.  Overridable for the
# local test environment, which may not provide the container account.
SANDBOX_UID = int(os.environ.get("WASM_SANDBOX_UID", "10001"))
SANDBOX_GID = int(os.environ.get("WASM_SANDBOX_GID", "10001"))
