FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TMPDIR=/tmp \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    WASM_SANDBOX_RUNTIME_ROOT=/tmp/wasm-sandbox

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY demo ./demo

RUN groupadd --system --gid 10001 sandbox \
    && useradd --system --uid 10001 --gid sandbox \
       --no-create-home --shell /usr/sbin/nologin sandbox

# PID 1 is the supervisor: it starts the single-worker API as an unprivileged
# child, owns every plugin runner, and survives API SIGSTOP / SIGKILL so that
# in-flight untrusted plugins are always terminated and reaped.
CMD ["python", "-B", "-m", "app.supervisor"]

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).read()"]
