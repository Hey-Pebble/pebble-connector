FROM python:3.11-slim AS builder

WORKDIR /app

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.in-project true

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --no-interaction

COPY src/ src/

FROM python:3.11-slim

WORKDIR /app

# Pin a deterministic non-root UID/GID so Kubernetes runAsNonRoot validation
# (and Pod Security Standards "restricted") can verify it without resolving
# /etc/passwd at runtime. Keep this in sync with helm values runAsUser/runAsGroup.
RUN groupadd -r -g 10001 pebble && useradd -r -u 10001 -g pebble pebble

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH"

USER 10001:10001

CMD ["python", "-m", "src.main"]
