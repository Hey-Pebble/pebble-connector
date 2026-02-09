FROM python:3.11-slim AS builder

WORKDIR /app

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.in-project true

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --no-interaction

COPY src/ src/

FROM python:3.11-slim

WORKDIR /app

RUN groupadd -r pebble && useradd -r -g pebble pebble

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH"

USER pebble

CMD ["python", "-m", "src.main"]
