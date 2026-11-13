FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

FROM base AS test

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY pyproject.toml .
COPY alembic.ini .
COPY alembic ./alembic
COPY src ./src
COPY tests ./tests
COPY contracts ./contracts

CMD ["pytest", "--cov=src/app", "--cov-report=term-missing"]

FROM base AS runtime

COPY alembic.ini .
COPY alembic ./alembic
COPY scripts/start.sh ./scripts/start.sh
COPY scripts/start-publisher.sh ./scripts/start-publisher.sh
COPY scripts/start-consumer.sh ./scripts/start-consumer.sh
COPY src ./src
COPY contracts ./contracts

RUN addgroup --system appgroup \
    && adduser --system --ingroup appgroup appuser \
    && chmod +x ./scripts/start.sh ./scripts/start-publisher.sh ./scripts/start-consumer.sh

USER appuser
EXPOSE 8002

CMD ["./scripts/start.sh"]
