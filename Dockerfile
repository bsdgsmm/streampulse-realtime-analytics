FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    STREAMPULSE_SQL_DIR=/app/sql

WORKDIR /app

RUN useradd --create-home --uid 10001 streampulse

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[dashboard,report]"

COPY sql ./sql
COPY scripts ./scripts
COPY dashboard ./dashboard

USER streampulse

CMD ["python", "-m", "streampulse.consumer.main", "--help"]
