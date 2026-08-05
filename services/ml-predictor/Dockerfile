FROM python:3.11-slim AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY requirements.txt .
RUN uv pip install --system --no-cache --target=/deps -r requirements.txt

FROM python:3.11-slim

RUN adduser --disabled-password --uid 10001 app
WORKDIR /app

COPY --from=builder /deps /usr/local/lib/python3.11/site-packages
COPY app.py .
COPY models/ ./models/

RUN mkdir -p /tmp/prometheus_multiproc && chown app:app /tmp/prometheus_multiproc
USER app

EXPOSE 8084
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8084"]