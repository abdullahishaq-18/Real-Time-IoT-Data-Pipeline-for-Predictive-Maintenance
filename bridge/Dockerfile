# MQTT -> Kafka bridge. Build context is ./bridge (see docker-compose.yml).
FROM python:3.12.16-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# confluent-kafka ships manylinux wheels with librdkafka bundled: no compiler needed.
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt \
 && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin bridge

WORKDIR /app
COPY --chown=root:root . /app/bridge/

USER bridge
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "/app/bridge/healthcheck.py"]
CMD ["python", "-m", "bridge.bridge"]
