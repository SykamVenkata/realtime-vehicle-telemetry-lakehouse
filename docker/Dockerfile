FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY pipelines ./pipelines
COPY scripts ./scripts

# Download the Delta and Kafka connector JARs once at build time, so the four
# Spark jobs don't race each other to fetch them when the demo starts.
RUN python -c "import sys; sys.path.insert(0, 'src'); \
from telemetry.spark import get_spark; get_spark('warmup', with_kafka=True).stop()"

CMD ["bash", "scripts/run_local_demo.sh"]
