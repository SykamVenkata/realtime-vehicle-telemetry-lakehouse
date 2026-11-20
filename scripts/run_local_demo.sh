#!/usr/bin/env bash
# Runs the whole pipeline inside the Docker "app" container:
# generator -> Kafka -> Bronze -> Silver -> Gold, all streaming at once.
set -euo pipefail

ROOT="${LAKEHOUSE_ROOT:-/app/data/lakehouse}"
VEHICLES="${DEMO_VEHICLES:-500}"
mkdir -p "$ROOT" /app/data/logs

echo "[demo] waiting for Kafka at ${KAFKA_BOOTSTRAP_SERVERS} ..."
python - <<'PY'
import os, socket, time
host, port = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(":")
for _ in range(60):
    try:
        socket.create_connection((host, int(port)), timeout=2).close()
        break
    except OSError:
        time.sleep(2)
else:
    raise SystemExit("Kafka did not come up")
PY

echo "[demo] starting generator: ${VEHICLES} vehicles, 1 event per vehicle per second"
python -m telemetry.generator --sink kafka --vehicles "$VEHICLES" \
  > /app/data/logs/generator.log 2>&1 &

python pipelines/bronze_ingest.py --lakehouse-root "$ROOT" > /app/data/logs/bronze.log 2>&1 &
python pipelines/silver_cleanse.py --lakehouse-root "$ROOT" > /app/data/logs/silver.log 2>&1 &
python pipelines/gold_stream.py --lakehouse-root "$ROOT" > /app/data/logs/gold.log 2>&1 &

echo "[demo] all jobs started. Logs: ./data/logs/. Run 'make show' in another terminal to inspect the tables."
wait -n
echo "[demo] a job exited; check ./data/logs/ for details" >&2
exit 1
