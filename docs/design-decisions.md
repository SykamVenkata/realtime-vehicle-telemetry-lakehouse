# Design decisions

Why the pipeline is built the way it is, and what each choice trades off.

## 1. Event time, not arrival time

Vehicles lose signal in tunnels and parking garages and send buffered readings
minutes later. Every aggregation uses `event_ts` (when the reading was taken),
never the Kafka timestamp, so a delayed reading lands in the minute it
belongs to. `ingest_delay_s` on every Silver row measures the gap, which makes
freshness observable (`sql/dashboard_queries.sql`, query 6).

## 2. Two watermarks, for two different jobs

| Where | Watermark | Why |
| --- | --- | --- |
| Silver deduplication | 20 minutes | Device retries can arrive late. A longer window catches more duplicates; state stays bounded because only 20 minutes of event IDs are kept. |
| Gold minute metrics | 10 minutes | Dashboards want minutes to settle quickly. Readings later than 10 minutes still reach Silver (nothing is lost), but no longer change their Gold minute. |

The generator sends late events 5 to 15 minutes old on purpose, so both paths
are exercised: some late readings correct their minute, the oldest ones don't.

## 3. Quarantine instead of drop

A record that breaks a rule goes to `vehicle_telemetry_quarantine` with the
names of the rules it broke and its raw payload. That gives three things a
silent filter can't: a count of bad data per rule (a firmware bug shows up as
a spike), the ability to fix and replay, and an audit trail.

Each rule is written once in `src/telemetry/quality.py` with a Spark SQL
expression and a Python predicate. A Spark test checks that both forms agree
on hundreds of generated events, so enforcement and tests can't drift apart.

## 4. Update mode plus MERGE for live Gold metrics

Delta's streaming sink only supports append and complete modes. Append mode
would hold each minute back until its watermark passes (10+ minutes of
latency); complete mode rewrites the whole table every trigger. Instead the
aggregation runs in **update** mode and each micro-batch is upserted with
`MERGE` on `(window_date, window_start, vehicle_id)`:

- a minute appears on the dashboard within one trigger (30 seconds on Databricks)
- a late reading updates its minute in place
- replaying a batch after a restart rewrites the same values, so it's idempotent

The daily summary uses the same idempotent MERGE in batch, recomputing the
last two days so late data is picked up without double counting.

## 5. Bronze keeps the raw payload

Bronze stores the message exactly as received, plus topic, partition, offset
and ingest time. If a parsing bug or a schema change ever corrupts Silver,
Silver can be rebuilt from Bronze without going back to the devices.

## 6. One Kafka API, two brokers

Event Hubs exposes a Kafka-compatible endpoint, so the same Spark Kafka
source reads from local Kafka in Docker and from Event Hubs in Azure. Only
the connection options change (`src/telemetry/config.py`), and the connection
string comes from a Databricks secret scope, never from code.

## 7. Layout and maintenance

- Bronze is partitioned by `ingest_date`, Silver by `event_date`, Gold metrics
  by `window_date`: date filters prune files, and old partitions can be
  expired independently.
- Streaming writes many small files, so the daily job compacts and Z-Orders
  Silver and Gold by `vehicle_id`, the most common filter, then vacuums
  versions older than 7 days.
- On Databricks, liquid clustering and predictive optimization can replace
  the partitioning and the maintenance job; the open-source version is kept
  here so the project runs anywhere.

## 8. Cost

- Event Hubs Standard with 1 throughput unit handles about 1,000 events per
  second, which covers a 500 to 1,000 vehicle simulation.
- Streaming jobs share one job cluster. `STREAM_TRIGGER=available-now` turns
  any streaming job into a run-and-stop batch job for cheap catch-up runs.
- Least-privilege Event Hubs keys: the simulator can only send, the Lakehouse
  can only listen.
