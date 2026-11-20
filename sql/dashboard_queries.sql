-- Queries behind the fleet dashboard (Databricks SQL or Power BI DirectQuery).

-- 1. Live fleet status: each vehicle's latest minute in the last 15 minutes
SELECT vehicle_id, powertrain, window_start, avg_speed_kph, max_engine_temp_c,
       min_energy_pct, last_lat, last_lon
FROM (
  SELECT *, row_number() OVER (PARTITION BY vehicle_id ORDER BY window_start DESC) AS rn
  FROM telemetry.gold.vehicle_minute_metrics
  WHERE window_start >= current_timestamp() - INTERVAL 15 MINUTES
)
WHERE rn = 1;

-- 2. Alerts per type over the last hour, by 5-minute bucket
SELECT window(event_ts, '5 minutes').start AS bucket, alert_type, count(*) AS alerts
FROM telemetry.gold.vehicle_alerts
WHERE event_ts >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY ALL
ORDER BY bucket, alert_type;

-- 3. Vehicles running hot right now (candidates for proactive service)
SELECT vehicle_id, max(max_engine_temp_c) AS peak_temp_c, sum(km_driven) AS km_last_hour
FROM telemetry.gold.vehicle_minute_metrics
WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY vehicle_id
HAVING peak_temp_c > 110
ORDER BY peak_temp_c DESC;

-- 4. Daily fleet trend
SELECT event_date, powertrain, active_vehicles, km_driven, harsh_brakes,
       vehicles_with_dtc, vehicles_overheated
FROM telemetry.gold.fleet_daily_summary
ORDER BY event_date DESC, powertrain;

-- 5. Data quality: share of events quarantined, by rule, today
SELECT rule, count(*) AS records
FROM telemetry.silver.vehicle_telemetry_quarantine
LATERAL VIEW explode(dq_failed_rules) AS rule
WHERE quarantine_date = current_date()
GROUP BY rule
ORDER BY records DESC;

-- 6. Freshness: p50 / p95 seconds from reading on the vehicle to Silver, last hour
SELECT percentile_approx(ingest_delay_s, array(0.5, 0.95)) AS p50_p95_seconds
FROM telemetry.silver.vehicle_telemetry
WHERE ingested_at >= current_timestamp() - INTERVAL 1 HOUR;
