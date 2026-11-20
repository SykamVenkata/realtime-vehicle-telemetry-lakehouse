-- Register the Lakehouse in Unity Catalog so it is governed, discoverable and
-- queryable by name (Databricks SQL, Power BI, notebooks).
-- Replace <account> with your storage account and <connector-id> with the
-- Terraform output access_connector_id.

CREATE STORAGE CREDENTIAL IF NOT EXISTS telemetry_cred
  WITH (AZURE_MANAGED_IDENTITY = (ACCESS_CONNECTOR_ID = '<connector-id>'));

CREATE EXTERNAL LOCATION IF NOT EXISTS telemetry_lake
  URL 'abfss://lakehouse@<account>.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL telemetry_cred);

CREATE CATALOG IF NOT EXISTS telemetry;
CREATE SCHEMA IF NOT EXISTS telemetry.bronze;
CREATE SCHEMA IF NOT EXISTS telemetry.silver;
CREATE SCHEMA IF NOT EXISTS telemetry.gold;

CREATE TABLE IF NOT EXISTS telemetry.bronze.vehicle_telemetry_raw
  LOCATION 'abfss://lakehouse@<account>.dfs.core.windows.net/bronze/vehicle_telemetry_raw';
CREATE TABLE IF NOT EXISTS telemetry.silver.vehicle_telemetry
  LOCATION 'abfss://lakehouse@<account>.dfs.core.windows.net/silver/vehicle_telemetry';
CREATE TABLE IF NOT EXISTS telemetry.silver.vehicle_telemetry_quarantine
  LOCATION 'abfss://lakehouse@<account>.dfs.core.windows.net/silver/vehicle_telemetry_quarantine';
CREATE TABLE IF NOT EXISTS telemetry.gold.vehicle_minute_metrics
  LOCATION 'abfss://lakehouse@<account>.dfs.core.windows.net/gold/vehicle_minute_metrics';
CREATE TABLE IF NOT EXISTS telemetry.gold.vehicle_alerts
  LOCATION 'abfss://lakehouse@<account>.dfs.core.windows.net/gold/vehicle_alerts';
CREATE TABLE IF NOT EXISTS telemetry.gold.fleet_daily_summary
  LOCATION 'abfss://lakehouse@<account>.dfs.core.windows.net/gold/fleet_daily_summary';

-- Governance: analysts read Gold only; engineers own every layer.
GRANT USE CATALOG ON CATALOG telemetry TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA telemetry.gold TO `data-analysts`;
GRANT ALL PRIVILEGES ON CATALOG telemetry TO `data-engineers`;

-- Location is personal data: mask exact coordinates for anyone outside engineering.
CREATE OR REPLACE FUNCTION telemetry.gold.mask_coordinate(v DOUBLE)
  RETURN CASE WHEN is_account_group_member('data-engineers') THEN v ELSE round(v, 2) END;

ALTER TABLE telemetry.gold.vehicle_alerts ALTER COLUMN lat SET MASK telemetry.gold.mask_coordinate;
ALTER TABLE telemetry.gold.vehicle_alerts ALTER COLUMN lon SET MASK telemetry.gold.mask_coordinate;
