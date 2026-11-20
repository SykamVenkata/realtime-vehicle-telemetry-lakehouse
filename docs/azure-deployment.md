# Deploying to Azure

End-to-end setup on Azure: Event Hubs, ADLS Gen2, Azure Databricks with Unity
Catalog, and a Power BI dashboard. Expect about an hour the first time.

> Cost: Event Hubs Standard, a storage account and two small Databricks job
> clusters. Stop the streaming job and run `terraform destroy` when you're done.

## Prerequisites

- An Azure subscription (a free account's credits are enough for a short run)
- [Terraform](https://developer.hashicorp.com/terraform/install) 1.6+, the
  [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) and the
  [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html)
- Python 3.11 with `pip install -e ".[producer]"` for the simulator

## 1. Provision the infrastructure

```bash
az login
cd infra/terraform
cp example.tfvars terraform.tfvars   # set your subscription_id
terraform init
terraform apply
```

Outputs you'll use: `lakehouse_root`, `databricks_workspace_url`,
`access_connector_id`, and the two sensitive connection strings:

```bash
terraform output -raw producer_connection_string
terraform output -raw consumer_connection_string
```

## 2. Set up Unity Catalog and secrets

1. Open the workspace URL. Make sure it's attached to a Unity Catalog metastore
   (Account console → Workspaces).
2. In a SQL editor, run `sql/unity_catalog_setup.sql` after replacing
   `<account>` and `<connector-id>`.
3. Store the consumer connection string as a secret:

```bash
databricks secrets create-scope telemetry
databricks secrets put-secret telemetry eventhubs-connection-string \
  --string-value "$(terraform -chdir=infra/terraform output -raw consumer_connection_string)"
```

## 3. Deploy and start the pipeline

```bash
databricks bundle validate
databricks bundle deploy -t dev --var lakehouse_root="$(terraform -chdir=infra/terraform output -raw lakehouse_root)"
databricks bundle run telemetry_streaming -t dev
```

The three streaming tasks start together; Silver and Gold wait until their
source tables exist.

## 4. Send traffic

From your machine:

```bash
export EVENTHUBS_CONNECTION_STRING="$(terraform -chdir=infra/terraform output -raw producer_connection_string)"
python -m telemetry.generator --sink kafka --vehicles 500
```

500 vehicles at one reading per second is about 30,000 events a minute.

## 5. Watch it work

- **Databricks SQL**: run the queries in `sql/dashboard_queries.sql`.
- **Power BI**: Get data → Azure Databricks, point it at the SQL warehouse,
  and pick the `telemetry.gold` tables. Use DirectQuery for the live views.
- **Spark UI**: each query shows input rate, processing rate and batch duration.

Capture for the README: events per second, p50/p95 ingest delay (query 6),
batch duration, and a screenshot of the dashboard.

## 6. Daily job

`telemetry_daily` runs at 01:30 UTC: the daily fleet summary, then compaction,
Z-Ordering and vacuum. Run it by hand with
`databricks bundle run telemetry_daily -t dev`.

## 7. Tear down

```bash
databricks bundle destroy -t dev
cd infra/terraform && terraform destroy
```
