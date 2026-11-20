output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.this.name
}

output "eventhub_name" {
  value = azurerm_eventhub.telemetry.name
}

output "producer_connection_string" {
  description = "For the simulator: export EVENTHUBS_CONNECTION_STRING=..."
  value       = azurerm_eventhub_authorization_rule.producer.primary_connection_string
  sensitive   = true
}

output "consumer_connection_string" {
  description = "Store in a Databricks secret scope for the Bronze job"
  value       = azurerm_eventhub_authorization_rule.consumer.primary_connection_string
  sensitive   = true
}

output "lakehouse_root" {
  description = "Pass to the jobs as LAKEHOUSE_ROOT / the bundle's lakehouse_root variable"
  value       = "abfss://${azurerm_storage_container.lakehouse.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "access_connector_id" {
  description = "Use when creating the Unity Catalog storage credential"
  value       = azurerm_databricks_access_connector.this.id
}
