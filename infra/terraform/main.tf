terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

locals {
  name = "${var.prefix}-${random_string.suffix.result}"
  tags = merge(var.tags, { project = "vehicle-telemetry-lakehouse" })
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# --- Ingestion: Event Hubs (Kafka endpoint) ----------------------------------

resource "azurerm_eventhub_namespace" "this" {
  name                = "evhns-${local.name}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "Standard" # Standard or above exposes the Kafka endpoint
  capacity            = var.eventhubs_throughput_units
  tags                = local.tags
}

resource "azurerm_eventhub" "telemetry" {
  name              = "vehicle-telemetry"
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = var.eventhub_partitions
  message_retention = 1
}

resource "azurerm_eventhub_consumer_group" "lakehouse" {
  name                = "lakehouse-bronze"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.this.name
}

# Least privilege: the simulator can only send, the Lakehouse can only listen.
resource "azurerm_eventhub_authorization_rule" "producer" {
  name                = "simulator-send"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.this.name
  send                = true
  listen              = false
  manage              = false
}

resource "azurerm_eventhub_authorization_rule" "consumer" {
  name                = "lakehouse-listen"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.this.name
  send                = false
  listen              = true
  manage              = false
}

# --- Storage: ADLS Gen2 for the Lakehouse ------------------------------------

resource "azurerm_storage_account" "lake" {
  name                     = substr(replace("st${local.name}", "-", ""), 0, 24)
  location                 = azurerm_resource_group.this.location
  resource_group_name      = azurerm_resource_group.this.name
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # hierarchical namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_container" "lakehouse" {
  name                  = "lakehouse"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# --- Compute: Azure Databricks (Premium for Unity Catalog) --------------------

resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-${local.name}"
  location                    = azurerm_resource_group.this.location
  resource_group_name         = azurerm_resource_group.this.name
  sku                         = "premium"
  managed_resource_group_name = "rg-${local.name}-dbw-managed"
  tags                        = local.tags
}

# Lets Unity Catalog reach the storage account without keys.
resource "azurerm_databricks_access_connector" "this" {
  name                = "dbac-${local.name}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  identity {
    type = "SystemAssigned"
  }
  tags = local.tags
}

resource "azurerm_role_assignment" "connector_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}
