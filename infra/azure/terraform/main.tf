terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Variables
variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "rg-data-platform-demo"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "East US"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "dataplatform"
}

# Optional features (disabled by default for cost optimization)
variable "enable_aks" {
  description = "Enable AKS cluster (additional cost)"
  type        = bool
  default     = false
}

variable "enable_adf" {
  description = "Enable Azure Data Factory (additional cost)"
  type        = bool
  default     = false
}

# Local values for consistent naming
locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
    CostCenter  = "demo"
  }
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.common_tags
}

# Storage Account for ADLS Gen2
resource "azurerm_storage_account" "adls" {
  name                     = replace("${local.name_prefix}adls", "-", "")
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"  # Lowest cost option
  account_kind             = "StorageV2"
  is_hns_enabled          = true   # Enables Data Lake Gen2

  # Cost optimization
  access_tier = "Hot"
  
  # Enable blob versioning for data protection
  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 7  # Minimal retention for cost savings
    }
  }

  tags = local.common_tags
}

# Storage Containers for different data layers
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "delta" {
  name                  = "delta"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "mlflow" {
  name                  = "mlflow"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# Event Hubs Namespace (Basic tier for cost optimization)
resource "azurerm_eventhub_namespace" "main" {
  name                = "${local.name_prefix}-eventhubs"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Basic"  # Lowest cost tier
  capacity            = 1        # Minimum capacity

  tags = local.common_tags
}

# Event Hub for orders
resource "azurerm_eventhub" "orders" {
  name                = "orders"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2    # Minimum for Basic tier
  message_retention   = 1    # Minimum retention in days
}

# Event Hub Authorization Rule
resource "azurerm_eventhub_authorization_rule" "orders_send_listen" {
  name                = "orders-send-listen"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.orders.name
  resource_group_name = azurerm_resource_group.main.name

  listen = true
  send   = true
  manage = false
}

# Optional: AKS Cluster (disabled by default)
resource "azurerm_kubernetes_cluster" "main" {
  count               = var.enable_aks ? 1 : 0
  name                = "${local.name_prefix}-aks"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = "${local.name_prefix}-aks"

  default_node_pool {
    name       = "default"
    node_count = 1
    vm_size    = "Standard_B2s"  # Cost-optimized VM size
    
    # Cost optimization
    enable_auto_scaling = false
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

# Optional: Azure Data Factory (disabled by default)
resource "azurerm_data_factory" "main" {
  count               = var.enable_adf ? 1 : 0
  name                = "${local.name_prefix}-adf"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  tags = local.common_tags
}

# Outputs
output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account"
  value       = azurerm_storage_account.adls.name
}

output "adls_abfss_base_url" {
  description = "Base ABFSS URL for ADLS Gen2"
  value       = "abfss://${azurerm_storage_account.adls.name}.dfs.core.windows.net"
}

output "eventhub_namespace_name" {
  description = "Name of the Event Hubs namespace"
  value       = azurerm_eventhub_namespace.main.name
}

output "eventhub_bootstrap_servers" {
  description = "Event Hubs Kafka-compatible bootstrap servers"
  value       = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
}

output "eventhub_connection_string" {
  description = "Event Hubs connection string for orders hub"
  value       = azurerm_eventhub_authorization_rule.orders_send_listen.primary_connection_string
  sensitive   = true
}

output "aks_cluster_name" {
  description = "Name of the AKS cluster (if enabled)"
  value       = var.enable_aks ? azurerm_kubernetes_cluster.main[0].name : null
}

output "data_factory_name" {
  description = "Name of the Azure Data Factory (if enabled)"
  value       = var.enable_adf ? azurerm_data_factory.main[0].name : null
}

# Cost estimation note
output "cost_estimation" {
  description = "Estimated monthly cost breakdown (USD)"
  value = {
    storage_account_lrs = "~$5-20 (depending on data volume)"
    eventhub_basic     = "~$11 (basic tier, 1 throughput unit)"
    aks_basic_node     = var.enable_aks ? "~$30 (1 x Standard_B2s node)" : "Not enabled"
    data_factory       = var.enable_adf ? "~$0.50 + execution costs" : "Not enabled"
    total_baseline     = "~$16-31 (without optional services)"
  }
}