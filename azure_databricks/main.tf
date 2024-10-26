# Configure the Azure provider
provider "azurerm" {
  features {}

  subscription_id = ""
  client_id       = ""
  client_secret   = ""
  tenant_id       = ""
}

# Variables for resource naming and region
variable "location" {
  default = "East US"
}

variable "resource_group_name" {
  default = "databricks_demo_rg"
}

# Create a resource group
resource "azurerm_resource_group" "etl_rg" {
  name     = "etl-demo-rg"
  location = "eastus"
}

resource "azurerm_databricks_workspace" "etl_databricks" {
  name                = "demo_etl_wsp"
  location            = azurerm_resource_group.etl_rg.location
  resource_group_name = azurerm_resource_group.etl_rg.name
  sku                 = "premium"
}


resource "azurerm_storage_account" "etl_storage" {
  name                     = "etldemoresource"
  resource_group_name      = azurerm_resource_group.etl_rg.name
  location                 = azurerm_resource_group.etl_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "etl_container" {
  name                  = "etl-demo"
  storage_account_name  = azurerm_storage_account.etl_storage.name
  container_access_type = "private"
}

output "storage_account_name" {
  value = azurerm_storage_account.etl_storage.name
}

output "storage_account_key" {
  value = azurerm_storage_account.etl_storage.primary_access_key
  sensitive = true
}

output "databricks_host" {
  value = azurerm_databricks_workspace.etl_databricks.workspace_url
}