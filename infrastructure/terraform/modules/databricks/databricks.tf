resource "azurerm_databricks_workspace" "example" {
  name                = "wu3databrick"
  resource_group_name = "wu3group"
  location            = "East US"
  sku                 = "standard"

  tags = {
    Environment = "Production"
  }
}