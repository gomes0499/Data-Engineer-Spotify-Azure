terraform {
  backend "azurerm" {
    resource_group_name  = "wu3group"
    storage_account_name = "wu3storage"
    container_name       = "wu3tfstate"
    key                  = "terraform.tfstate"
  }
}
