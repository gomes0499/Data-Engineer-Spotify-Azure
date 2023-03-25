resource "azurerm_storage_account" "example" {
  name                     = "wu3synapsestorage"
  resource_group_name      = "wu3group"
  location                 = "East US"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "example" {
  name               = "wu3gen2"
  storage_account_id = azurerm_storage_account.example.id
}

# resource "azurerm_synapse_workspace" "example" {
#   name                                 = "wu3synapseworkspace"
#   resource_group_name                  = "wu3group"
#   location                             = "East US"
#   storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.example.id
#   sql_administrator_login              = "wu3user"
#   sql_administrator_login_password     = "Wu3password"

#   aad_admin {
#     login     = "AzureAD Admin"
#     object_id = "00000000-0000-0000-0000-000000000000"
#     tenant_id = "00000000-0000-0000-0000-000000000000"
#   }

#   identity {
#     type = "SystemAssigned"
#   }

#   tags = {
#     Env = "production"
#   }
# }