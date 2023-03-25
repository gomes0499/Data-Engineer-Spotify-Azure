resource "azurerm_storage_container" "example" {
  name                  = "datalake"
  storage_account_name  = "wu3storage"
  container_access_type = "private"
}

resource "azurerm_storage_blob" "landing" {
  name                   = "landing/"
  storage_account_name   = "wu3storage"
  storage_container_name = azurerm_storage_container.example.name
  type                   = "Block"
  content_type           = "application/octet-stream"
}

resource "azurerm_storage_blob" "process" {
  name                   = "process/"
  storage_account_name   = "wu3storage"
  storage_container_name = azurerm_storage_container.example.name
  type                   = "Block"
  content_type           = "application/octet-stream"
}

resource "azurerm_storage_blob" "curated" {
  name                   = "curated/"
  storage_account_name   = "wu3storage"
  storage_container_name = azurerm_storage_container.example.name
  type                   = "Block"
  content_type           = "application/octet-stream"
}
