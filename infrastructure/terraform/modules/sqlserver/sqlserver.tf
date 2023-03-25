resource "azurerm_sql_server" "example" {
  name                         = "wu3sqlserver"
  resource_group_name          = "wu3group"
  location                     = "East US"
  version                      = "12.0"
  administrator_login          = "wu3user"
  administrator_login_password = "Wu3password"

  tags = {
    environment = "development"
  }
}

resource "azurerm_mssql_database" "test" {
  name           = "wu3database"
  server_id      = azurerm_sql_server.example.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  license_type   = "LicenseIncluded"
  max_size_gb    = 50
  sku_name       = "S0"

  tags = {
    foo = "bar"
  }
}