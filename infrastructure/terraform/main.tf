module "sqlserver" {
    source = "./modules/sqlserver/"
}

module "blob" {
    source = "./modules/blob/"
}

module "datafactory" {
    source = "./modules/datafactory/"
}

module "databricks" {
    source = "./modules/databricks/"
}

module "synapses" {
    source = "./modules/synapses/"
}