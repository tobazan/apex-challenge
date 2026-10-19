# ONBOARDING PARA NUEVO TENANT

## Onboarding

Para un nuevo tenant/pais, a grandes rasgos los siguientes requisitos deben ser cubiertos:

1. Asumiendo el proveedor de nube Azure
    - 2.1 Entra Groups y sus privilegios RBAC
    - 2.2 Una Storage Account en ADLS y los containers necesarios
    - 2.3 Las storage credentials para estas ext locations
    - 2.4 Una Key Vault para que la identity/svc ppal de Databricks acceda a secretos
2. Una entrada en los YAML file correspondientes dentro de `config/` y rebuildear
3. En Databricks
    - 3.1 Un nuevo catalogo con los schemas por capa en Unity Catalog
    - 3.2 Acceso y privilegios para los Entra Group al workspace
    - 3.3 Parametrizar el Job/Workflow para correr el SAAS_PIPELINE con el nuevo tenant

El **1** es mas de **IaC** y **Cloud Admin**

El **2** es mas de mantenimiento del codigo 

El **3** es propio de **Databricks**

**Pre Requisitos**
Se asumen ya disponibles
- Azure Subscription + Resource Group + Databricks
- Azure ARM provider identity para **1** y **3**
- Un Terraform state en Git *(al tanto de los recursos ya provisionados)*
- Una KV para secretos *(si se comparte)*

## Repo Layout
Se sugiere algo al estilo

```text
infra/
├── modules/
├── tenant/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── envs/
│   ├── dev/
│   │   ├── main.tf
│   │   └── terraform.tfvars
│   ├── qa/
│   └── main/  
```

Que sea reutilizable por tenant y ambiente

## VERIFICAR

**Pre Apply**

Los cambios en el modulo de Terraform antes de desplegar/provisionar deberian ser parte del Workflow de CI para evitar errores de sintaxis, formato, configuraciones no validas y posibles regresiones

- `terraform fmt -check`
- `terraform init`
- `terraform validate`
- `terraform plan -var="tenant-code" -out="tenant-code.tfplan"`

**Post Apply**
- Seguridad de Storage Account y acceso de la identidad Databricks
- Privilegios de Entra Groups en Azure *(least privilege)*
    - Reader group solo container Gold y quizas Shared
    - Steward group containers Bronze, Gold, Silver y Shared
    - Data Team group contributor de la storage account completa 
- Privilegios de Entra Groups en Databricks *(least privilege)*
    - Reader group solo `SELECT` schema Gold y quizas Shared
    - Steward group `SELECT`, `APPLY TAG`, etc
    - Data Team group owners 
- Los logs de la KV solo muestran actividad de la identidad de Databricks

## SNIPPETS

### Providers e Inputs
```hcl
terraform { 
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = { }
    azuread = { }
    databricks = { }
  }
}

variable "environment" {}
variable "tenant_code" {}
variable "location" {}
variable "resource_group_name" {}
variable "storage_replication_type" {}

locals {
  prefix = "saas-${var.environment}-${var.tenant_code}"
  tags = {
    application = "saas-medallion-pipeline"
    environment = var.environment
    tenant      = var.tenant_code
    managed_by  = "terraform"
  }
}
```
### Entra Groups
```hcl
resource "azuread_group" "data_engineers" {
  display_name     = "grp-saas-${var.environment}-${var.tenant_code}-data-engineers"
  security_enabled = true
  description      = "Data engineering operators for ${var.tenant_code}"
}
resource "azuread_group" "analyst_readers" { }
resource "azuread_group" "service_principals" { }
```

### ADLS
```hcl
resource "azurerm_storage_account" "tenant" {
  name                     = replace("st${var.environment}${var.tenant_code}data", "-", "")
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  https_traffic_only_enabled = true
  shared_access_key_enabled  = false

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 30
    }
  }

  tags = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["landing", "bronze", "silver", "quarantine", "gold", "checkpoints"])
  name               = each.key
  storage_account_id = azurerm_storage_account.tenant.id
}
```

### KEY VAULT
```hcl
resource "azurerm_key_vault" "tenant" {
  name                       = replace("kv-${var.environment}-${var.tenant_code}", "-", "")
  location                   = var.location
  resource_group_name        = var.resource_group_name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = true
  soft_delete_retention_days = 90
  enable_rbac_authorization  = true

  tags = local.tags
}

data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "databricks_key_vault_reader" {
  scope                = azurerm_key_vault.tenant.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.pipeline_principal_object_id
}
```

### STORAGE ACCESS
```hcl
variable "pipeline_principal_object_id" { }

variable "databricks_host" { }

variable "databricks_pipeline_principal" { }

variable "databricks_storage_credential_name" { }

resource "azurerm_role_assignment" "pipeline_storage" {
  scope                = azurerm_storage_account.tenant.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.pipeline_principal_object_id
}
```

### UNITY CATALOG
```hcl
provider "databricks" { }

resource "databricks_catalog" "tenant" {
  name         = "saas_${var.environment}_${var.tenant_code}"
  comment      = "Catalog for tenant ${var.tenant_code}"
  isolation_mode = "ISOLATED"
  properties = {
    environment = var.environment
    tenant      = var.tenant_code
  }
}

resource "databricks_schema" "layer" {
  for_each     = toset(["bronze", "silver", "gold", "silver_quarantine"])
  catalog_name = databricks_catalog.tenant.name
  name         = each.key
  comment      = "${each.key} layer for ${var.tenant_code}"
}

resource "databricks_grants" "tenant_readers" {
  catalog = databricks_catalog.tenant.name

  grant {
    principal  = azuread_group.analyst_readers.display_name
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "gold_readers" {
  schema = "${databricks_catalog.tenant.name}.gold"

  grant {
    principal  = azuread_group.analyst_readers.display_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "pipeline_silver_writer" {
  schema = "${databricks_catalog.tenant.name}.silver"

  grant {
    principal  = var.databricks_pipeline_principal
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
}
```

### EXT LOCATIONS
```hcl
resource "databricks_external_location" "tenant_bronze" {
  name            = "${var.environment}_${var.tenant_code}_bronze"
  url             = "abfss://bronze@${azurerm_storage_account.tenant.name}.dfs.core.windows.net/"
  credential_name = var.databricks_storage_credential_name
  comment         = "Bronze external location for ${var.tenant_code}"
}
```