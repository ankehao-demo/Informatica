terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  # Reads DATABRICKS_HOST and DATABRICKS_TOKEN from env vars automatically
}
