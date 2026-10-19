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

# -----------------------------------------------------------------------------
# EHRP2BIIS pilot (port of Informatica wf_EHRP2BIIS_UPDATE) -- see docs/migration/
# -----------------------------------------------------------------------------

variable "ehrp2biis_git_url" {
  description = "Repository holding pipelines/ehrp2biis"
  type        = string
  default     = "https://github.com/ankehao-demo/Informatica"
}

variable "ehrp2biis_git_branch" {
  type    = string
  default = "master"
}

variable "ehrp2biis_catalog" {
  description = "Unity Catalog catalog for the bronze/silver schemas"
  type        = string
  default     = "main"
}

variable "ehrp2biis_spark_version" {
  type    = string
  default = "15.4.x-scala2.12" # LTS; Spark 3.5 / Delta 3.2 (matches local tests)
}

variable "ehrp2biis_node_type_id" {
  type    = string
  default = "i3.xlarge"
}

variable "ehrp2biis_failure_emails" {
  description = "Recipients for Databricks job failure notifications (Airflow owns end-to-end alerting)"
  type        = list(string)
  default     = []
}

resource "databricks_secret_scope" "ehrp2biis" {
  name = "ehrp2biis"
  # Values are written out-of-band so they never land in Terraform state:
  #   databricks secrets put-secret ehrp2biis oracle-jdbc-url
  #   databricks secrets put-secret ehrp2biis oracle-user
  #   databricks secrets put-secret ehrp2biis oracle-password
}

resource "databricks_schema" "ehrp2biis_bronze" {
  catalog_name = var.ehrp2biis_catalog
  name         = "ehrp2biis_bronze"
  comment      = "Per-LOAD_ID snapshots of the PeopleSoft/NKNIGHT inputs of m_EHRP2BIIS_UPDATE"
}

resource "databricks_schema" "ehrp2biis_silver" {
  catalog_name = var.ehrp2biis_catalog
  name         = "ehrp2biis_silver"
  comment      = "NWK_ACTION_PRIMARY_TBL / NWK_ACTION_SECONDARY_TBL / EHRP_RECS_TRACKING_TBL + rejects"
}

locals {
  ehrp2biis_common_args = [
    "--run-date", "{{job.parameters.run_date}}",
    "--bronze", "${var.ehrp2biis_catalog}.${databricks_schema.ehrp2biis_bronze.name}",
    "--silver", "${var.ehrp2biis_catalog}.${databricks_schema.ehrp2biis_silver.name}",
    "--secret-scope", databricks_secret_scope.ehrp2biis.name,
  ]
  ehrp2biis_entry = "pipelines/run_ehrp2biis.py"
}

resource "databricks_job" "ehrp2biis_update" {
  name                = "ehrp2biis-update-pilot"
  max_concurrent_runs = 1 # one LOAD_ID at a time, like the Oracle work tables

  git_source {
    url      = var.ehrp2biis_git_url
    provider = "gitHub"
    branch   = var.ehrp2biis_git_branch
  }

  parameter {
    name    = "run_date"
    default = "" # Airflow always passes {{ ds }}
  }

  job_cluster {
    job_cluster_key = "ehrp2biis"
    new_cluster {
      spark_version      = var.ehrp2biis_spark_version
      node_type_id       = var.ehrp2biis_node_type_id
      num_workers        = 2
      data_security_mode = "SINGLE_USER"
      spark_conf = {
        "spark.sql.session.timeZone" = "America/New_York"
      }
      custom_tags = {
        project = "ehrp2biis-pilot"
      }
    }
  }

  task {
    task_key        = "extract"
    job_cluster_key = "ehrp2biis"
    spark_python_task {
      python_file = local.ehrp2biis_entry
      source      = "GIT"
      parameters  = concat(["extract"], local.ehrp2biis_common_args)
    }
    library {
      maven {
        coordinates = "com.oracle.database.jdbc:ojdbc11:23.4.0.24.05"
      }
    }
  }

  task {
    task_key        = "transform"
    job_cluster_key = "ehrp2biis"
    depends_on {
      task_key = "extract"
    }
    spark_python_task {
      python_file = local.ehrp2biis_entry
      source      = "GIT"
      parameters  = concat(["transform"], local.ehrp2biis_common_args)
    }
  }

  task {
    task_key        = "publish"
    job_cluster_key = "ehrp2biis"
    depends_on {
      task_key = "transform"
    }
    spark_python_task {
      python_file = local.ehrp2biis_entry
      source      = "GIT"
      parameters  = concat(["publish"], local.ehrp2biis_common_args)
    }
    library {
      maven {
        coordinates = "com.oracle.database.jdbc:ojdbc11:23.4.0.24.05"
      }
    }
  }

  email_notifications {
    on_failure = var.ehrp2biis_failure_emails
  }
}

resource "databricks_job" "ehrp2biis_reconcile" {
  name = "ehrp2biis-reconcile-dual-run"

  git_source {
    url      = var.ehrp2biis_git_url
    provider = "gitHub"
    branch   = var.ehrp2biis_git_branch
  }

  parameter {
    name    = "run_date"
    default = ""
  }

  job_cluster {
    job_cluster_key = "reconcile"
    new_cluster {
      spark_version      = var.ehrp2biis_spark_version
      node_type_id       = var.ehrp2biis_node_type_id
      num_workers        = 1
      data_security_mode = "SINGLE_USER"
    }
  }

  task {
    task_key        = "reconcile"
    job_cluster_key = "reconcile"
    spark_python_task {
      python_file = "pipelines/run_reconcile.py"
      source      = "GIT"
      parameters = [
        "--run-date", "{{job.parameters.run_date}}",
        "--legacy", "oracle",
        "--new", "${var.ehrp2biis_catalog}.${databricks_schema.ehrp2biis_silver.name}",
        "--secret-scope", databricks_secret_scope.ehrp2biis.name,
        "--report-dir", "/Volumes/${var.ehrp2biis_catalog}/${databricks_schema.ehrp2biis_silver.name}/reports",
      ]
    }
    library {
      maven {
        coordinates = "com.oracle.database.jdbc:ojdbc11:23.4.0.24.05"
      }
    }
  }
}

output "ehrp2biis_job_id" {
  value = databricks_job.ehrp2biis_update.id
}
