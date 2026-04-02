resource "databricks_job" "ehrp2biis_update" {
  name = "EHRP2BIIS_UPDATE_Pipeline"

  task {
    task_key = "preload"

    notebook_task {
      notebook_path = "/pipelines/EHRP2BIIS_UPDATE/EHRP2BIIS_UPDATE_preload"
    }
  }

  task {
    task_key = "etl"

    depends_on {
      task_key = "preload"
    }

    notebook_task {
      notebook_path = "/pipelines/EHRP2BIIS_UPDATE/EHRP2BIIS_UPDATE_etl"
    }
  }

  task {
    task_key = "afterload"

    depends_on {
      task_key = "etl"
    }

    notebook_task {
      notebook_path = "/pipelines/EHRP2BIIS_UPDATE/EHRP2BIIS_UPDATE_afterload"
    }
  }
}

output "job_id" {
  value = databricks_job.ehrp2biis_update.id
}
