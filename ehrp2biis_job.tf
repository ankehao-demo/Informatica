resource "databricks_job" "ehrp2biis_update" {
  name = "EHRP2BIIS_UPDATE"

  task {
    task_key = "pre_load"

    notebook_task {
      notebook_path = "/pipelines/EHRP2BIIS_UPDATE/EHRP2BIIS_UPDATE_preload"
    }
  }

  task {
    task_key = "etl"

    depends_on {
      task_key = "pre_load"
    }

    notebook_task {
      notebook_path = "/pipelines/EHRP2BIIS_UPDATE/EHRP2BIIS_UPDATE_etl"
    }
  }

  task {
    task_key = "post_load"

    depends_on {
      task_key = "etl"
    }

    notebook_task {
      notebook_path = "/pipelines/EHRP2BIIS_UPDATE/EHRP2BIIS_UPDATE_afterload"
    }
  }
}
