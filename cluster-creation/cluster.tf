terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.7.0"
    }
  }
}

provider "databricks" {
  host  = "https://dbc-ad3d47af-affb.cloud.databricks.com"
  token = "<your-databricks-token>"
}

resource "databricks_cluster" "terraform_test_cluster" {
  cluster_name            = "terraform-test-cluster"
  spark_version           = "15.1.x-scala2.12"
  node_type_id            = "r5d.large"
  autotermination_minutes = 120
  enable_local_disk_encryption = true

  autoscale {
    min_workers = 1
    max_workers = 4
  }

  custom_tags = {
    "Environment" = "Test"
    "Comment"     = "This is a test cluster"
  }
}
