terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket
resource "google_storage_bucket" "de_camp_bucket" {
  name          = var.bucket_name
  location      = var.location
  force_destroy = false
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "de_hw_dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "Data Engineering Homework Dataset"
  description                 = "Dataset for data engineering homework assignments"
  location                    = var.location
  default_table_expiration_ms = null
  
  labels = {
    env = "dev"
  }
}
