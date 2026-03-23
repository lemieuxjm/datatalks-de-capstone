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
resource "google_storage_bucket" "raw_data_bucket1" {
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

# BigQuery — Raw (Bronze) layer
resource "google_bigquery_dataset" "raw" {
  dataset_id  = var.bq_dataset_raw
  description = "Bronze layer — raw data loaded from GCS"
  location    = var.location

  labels = {
    env = "dev"
  }
}

# BigQuery — Silver layer
resource "google_bigquery_dataset" "silver" {
  dataset_id  = var.bq_dataset_silver
  description = "Silver layer — cleaned and conformed data"
  location    = var.location

  labels = {
    env = "dev"
  }
}

# BigQuery — Gold layer
resource "google_bigquery_dataset" "gold" {
  dataset_id  = var.bq_dataset_gold
  description = "Gold layer — aggregated, partitioned, analyst-ready"
  location    = var.location

  labels = {
    env = "dev"
  }
}