variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Location for GCS bucket and BigQuery dataset"
  type        = string
  default     = "US"
}

variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "bq_dataset_raw" {
  description = "BigQuery bronze dataset"
  type        = string
}

variable "bq_dataset_silver" {
  description = "BigQuery silver dataset"
  type        = string
}

variable "bq_dataset_gold" {
  description = "BigQuery gold dataset"
  type        = string
}
