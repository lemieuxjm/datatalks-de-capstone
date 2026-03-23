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
  default     = "de-camp-01-terra-bucket"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "de_hw_01_dataset"
}
