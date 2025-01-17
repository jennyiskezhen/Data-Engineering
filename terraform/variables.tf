variable "project" {
  description = "Project"
  default     = "ny-taxi-jenny"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "ny_taxi_dataset"
}

variable "gcs_bucket_name" {
  description = "my storage Bucket Name"
  default     = "ny-taxi-jenny-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}