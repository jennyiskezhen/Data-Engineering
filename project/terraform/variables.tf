variable "project" {
  description = "Project"
  default     = "ny-taxi-jenny"
}

variable "region" {
  description = "Region"
  default     = "europe-west10"
}

variable "location" {
  description = "Project Location"
  default     = "europe-west3"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "manufacture"
}

variable "gcs_bucket_name" {
  description = "my storage Bucket Name"
  default     = "ny-taxi-jenny-manufacturing"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}