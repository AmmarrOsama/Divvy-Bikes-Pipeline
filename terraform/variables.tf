# Enter Your Project ID and Service Account Email in order to run the terraform code 
# Put your service account json creds in "./keys/gcp_cred.json" in order to run the terraform code
# Note That: you should give the service account the needed permissions or "Owner" permission for simpilicity

variable "sa_email" {
  description = "Service Account Email"
  type        = string
  default     = "<Place_Your_Service_Account_Email_Here>"
}

variable "sa_file" {
  description = "Path to the GCP Service Account File"
  type        = string
  default     = "./keys/gcp_cred.json"
}

variable "project_id" {
  description = "Project ID"
  type        = string
  default     = "<Place_Your_Project_ID_Here>"
}

variable "region_name" {
  description = "GCP Region Name"
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "GCS Bucket Name"
  type        = string
  default     = "divvy_bikes_bucket_dezoomcamp"

}

variable "dataset_name" {
  description = "BigQuery Dataset Name"
  type        = string
  default     = "divvy_bikes_dataset_dezoomcamp"

}

variable "composer_name" {
  description = "GCP Composer Cluster Name"
  type        = string
  default     = "divvy-bikes-airflow-3"

}

variable "dag_path" {
  description = "Path For Pipeline Dag"
  type        = string
  default     = "./assets/bikes_pipeline.py"

}