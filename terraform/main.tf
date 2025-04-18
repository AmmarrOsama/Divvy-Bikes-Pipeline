terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.30.0"
    }
  }
}

provider "google" {
  credentials = file(var.sa_file)
  project     = var.project_id
  region      = var.region_name
}

resource "google_storage_bucket" "divvy_bikes_bucket" {
  name                     = var.bucket_name
  location                 = var.region_name
  storage_class            = "STANDARD"
  public_access_prevention = "enforced"
  force_destroy = true
}

resource "google_bigquery_dataset" "divvy_bikes_dataset" {
  dataset_id = var.dataset_name
  location   = var.region_name
}

resource "google_storage_bucket_object" "dag_file" {
  name   = "dags/divvy_bikes_pipeline.py"
  source = var.dag_path
  bucket = var.bucket_name
  depends_on = [google_storage_bucket.divvy_bikes_bucket, google_composer_environment.pipeline_composer]
}

resource "google_secret_manager_secret" "sa_key_secret" {
  secret_id = "service-account-key"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "sa_key_version" {
  secret      = google_secret_manager_secret.sa_key_secret.id
  secret_data = file(var.sa_file)
}

data "google_project" "secret_project" {
  project_id = google_secret_manager_secret.sa_key_secret.project
}

resource "google_project_service" "composer" {
  project = var.project_id
  service = "composer.googleapis.com"
}

resource "google_composer_environment" "pipeline_composer" {
  name   = var.composer_name
  region = var.region_name

  config {

    software_config {
      image_version = "composer-3-airflow-2"
      pypi_packages = {
        dbt-bigquery = ""
      }

      env_variables = {
        DAG_PROJECT_ID = var.project_id
        DAG_BUCKET_NAME = var.bucket_name
        DAG_DATASET_NAME = var.dataset_name
        DAG_PROJECT_NUMBER = data.google_project.secret_project.number
      }
    }

    node_config {
      service_account = var.sa_email
    }

  }

  storage_config {
    bucket = "gs://${var.bucket_name}"
  }

  depends_on = [ google_project_service.composer]
}