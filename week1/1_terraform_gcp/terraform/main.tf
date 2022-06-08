terraform {
    required_version = ">= 1.1"
    backend "local" {}
    required_providers {
        google = {
            source = "hashicorp/google"
        }
    }
}

# Adding Provider details
provider "google" {
    project = var.project
    region = var.region
}

# Adding Resource details

# Data Lake / Storage Bucket
resource "google_storage_bucket" "data_lake" {
    name = "${var.project}_${local.data_bucket}"
    location = var.region

    storage_class = var.storage_class
    uniform_bucket_level_access = true

    versioning {
        enabled = true
    }

    lifecycle_rule {
        action {
            type = "Delete"
        }
        condition {
            age = 30
        }
    }

    force_destroy = true
}

# Data Warehouse
resource "google_bigquery_dataset" "data_warehouse" {
    dataset_id = var.bq_dataset
    project = var.project
    location = var.region
}