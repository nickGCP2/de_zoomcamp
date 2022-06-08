locals {
    data_bucket = "ny_taxi_data_lake"
}

variable "project" {
    description = "Your GCP Project ID"
    default = "ny-trips-de"
}

variable "region" {
    description = "Zone in which the project and resources will reside"
    default = "us-central1"
}

variable "storage_class" {
    description = "Storage Class for your Bucket"
    default = "STANDARD"
}

variable "bq_dataset" {
    description = "Big Query Dataset"
    default = "ny_trips_all"
}

variable "table_name" {
    description = "Big Query Table"
    default = "ny_trips_data"
}

