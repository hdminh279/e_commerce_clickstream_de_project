terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~>5.0"
    }
  }
  
  backend "s3" {
    bucket = "minh-terraform-state-bucket-2026"
    key = "prod/clickstream/terraform.tfstate"
    region = "ap-southeast-1"
  }
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

# Amzon S3 Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-data-lake"
  force_destroy = true
}

# Add bucket athena to save results from athena
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project_name}-athena-result"
  force_destroy = true
}

# Create Glue
resource "aws_glue_catalog_database" "data_warehouse" {
  name = "event_raw"
}
