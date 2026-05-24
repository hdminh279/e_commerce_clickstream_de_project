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

# Amazon S3 Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-data-lake"
  force_destroy = true
}

# Enable versioning to protect against accidental overwrites/delections adn allow state recovery
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Configure Lifecycle Rules to optimize storage costs by automatically transitioning old data to cheaper storage classes or deleting it.
resource "aws_s3_bucket_lifecycle_configuration" "data_lake_lifecycle" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    id = "archive-and-delete-old-data"
    status = "Enabled"

    filter {}

    transition {
      days = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days = 90
      storage_class = "GLACIER"
    }
    expiration {
      days = 750
    }
  }
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
