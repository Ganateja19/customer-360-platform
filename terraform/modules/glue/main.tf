# ============================================================
# Glue Module — ETL Jobs & Catalog
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "raw_bucket_name" { type = string }
variable "clean_bucket_name" { type = string }
variable "curated_bucket_name" { type = string }
variable "kms_key_arn" { type = string }
variable "glue_scripts_bucket" { type = string }

# IAM Role
resource "aws_iam_role" "glue_role" {
  name = "${var.project}-glue-etl-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "databases" {
  for_each = toset(["raw", "clean", "curated"])
  name     = "${var.project}_${each.key}_${var.environment}"
}

# Glue Jobs
locals {
  glue_jobs = {
    raw_to_clean = {
      name   = "${var.project}-raw-to-clean"
      script = "s3://${var.glue_scripts_bucket}/scripts/raw_to_clean.py"
      args = {
        "--RAW_BUCKET"   = var.raw_bucket_name
        "--CLEAN_BUCKET" = var.clean_bucket_name
        "--DATABASE"     = aws_glue_catalog_database.databases["clean"].name
      }
    }
    clean_to_curated = {
      name   = "${var.project}-clean-to-curated"
      script = "s3://${var.glue_scripts_bucket}/scripts/clean_to_curated.py"
      args = {
        "--CLEAN_BUCKET"   = var.clean_bucket_name
        "--CURATED_BUCKET" = var.curated_bucket_name
        "--DATABASE"       = aws_glue_catalog_database.databases["curated"].name
      }
    }
    curated_to_redshift = {
      name   = "${var.project}-curated-to-redshift"
      script = "s3://${var.glue_scripts_bucket}/scripts/curated_to_redshift.py"
      args = {
        "--CURATED_BUCKET" = var.curated_bucket_name
      }
    }
  }
}

resource "aws_glue_job" "etl_jobs" {
  for_each = local.glue_jobs

  name     = each.value.name
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = each.value.script
    python_version  = "3"
  }

  default_arguments = merge(each.value.args, {
    "--job-language"             = "python"
    "--enable-metrics"           = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  })

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 60

  security_configuration = aws_glue_security_configuration.encrypted.name
}

resource "aws_glue_security_configuration" "encrypted" {
  name = "${var.project}-glue-security-${var.environment}"

  encryption_configuration {
    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = var.kms_key_arn
    }
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = var.kms_key_arn
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = var.kms_key_arn
    }
  }
}

output "job_names" {
  value = { for k, v in aws_glue_job.etl_jobs : k => v.name }
}
