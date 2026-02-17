# ============================================================
# S3 Module — Data Lake Buckets
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "kms_key_arn" { type = string }

locals {
  buckets = {
    raw = {
      name       = "${var.project}-raw-${var.environment}"
      lifecycle  = { ia_days = 30, glacier_days = 60, expire_days = 90 }
    }
    clean = {
      name       = "${var.project}-clean-${var.environment}"
      lifecycle  = { ia_days = 90, glacier_days = null, expire_days = 180 }
    }
    curated = {
      name       = "${var.project}-curated-${var.environment}"
      lifecycle  = { ia_days = 180, glacier_days = null, expire_days = 365 }
    }
    quality_logs = {
      name       = "${var.project}-quality-logs-${var.environment}"
      lifecycle  = { ia_days = null, glacier_days = null, expire_days = 30 }
    }
    glue_scripts = {
      name       = "${var.project}-glue-scripts-${var.environment}"
      lifecycle  = { ia_days = null, glacier_days = null, expire_days = null }
    }
  }
}

resource "aws_s3_bucket" "data_lake" {
  for_each = local.buckets
  bucket   = each.value.name

  tags = {
    Layer = each.key
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  for_each = aws_s3_bucket.data_lake
  bucket   = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  for_each = aws_s3_bucket.data_lake
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  for_each = aws_s3_bucket.data_lake
  bucket   = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  for_each = {
    for k, v in local.buckets : k => v if v.lifecycle.expire_days != null
  }
  bucket = aws_s3_bucket.data_lake[each.key].id

  rule {
    id     = "${each.key}-lifecycle"
    status = "Enabled"

    dynamic "transition" {
      for_each = each.value.lifecycle.ia_days != null ? [1] : []
      content {
        days          = each.value.lifecycle.ia_days
        storage_class = "STANDARD_IA"
      }
    }

    dynamic "transition" {
      for_each = each.value.lifecycle.glacier_days != null ? [1] : []
      content {
        days          = each.value.lifecycle.glacier_days
        storage_class = "GLACIER"
      }
    }

    expiration {
      days = each.value.lifecycle.expire_days
    }
  }
}

# Outputs
output "raw_bucket_name"          { value = aws_s3_bucket.data_lake["raw"].id }
output "raw_bucket_arn"           { value = aws_s3_bucket.data_lake["raw"].arn }
output "clean_bucket_name"        { value = aws_s3_bucket.data_lake["clean"].id }
output "clean_bucket_arn"         { value = aws_s3_bucket.data_lake["clean"].arn }
output "curated_bucket_name"      { value = aws_s3_bucket.data_lake["curated"].id }
output "curated_bucket_arn"       { value = aws_s3_bucket.data_lake["curated"].arn }
output "quality_logs_bucket_name" { value = aws_s3_bucket.data_lake["quality_logs"].id }
output "glue_scripts_bucket_name" { value = aws_s3_bucket.data_lake["glue_scripts"].id }
