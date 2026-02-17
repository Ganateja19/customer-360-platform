# ============================================================
# Customer 360 Data Platform — Terraform Root Configuration
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "c360-terraform-state"
    key            = "customer-360/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "c360-terraform-locks"
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "customer-360"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ── Modules ──────────────────────────────────────────────────

module "s3" {
  source      = "./modules/s3"
  environment = var.environment
  project     = var.project_name
  kms_key_arn = module.kms.key_arn
}

module "kms" {
  source      = "./modules/kms"
  environment = var.environment
  project     = var.project_name
  account_id  = data.aws_caller_identity.current.account_id
}

module "kinesis" {
  source       = "./modules/kinesis"
  environment  = var.environment
  project      = var.project_name
  shard_count  = var.kinesis_shard_count
  kms_key_arn  = module.kms.key_arn
}

module "lambda" {
  source              = "./modules/lambda"
  environment         = var.environment
  project             = var.project_name
  kinesis_stream_arn  = module.kinesis.stream_arn
  raw_bucket_name     = module.s3.raw_bucket_name
  raw_bucket_arn      = module.s3.raw_bucket_arn
  kms_key_arn         = module.kms.key_arn
}

module "glue" {
  source             = "./modules/glue"
  environment        = var.environment
  project            = var.project_name
  raw_bucket_name    = module.s3.raw_bucket_name
  clean_bucket_name  = module.s3.clean_bucket_name
  curated_bucket_name = module.s3.curated_bucket_name
  kms_key_arn        = module.kms.key_arn
  glue_scripts_bucket = module.s3.glue_scripts_bucket_name
}

module "redshift" {
  source            = "./modules/redshift"
  environment       = var.environment
  project           = var.project_name
  node_type         = var.redshift_node_type
  num_nodes         = var.redshift_num_nodes
  master_username   = var.redshift_master_username
  master_password   = var.redshift_master_password
  vpc_id            = var.vpc_id
  subnet_ids        = var.private_subnet_ids
  kms_key_arn       = module.kms.key_arn
  curated_bucket_arn = module.s3.curated_bucket_arn
}

module "step_functions" {
  source            = "./modules/step_functions"
  environment       = var.environment
  project           = var.project_name
  glue_job_names    = module.glue.job_names
  lambda_arns       = module.lambda.function_arns
  sns_topic_arn     = module.monitoring.sns_topic_arn
}

module "monitoring" {
  source          = "./modules/monitoring"
  environment     = var.environment
  project         = var.project_name
  alert_email     = var.alert_email
  kinesis_stream  = module.kinesis.stream_name
  lambda_function = module.lambda.function_name
  redshift_cluster = module.redshift.cluster_identifier
  state_machine_arn = module.step_functions.state_machine_arn
}

# ── Data Sources ─────────────────────────────────────────────

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
