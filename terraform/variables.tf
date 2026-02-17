# ============================================================
# Input Variables
# ============================================================

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev/staging/prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project identifier used in resource naming"
  type        = string
  default     = "c360"
}

variable "kinesis_shard_count" {
  description = "Number of shards for Kinesis Data Stream"
  type        = number
  default     = 2
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_num_nodes" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 2
}

variable "redshift_master_username" {
  description = "Redshift master username"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_master_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
}

variable "vpc_id" {
  description = "VPC ID for Redshift cluster"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for Redshift"
  type        = list(string)
}

variable "alert_email" {
  description = "Email address for pipeline alerts"
  type        = string
}

variable "data_retention_days" {
  description = "Days to retain data in curated layer"
  type        = number
  default     = 365
}
