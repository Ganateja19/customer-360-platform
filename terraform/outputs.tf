# ============================================================
# Output Values
# ============================================================

output "s3_raw_bucket" {
  description = "S3 Raw layer bucket name"
  value       = module.s3.raw_bucket_name
}

output "s3_clean_bucket" {
  description = "S3 Clean layer bucket name"
  value       = module.s3.clean_bucket_name
}

output "s3_curated_bucket" {
  description = "S3 Curated layer bucket name"
  value       = module.s3.curated_bucket_name
}

output "kinesis_stream_arn" {
  description = "Kinesis Data Stream ARN"
  value       = module.kinesis.stream_arn
}

output "redshift_endpoint" {
  description = "Redshift cluster endpoint"
  value       = module.redshift.cluster_endpoint
}

output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = module.step_functions.state_machine_arn
}

output "sns_topic_arn" {
  description = "SNS alerts topic ARN"
  value       = module.monitoring.sns_topic_arn
}

output "kms_key_arn" {
  description = "KMS encryption key ARN"
  value       = module.kms.key_arn
}

output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=C360-Pipeline-${var.environment}"
}
