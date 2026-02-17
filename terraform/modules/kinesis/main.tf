# ============================================================
# Kinesis Module — Data Stream
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "shard_count" { type = number }
variable "kms_key_arn" { type = string }

resource "aws_kinesis_stream" "clickstream" {
  name             = "${var.project}-clickstream-events"
  shard_count      = var.shard_count
  retention_period = 24

  encryption_type = "KMS"
  kms_key_id      = var.kms_key_arn

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Name = "${var.project}-clickstream-events"
  }
}

output "stream_arn"  { value = aws_kinesis_stream.clickstream.arn }
output "stream_name" { value = aws_kinesis_stream.clickstream.name }
