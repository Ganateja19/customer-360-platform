# ============================================================
# KMS Module — Encryption Key
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "account_id" { type = string }

resource "aws_kms_key" "data_key" {
  description             = "${var.project} data encryption key - ${var.environment}"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EnableRootAccountAccess"
        Effect = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.account_id}:root" }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "data_key" {
  name          = "alias/${var.project}-data-key-${var.environment}"
  target_key_id = aws_kms_key.data_key.key_id
}

output "key_arn"  { value = aws_kms_key.data_key.arn }
output "key_id"   { value = aws_kms_key.data_key.key_id }
output "alias"    { value = aws_kms_alias.data_key.name }
