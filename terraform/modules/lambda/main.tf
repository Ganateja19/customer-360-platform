# ============================================================
# Lambda Module — Kinesis Consumer
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "kinesis_stream_arn" { type = string }
variable "raw_bucket_name" { type = string }
variable "raw_bucket_arn" { type = string }
variable "kms_key_arn" { type = string }

# IAM Role
resource "aws_iam_role" "lambda_role" {
  name = "${var.project}-lambda-consumer-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project}-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListShards"]
        Resource = var.kinesis_stream_arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${var.raw_bucket_arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["kms:GenerateDataKey", "kms:Encrypt"]
        Resource = var.kms_key_arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "consumer" {
  filename         = "${path.module}/consumer_lambda.zip"
  function_name    = "${var.project}-kinesis-consumer-${var.environment}"
  role             = aws_iam_role.lambda_role.arn
  handler          = "consumer_lambda.lambda_handler"
  runtime          = "python3.12"
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      RAW_BUCKET = var.raw_bucket_name
      RAW_PREFIX = "raw/"
    }
  }
}

# Kinesis Event Source Mapping
resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn  = var.kinesis_stream_arn
  function_name     = aws_lambda_function.consumer.arn
  starting_position = "LATEST"
  batch_size        = 100

  maximum_batching_window_in_seconds = 30
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }
}

# Dead Letter Queue
resource "aws_sqs_queue" "dlq" {
  name = "${var.project}-kinesis-consumer-dlq-${var.environment}"
  kms_master_key_id = var.kms_key_arn
}

output "function_name" { value = aws_lambda_function.consumer.function_name }
output "function_arns" {
  value = {
    consumer = aws_lambda_function.consumer.arn
  }
}
