# ============================================================
# Monitoring Module — CloudWatch + SNS
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "alert_email" { type = string }
variable "kinesis_stream" { type = string }
variable "lambda_function" { type = string }
variable "redshift_cluster" { type = string }
variable "state_machine_arn" { type = string }

# SNS Topic
resource "aws_sns_topic" "alerts" {
  name = "${var.project}-pipeline-alerts-${var.environment}"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# CloudWatch Alarms
resource "aws_cloudwatch_metric_alarm" "sfn_failures" {
  alarm_name          = "${var.project}-${var.environment}-sfn-failures"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  dimensions = {
    StateMachineArn = var.state_machine_arn
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project}-${var.environment}-lambda-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    FunctionName = var.lambda_function
  }
}

resource "aws_cloudwatch_metric_alarm" "redshift_cpu" {
  alarm_name          = "${var.project}-${var.environment}-redshift-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/Redshift"
  period              = 600
  statistic           = "Average"
  threshold           = 80
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    ClusterIdentifier = var.redshift_cluster
  }
}

output "sns_topic_arn" { value = aws_sns_topic.alerts.arn }
