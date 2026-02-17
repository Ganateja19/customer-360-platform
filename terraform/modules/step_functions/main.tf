# ============================================================
# Step Functions Module — Pipeline Orchestrator
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "glue_job_names" { type = map(string) }
variable "lambda_arns" { type = map(string) }
variable "sns_topic_arn" { type = string }

resource "aws_iam_role" "sfn_role" {
  name = "${var.project}-step-functions-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "sfn_policy" {
  name = "sfn-orchestration-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = [for arn in values(var.lambda_arns) : arn]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = var.sns_topic_arn
      }
    ]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline-orchestrator-${var.environment}"
  role_arn = aws_iam_role.sfn_role.arn

  definition = file("${path.module}/../../../../orchestration/step_functions/pipeline_state_machine.json")

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/stepfunctions/${var.project}-pipeline-${var.environment}"
  retention_in_days = 30
}

# EventBridge Schedule
resource "aws_cloudwatch_event_rule" "pipeline_schedule" {
  name                = "${var.project}-pipeline-schedule-${var.environment}"
  schedule_expression = "rate(1 hour)"
  state               = var.environment == "prod" ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "pipeline_target" {
  rule     = aws_cloudwatch_event_rule.pipeline_schedule.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.sfn_role.arn

  input = jsonencode({
    processDate  = "auto"
    environment  = var.environment
    triggeredBy  = "eventbridge-schedule"
  })
}

output "state_machine_arn" { value = aws_sfn_state_machine.pipeline.arn }
