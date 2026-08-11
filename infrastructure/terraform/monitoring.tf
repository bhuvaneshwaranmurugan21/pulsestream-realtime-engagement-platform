resource "aws_cloudwatch_log_group" "register" {
  name              = "/aws/lambda/${aws_lambda_function.register.function_name}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.data.arn
}

resource "aws_cloudwatch_log_group" "publish" {
  name              = "/aws/lambda/${aws_lambda_function.publish.function_name}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.data.arn
}

resource "aws_cloudwatch_metric_alarm" "publication_errors" {
  alarm_name          = "${local.name}-publication-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  dimensions          = { FunctionName = aws_lambda_function.publish.function_name }
}

resource "aws_cloudwatch_metric_alarm" "emr_failed_jobs" {
  alarm_name          = "${local.name}-emr-failed-jobs"
  namespace           = "AWS/EMRServerless"
  metric_name         = "FailedJobs"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  dimensions          = { ApplicationId = aws_emrserverless_application.spark.id }
}

resource "aws_cloudwatch_dashboard" "platform" {
  dashboard_name = "${local.name}-platform"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6,
        properties = {
          title = "Publication controller", region = var.aws_region, stat = "Sum", period = 300,
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.publish.function_name],
            [".", "Errors", ".", "."], [".", "Throttles", ".", "."]
          ]
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6,
        properties = {
          title = "EMR Serverless generation jobs", region = var.aws_region, stat = "Sum", period = 300,
          metrics = [
            ["AWS/EMRServerless", "RunningWorkerCount", "ApplicationId", aws_emrserverless_application.spark.id],
            [".", "FailedJobs", ".", "."]
          ]
        }
      }
    ]
  })
}
