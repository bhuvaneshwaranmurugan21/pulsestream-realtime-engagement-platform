locals {
  name          = "${var.project_name}-${var.environment}"
  alarm_actions = var.alarm_topic_arn == "" ? [] : [var.alarm_topic_arn]
}
