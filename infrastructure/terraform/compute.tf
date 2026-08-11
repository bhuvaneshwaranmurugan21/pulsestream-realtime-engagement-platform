data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr" {
  name_prefix        = "${local.name}-emr-"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "emr" {
  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid       = "GlueCatalog"
    actions   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"]
    resources = ["*"]
  }
  statement {
    sid       = "Encryption"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
  statement {
    sid       = "Metrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr" {
  role   = aws_iam_role.emr.id
  policy = data.aws_iam_policy_document.emr.json
}

resource "aws_emrserverless_application" "spark" {
  name          = "${local.name}-spark"
  release_label = "emr-7.6.0"
  type          = "SPARK"

  auto_start_configuration {
    enabled = true
  }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}
