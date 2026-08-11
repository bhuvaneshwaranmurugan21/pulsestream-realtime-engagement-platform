resource "aws_dynamodb_table" "generation" {
  name         = "${local.name}-generation"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "generation_id"

  attribute {
    name = "generation_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.data.arn
  }
}

resource "aws_dynamodb_table_item" "active_pointer" {
  table_name = aws_dynamodb_table.generation.name
  hash_key   = aws_dynamodb_table.generation.hash_key
  item = jsonencode({
    generation_id   = { S = "ACTIVE#engagement" }
    pointer_version = { N = "0" }
  })
  lifecycle {
    ignore_changes = [item]
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "publication_lambda" {
  name_prefix        = "${local.name}-publication-"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "publication_lambda" {
  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:TransactWriteItems"]
    resources = [aws_dynamodb_table.generation.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/manifests/*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.aws_region}:*:*"]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
}

resource "aws_iam_role_policy" "publication_lambda" {
  role   = aws_iam_role.publication_lambda.id
  policy = data.aws_iam_policy_document.publication_lambda.json
}

resource "aws_lambda_function" "register" {
  function_name    = "${local.name}-register-generation"
  role             = aws_iam_role.publication_lambda.arn
  runtime          = "python3.12"
  handler          = "lambdas.register_generation.handler.handler"
  filename         = abspath("${path.module}/${var.lambda_package_path}")
  source_code_hash = filebase64sha256(abspath("${path.module}/${var.lambda_package_path}"))
  timeout          = 30
  memory_size      = 256
  environment {
    variables = {
      GENERATION_TABLE = aws_dynamodb_table.generation.name
      MANIFEST_BUCKET  = aws_s3_bucket.lake.id
      REQUIRED_TABLES  = "curated_event,session_version,engagement_aggregate,correction_exception"
    }
  }
}

resource "aws_lambda_function" "publish" {
  function_name    = "${local.name}-publish-generation"
  role             = aws_iam_role.publication_lambda.arn
  runtime          = "python3.12"
  handler          = "lambdas.publish_generation.handler.handler"
  filename         = abspath("${path.module}/${var.lambda_package_path}")
  source_code_hash = filebase64sha256(abspath("${path.module}/${var.lambda_package_path}"))
  timeout          = 30
  memory_size      = 256
  environment {
    variables = { GENERATION_TABLE = aws_dynamodb_table.generation.name }
  }
}
