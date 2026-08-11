output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.events.arn
}

output "glue_database" {
  value = aws_glue_catalog_database.pulsestream.name
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  value = aws_iam_role.emr.arn
}

output "generation_table" {
  value = aws_dynamodb_table.generation.name
}

output "register_function" {
  value = aws_lambda_function.register.function_name
}

output "publish_function" {
  value = aws_lambda_function.publish.function_name
}
