# Data Platform outputs (from external module)
output "s3_data_buckets" {
  description = "Data lake S3 bucket names by layer"
  value       = module.data_platform.s3_buckets
}

output "glue_job_arns" {
  description = "Glue job ARNs"
  value = {
    simple_etl  = aws_glue_job.simple_etl.arn
    api_to_lake = aws_glue_job.api_to_lake.arn
  }
}

output "glue_job_names" {
  description = "Glue job names for triggers and monitoring"
  value = {
    simple_etl  = aws_glue_job.simple_etl.name
    api_to_lake = aws_glue_job.api_to_lake.name
  }
}

output "glue_deployment_info" {
  description = "Glue deployment configuration"
  value = {
    version         = var.glue_jobs_version
    scripts_path    = "s3://${local.glue_scripts_bucket_name}/scripts/${var.glue_jobs_version}/"
    dependencies_path = "s3://${local.glue_scripts_bucket_name}/dependencies/${var.glue_jobs_version}/"
  }
}

# Glue Scripts bucket
output "glue_scripts_bucket" {
  description = "S3 bucket name for Glue job scripts"
  value       = local.glue_scripts_bucket_name
}

# Lambda function outputs
output "lambda_functions" {
  description = "Lambda function ARNs"
  value = {
    data_lake_to_api  = module.lambda_data_lake_to_api.function_arn
    sns_glue_trigger  = module.lambda_sns_glue_trigger.function_arn
  }
}

# SNS topic outputs
output "notification_topics" {
  description = "SNS topic ARNs"
  value       = module.notifications.topic_arns
}

# Secrets Manager outputs
output "secret_arns" {
  description = "Secrets Manager secret ARNs"
  value       = module.secrets.secret_arns
}