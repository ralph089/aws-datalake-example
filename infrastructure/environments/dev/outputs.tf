# Data Platform outputs (from external module)
output "s3_data_buckets" {
  description = "Data lake S3 bucket names by layer"
  value       = module.data_platform.s3_buckets
}

output "glue_job_arns" {
  description = "Glue job ARNs"
  value       = module.data_platform.glue_job_arns
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