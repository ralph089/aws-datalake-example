variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "aws-glue-etl"
}

variable "api_base_url" {
  description = "External API base URL"
  type        = string
  default     = "https://api.example.com"
}

variable "api_bearer_token" {
  description = "External API bearer token"
  type        = string
  sensitive   = true
}

variable "notification_email" {
  description = "Email address for notifications"
  type        = string
}

variable "external_sns_topic_arn" {
  description = "ARN of external SNS topic that publishes S3 events"
  type        = string
  # Example: "arn:aws:sns:eu-west-1:123456789012:s3-events-topic"
}

variable "lambda_artifacts_bucket" {
  description = "S3 bucket name for storing lambda deployment artifacts"
  type        = string
  # This should be provided externally as it's a pre-existing bucket
}

variable "glue_scripts_bucket" {
  description = "S3 bucket name for storing Glue job scripts and code artifacts"
  type        = string
  default     = null  # If null, will create bucket with auto-generated name
}

variable "glue_jobs_version" {
  description = "Version/tag of Glue jobs to deploy (e.g., 'latest', 'v1.0.0')"
  type        = string
  default     = "latest"
}

variable "glue_wheel_version" {
  description = "Version of the wheel file (should match semantic release version, e.g., '1.0.0')"
  type        = string
  default     = "1.0.0"
}