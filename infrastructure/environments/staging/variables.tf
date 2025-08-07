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