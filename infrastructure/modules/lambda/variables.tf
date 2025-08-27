variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "handler" {
  description = "Lambda function handler"
  type        = string
  default     = "handler.lambda_handler"
}

variable "runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.13"
}

variable "timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 60
}

variable "memory_size" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 512
}

variable "deployment_s3_bucket" {
  description = "S3 bucket containing the lambda deployment package"
  type        = string
}

variable "deployment_s3_key" {
  description = "S3 key of the lambda deployment package"
  type        = string
}

variable "environment_variables" {
  description = "Environment variables for the Lambda function"
  type        = map(string)
  default     = {}
}

variable "vpc_config" {
  description = "VPC configuration for the Lambda function"
  type = object({
    subnet_ids         = list(string)
    security_group_ids = list(string)
  })
  default = null
}


variable "custom_policies" {
  description = "List of custom IAM policy statements for the Lambda function"
  type = list(object({
    Effect   = string
    Action   = list(string)
    Resource = list(string)
    Condition = optional(map(map(list(string))))
  }))
  default = []
}

variable "external_permissions" {
  description = "External service permissions to invoke the Lambda function"
  type = map(object({
    principal  = string
    source_arn = optional(string)
  }))
  default = {}
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "enable_monitoring" {
  description = "Enable CloudWatch alarms for monitoring"
  type        = bool
  default     = true
}

variable "error_threshold" {
  description = "Error count threshold for CloudWatch alarm"
  type        = number
  default     = 0
}

variable "alarm_sns_topic_arn" {
  description = "SNS topic ARN for alarm notifications"
  type        = string
  default     = null
}

variable "tags" {
  description = "Additional tags for the Lambda function"
  type        = map(string)
  default     = {}
}