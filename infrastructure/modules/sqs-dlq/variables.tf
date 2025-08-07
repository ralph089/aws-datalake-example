variable "environment" {
  description = "Environment name"
  type        = string
}

variable "queues" {
  description = "Map of SQS queues to create"
  type = map(object({
    message_retention_seconds  = number
    visibility_timeout_seconds = number
  }))
}