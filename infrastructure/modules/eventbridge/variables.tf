variable "environment" {
  description = "Environment name"
  type        = string
}

variable "schedules" {
  description = "Map of scheduled events"
  type = map(object({
    schedule_expression = string
    target_arn         = string
  }))
  default = {}
}

variable "s3_event_rules" {
  description = "Map of S3 event rules"
  type = map(object({
    bucket_name = string
    prefix      = string
    suffix      = string
    target_arn  = string
  }))
  default = {}
}