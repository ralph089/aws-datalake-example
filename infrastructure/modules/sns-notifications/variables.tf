variable "environment" {
  description = "Environment name"
  type        = string
}

variable "topics" {
  description = "Map of SNS topics to create"
  type = map(object({
    display_name = string
    subscriptions = list(object({
      protocol = string
      endpoint = string
    }))
  }))
}