variable "environment" {
  description = "Environment name"
  type        = string
}

variable "secrets" {
  description = "Map of secrets to create"
  type = map(object({
    description   = string
    secret_string = string
  }))
}