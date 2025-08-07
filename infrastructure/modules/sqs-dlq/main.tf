resource "aws_sqs_queue" "dlq" {
  for_each = var.queues

  name                      = "${var.environment}-${each.key}"
  message_retention_seconds = each.value.message_retention_seconds
  visibility_timeout_seconds = each.value.visibility_timeout_seconds

  tags = {
    Environment = var.environment
    Purpose     = "Dead Letter Queue"
    ManagedBy   = "Terraform"
  }
}

resource "aws_sqs_queue_policy" "dlq" {
  for_each = var.queues

  queue_url = aws_sqs_queue.dlq[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.dlq[each.key].arn
      }
    ]
  })
}