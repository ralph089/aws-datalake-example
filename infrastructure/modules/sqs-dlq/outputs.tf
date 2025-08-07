output "queue_urls" {
  description = "URLs of created SQS queues"
  value = {
    for k, v in aws_sqs_queue.dlq : k => v.id
  }
}

output "queue_arns" {
  description = "ARNs of created SQS queues"
  value = {
    for k, v in aws_sqs_queue.dlq : k => v.arn
  }
}