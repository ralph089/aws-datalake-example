output "schedule_rule_arns" {
  description = "ARNs of schedule rules"
  value = {
    for k, v in aws_cloudwatch_event_rule.schedules : k => v.arn
  }
}

output "s3_event_rule_arns" {
  description = "ARNs of S3 event rules"
  value = {
    for k, v in aws_cloudwatch_event_rule.s3_events : k => v.arn
  }
}