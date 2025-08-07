resource "aws_sns_topic" "topics" {
  for_each = var.topics

  name         = "${var.environment}-${each.key}"
  display_name = each.value.display_name

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_sns_topic_subscription" "subscriptions" {
  for_each = {
    for combo in flatten([
      for topic_key, topic in var.topics : [
        for idx, subscription in topic.subscriptions : {
          topic_key = topic_key
          sub_key   = "${topic_key}-${idx}"
          protocol  = subscription.protocol
          endpoint  = subscription.endpoint
        }
      ]
    ]) : combo.sub_key => combo
  }

  topic_arn = aws_sns_topic.topics[each.value.topic_key].arn
  protocol  = each.value.protocol
  endpoint  = each.value.endpoint
}