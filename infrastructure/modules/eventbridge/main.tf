resource "aws_cloudwatch_event_rule" "schedules" {
  for_each = var.schedules

  name                = "${var.environment}-${each.key}"
  description         = "Schedule for ${each.key}"
  schedule_expression = each.value.schedule_expression

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_event_target" "schedule_targets" {
  for_each = var.schedules

  rule      = aws_cloudwatch_event_rule.schedules[each.key].name
  target_id = "${each.key}-target"
  arn       = each.value.target_arn

  role_arn = aws_iam_role.eventbridge.arn
}

resource "aws_cloudwatch_event_rule" "s3_events" {
  for_each = var.s3_event_rules

  name        = "${var.environment}-${each.key}"
  description = "S3 event rule for ${each.key}"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [each.value.bucket_name]
      }
      object = {
        key = [
          {
            prefix = each.value.prefix
            suffix = each.value.suffix
          }
        ]
      }
    }
  })

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_event_target" "s3_event_targets" {
  for_each = var.s3_event_rules

  rule      = aws_cloudwatch_event_rule.s3_events[each.key].name
  target_id = "${each.key}-target"
  arn       = each.value.target_arn

  role_arn = aws_iam_role.eventbridge.arn

  # Transform S3 event details into Glue job arguments
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = jsonencode({
      "--bucket"     = "<bucket>"
      "--object_key" = "<key>"
      "--env"        = var.environment
    })
  }
}

resource "aws_iam_role" "eventbridge" {
  name = "${var.environment}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_role_policy" "eventbridge" {
  name = "${var.environment}-eventbridge-policy"
  role = aws_iam_role.eventbridge.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun"
        ]
        Resource = "*"
      }
    ]
  })
}