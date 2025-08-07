resource "aws_secretsmanager_secret" "secrets" {
  for_each = var.secrets

  name        = "${var.environment}/${each.key}"
  description = each.value.description

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_secretsmanager_secret_version" "secrets" {
  for_each = var.secrets

  secret_id     = aws_secretsmanager_secret.secrets[each.key].id
  secret_string = each.value.secret_string
}