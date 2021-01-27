resource "aws_secretsmanager_secret" "secret" {
  name = var.secret_name
  description = "Secrets Manager for the ESP Analytics Project"
  tags = var.secret_tags
}

resource "aws_secretsmanager_secret_version" "secret_values" {
  secret_id = aws_secretsmanager_secret.secret.id
  secret_string = jsonencode(var.secret_string)
}

output "sm_id" {
  value = aws_secretsmanager_secret.secret.id
}
