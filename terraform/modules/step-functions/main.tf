resource "aws_sfn_state_machine" "sfn_war" {
  name = "mro-${var.environment}-esp-analytics-war-sfn"
  role_arn = var.sfn_role_arn
  tags = var.sfn_tags
  definition = file("${path.module}/sfn.json")
}
