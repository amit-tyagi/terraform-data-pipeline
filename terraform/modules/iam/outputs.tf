output "sfn_policy_id" {
  value = aws_iam_policy.sfn_policy.id
}

output "sfn_role_id" {
  value = aws_iam_role.sfn_role.id
}