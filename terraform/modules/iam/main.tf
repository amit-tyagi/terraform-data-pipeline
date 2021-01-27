resource "aws_iam_policy" "sfn_policy" {
  name = "mro-${var.environment}-esp-analytics-sfn-policy01"
  path = "/"
  policy = data.aws_iam_policy_document.sfn_policy_document.json
}

resource "aws_iam_role" "sfn_role" {
  name = "mro-${var.environment}-esp-analytics-sfn-service-role01"
  path = "/"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_policy_document.json
}

resource "aws_iam_role_policy_attachment" "sfn_role_policy_attachment" {
  role = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_policy.arn
}