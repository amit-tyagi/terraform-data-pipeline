data "aws_iam_policy_document" "sfn_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
      "logs:*",
      "sns:Publish"
    ]

    resources = [
      "*"
    ]

  }
}