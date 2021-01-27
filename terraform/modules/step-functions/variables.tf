variable environment {
  description = "Name of the environment"
  type = string
}

variable "sfn_role_arn" {
  description = "ARN of the role to be used for the Step functions"
  type = string
}

variable "sfn_tags" {
  description = "Tags for the Step Functions"
  type = map(string)
}
