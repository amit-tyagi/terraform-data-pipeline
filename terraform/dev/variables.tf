variable "environment" {
  type = string
  description = "The Envrionemnt of the deployment"
  default = "dev"
}

variable "project_name" {
  type = string
  description = "The name of the project"
}

variable "aws_region" {
  type = string
  default = "us-east-1"
  description = "The AWS region of deployment"
}

variable "aws_account" {
  type = string
  description = "The AWS account number"
}