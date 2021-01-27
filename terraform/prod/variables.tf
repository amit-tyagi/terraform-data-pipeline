variable environment {
  description = "Name of the environment"
  type = string
}

variable profile {
  description = "Profile that needs to be used for AWS credentials"
  type = string
}

variable region {
  description = "AWS Region"
  type = string
}

variable "python_version" {
  description = "The version of the Python to be use by Glue Jobs"
  type = string
}

variable "role_arn" {
  description = "The Role to be used by the Glue Jobs"
  type = string
}

variable "bucket_name" {
  description = "The Bucket name to be used to store the Glue Jobs code"
  type = string
}

variable "bucket_prefix" {
  description = "The Bucket prefix for the glue code"
  type = string
}

variable "job_lanuguage" {
  description = "The Job language (python or scala)"
  type = string
}

variable "war_script_file_name" {
  type = string
}

variable "par_script_file_name" {
  description = "The PAR script file name"
  type = string
}

variable "ear_script_file_name" {
  description = "The EAR script file name"
  type = string
}

variable "secret_name" {
  description = "The name of the Secret"
  type = string
}

variable "secret_string" {
  description = "The secrets key value pair"
  type = map(string)
}

variable "topic_name" {
  description = "Name of the SNS Topic"
  type = string
}

variable "sfn_role_arn" {
  description = "ARN of the role to be used for the Step functions"
  type = string
}

variable default_tags {
  type = map(string)
}
