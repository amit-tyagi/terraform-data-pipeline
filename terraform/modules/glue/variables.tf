variable "job_name" {
  type = string
  description = "Name of the Job"
}

variable "job_description" {
  type = string
  description = "The description of the job"
}

variable "job_role_arn" {
  type = string
  description = "The ARN of the IAM Role to be used for the Job"
}

variable "job_script_location" {
  type = string
  description = "The S3 location of the script file"
}

variable "job_python_version" {
  type = string
  description = "(Optional) The Python version being used to execute a Python shell job. Allowed values are 2 or 3."
  default = "2"
}

variable "job_temp_dir" {
  type = string
  description = "The S3 location of the script temporary file"
}

variable "job_glue_version" {
  type = string
  description = " (Optional) The version of glue to use, for example 1.0 and 2.0"
  default = "2.0"
}

variable "job_worker_type" {
  type = string
  description = "(Optional) The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X"
  default = "Standard"
}

variable "job_number_of_workers" {
  type = number
  description = " (Optional) The number of workers of a defined workerType that are allocated when a job runs"
  default = 5
}

variable "job_lanuguage" {
  type = string
  description = "The language of the job. Possible values: python, scala"
  default = "python"
}

variable "job_bookmark" {
  type = string
  description = "The job bookmark. Possible values: job-bookmark-disable, job-bookmark-enable"
  default = "job-bookmark-disable"
}

variable "job_timeout" {
  type = number
  description = "(Optional) The job timeout in minutes. The default is 2880 minutes (48 hours)"
  default = 2880
}

variable "job_tags" {
  type = map(string)
  description = "The Tags for the job"
}

