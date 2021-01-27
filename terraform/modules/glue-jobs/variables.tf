variable environment {
  description = "Name of the environment"
  type = string
}

variable "war_script_location" {
  description = "The WAR script file location"
  type = string
}

variable "par_script_location" {
  description = "The PAR script file location"
  type = string
}

variable "ear_script_location" {
  description = "The EAR script file location"
  type = string
}

variable "temp_dir" {
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

variable "job_lanuguage" {
  description = "The Job language (python or scala)"
  type = string
}

variable "default_tags" {
  type = map(string)
}
