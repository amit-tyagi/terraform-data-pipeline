variable "bucket_name" {
  description = "The Bucket name to be used to store the Glue Jobs code"
  type = string
}

variable "bucket_region" {
  description = "The region of the bucket"
  type = string
}

variable "bucket_prefix" {
  description = "The Bucket prefix for the glue code"
  type = string
}

variable "bucket_tags" {
  description = "The tags for the bucket"
  type = map(string)
}

variable "source_file_location" {
  type = string
}

variable "war_script_file_name" {
  description = "The WAR script file name"
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
