variable "secret_name" {
  description = "Name of the secret"
  type = string
}

variable "secret_string" {
  description = "The secrets key value pair"
  type = map(string)
}

variable "secret_tags" {
  description = "Tags for the secrets manager"
  type = map(string)
}
