variable "topic_name" {
  description = "Name of the SNS Topic"
  type = string
}

variable "sns_tags" {
  description = "Tags for the SNS Topic"
  type = map(string)
}
