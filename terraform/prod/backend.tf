terraform {
  backend "s3" {
    bucket = "mro-prod-terraform-remote-backends"
    key = "esp-analytics/terraform.tfstate"
    region = "us-east-1"
    dynamodb_table = "mro-prod-terraform-lock"
  }
}
