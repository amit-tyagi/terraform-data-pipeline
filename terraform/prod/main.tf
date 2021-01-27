module "iam" {
  source = "../modules/iam"
  environment = var.environment
}

module "dynamodb" {
  source = "../modules/dynamodb"
  environment = var.environment
}

module "s3_buckets" {
  source = "../modules/s3"
  bucket_name = var.bucket_name
  bucket_region = var.region
  bucket_prefix = var.bucket_prefix
  bucket_tags = var.default_tags
  source_file_location = "${abspath(path.module)}/../../glue-jobs"
  war_script_file_name = var.war_script_file_name
  par_script_file_name = var.par_script_file_name
  ear_script_file_name = var.ear_script_file_name
}

module "secret_manager" {
  source = "../modules/secrets-manager"
  secret_name = var.secret_name
  secret_string = var.secret_string
  secret_tags = var.default_tags
}

module "sns_topic" {
  source = "../modules/sns"
  topic_name = var.topic_name
  sns_tags = var.default_tags
}

module "glue_jobs" {
  source = "../modules/glue-jobs"
  war_script_location = "s3://${module.s3_buckets.bucket_name}/${var.bucket_prefix}/${module.s3_buckets.war_script_file_name}"
  par_script_location = "s3://${module.s3_buckets.bucket_name}/${var.bucket_prefix}/${module.s3_buckets.par_script_file_name}"
  ear_script_location = "s3://${module.s3_buckets.bucket_name}/${var.bucket_prefix}/${module.s3_buckets.ear_script_file_name}"
  temp_dir = "s3://${module.s3_buckets.bucket_name}/temp"
  job_lanuguage = var.job_lanuguage
  environment = var.environment
  role_arn = var.role_arn
  python_version = var.python_version
  default_tags = var.default_tags
}

module "step_functions" {
  source = "../modules/step-functions"
  environment = var.environment
  sfn_role_arn = "arn:aws:iam::783481907866:role/mro-prod-esp-analytics-sfn-service-role01"
  sfn_tags = var.default_tags
}
