// AWS settings
profile = "default"
region = "us-east-1"

// AWS environment
environment = "prod"

// IAM module variables

// Secret Manager module variables
secret_name = "mro-prod-esp-analytics-secrets"
secret_string = {
  hana-raw-bucket-name = "mro-dl-prod-hanap-raw-bucket01"
  procount-raw-bucket-name = "mro-dl-prod-procntp-raw-bucket01"
  tdm-raw-bucket-name = "mro-dl-prod-tdmp-raw-bucket01"
  wellview-raw-bucket-name = "mro-dl-prod-wellv10p-raw-bucket01"
  output-bucket-name = "mro-prod-esp-analytics-bucket"
}

// SNS module variables
topic_name = "mro-prod-esp-analytics-notification-topic"

// S3 Bucket module variables
bucket_name = "mro-prod-glue-scripts"
bucket_prefix = "esp-analytics/glue-scripts"
war_script_file_name = "MRO_ANALYTICS_WAR.py"
par_script_file_name = "MRO_ANALYTICS_PAR.py"
ear_script_file_name = "MRO_ANALYTICS_EAR.py"

// Glue Jobs module variables
job_lanuguage = "python"
python_version = "3"
role_arn = "arn:aws:iam::783481907866:role/mro-prod-esp-analytics-glue-service-role01"

// Step functions module variables
sfn_role_arn = "arn:aws:iam::783481907866:role/mro-prod-esp-analytics-sfn-service-role01"

// Tags shared across all the modules
default_tags = {
  Terraform = "true"
  CostCenter = "19831000"
  createdBy = "atyagi@marathonoil.com"
  Environment = "Production"
  Application = "ESP Analytics"
}

