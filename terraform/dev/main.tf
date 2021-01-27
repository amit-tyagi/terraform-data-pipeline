locals {
  tags = {
    CostCenter: "19831000"
    createdBy: "atyagi@marathonoil.com"
    Environment: var.environment
    Application: "ESP Analytics"
    Terraform: "true"
  }
}

module "glue" {
  source = "../modules/glue"
  job_name = "mro-${var.environment}-${var.project_name}-war-job01"
  job_description = "The job for generating the Well Analytical Record (WAR)"
  job_worker_type = "G.1X"
  job_number_of_workers = 5
  job_role_arn = "arn:aws:iam::${var.aws_account}:role/MRO-Glue-Poc"
  job_script_location = "s3://mro-dev-esp-analytics-bucket/glue-jobs/mro-dev-esp-analytics-war-job.py"
  job_temp_dir = "s3://aws-glue-temporary-${var.aws_account}-${var.aws_region}/admin"
  job_python_version = "3"
  job_tags = local.tags
}