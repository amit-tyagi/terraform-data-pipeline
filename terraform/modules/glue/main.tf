resource "aws_glue_job" "glue-job" {
  name = var.job_name
  description = var.job_description
  role_arn = var.job_role_arn
  glue_version = var.job_glue_version
  worker_type = var.job_worker_type
  number_of_workers = var.job_number_of_workers
  timeout = var.job_timeout
  command {
    script_location = var.job_script_location
    python_version = var.job_python_version
  }
  default_arguments = {
    "--TempDir" = var.job_temp_dir
    "--job-language" = var.job_lanuguage
    "--job-bookmark-option" = var.job_bookmark
  }
  tags = var.job_tags
}