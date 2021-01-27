resource "aws_glue_job" "war" {
  name = "mro-${var.environment}-esp-analytics-war-job"
  description = "The WAR Job"
  role_arn = var.role_arn
  command {
    script_location = var.war_script_location
    python_version = var.python_version
  }
  glue_version = "1.0"
  worker_type = "G.1X"
  number_of_workers = 5
  default_arguments = {
    "--TempDir" = var.temp_dir
    "--job-language" = var.job_lanuguage
    "--job-bookmark-option" = "job-bookmark-disable"
  }
  tags = var.default_tags
}

resource "aws_glue_job" "ear" {
  name = "mro-${var.environment}-esp-analytics-ear-job"
  description = "The EAR Job"
  role_arn = var.role_arn
  command {
    script_location = var.ear_script_location
    python_version = var.python_version
  }
  glue_version = "1.0"
  worker_type = "G.1X"
  number_of_workers = 5
  default_arguments = {
    "--TempDir" = var.temp_dir
    "--job-language" = var.job_lanuguage
    "--job-bookmark-option" = "job-bookmark-disable"
  }
  tags = var.default_tags
}

resource "aws_glue_job" "par" {
  name = "mro-${var.environment}-esp-analytics-par-job"
  description = "The PAR Job"
  role_arn = var.role_arn
  command {
    script_location = var.par_script_location
    python_version = var.python_version
  }
  glue_version = "1.0"
  worker_type = "G.1X"
  number_of_workers = 5
  default_arguments = {
    "--TempDir" = var.temp_dir
    "--job-language" = var.job_lanuguage
    "--job-bookmark-option" = "job-bookmark-disable"
  }
  tags = var.default_tags
}
