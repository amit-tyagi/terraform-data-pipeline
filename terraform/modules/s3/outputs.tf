output "bucket_name" {
  value = aws_s3_bucket.glue_job_code_bucket.bucket
}

output "war_script_file_name" {
  value = var.war_script_file_name
}

output "par_script_file_name" {
  value = var.par_script_file_name
}

output "ear_script_file_name" {
  value = var.ear_script_file_name
}

output "bucket_arn" {
  value = aws_s3_bucket.glue_job_code_bucket.arn
}

output "bucket_id" {
  value = aws_s3_bucket.glue_job_code_bucket.id
}
