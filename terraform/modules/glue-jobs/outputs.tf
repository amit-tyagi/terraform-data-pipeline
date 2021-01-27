output "glue_job_war_arn" {
  value = aws_glue_job.war.id
}

output "glue_job_par_arn" {
  value = aws_glue_job.par.id
}

output "glue_job_ear_arn" {
  value = aws_glue_job.ear.id
}
