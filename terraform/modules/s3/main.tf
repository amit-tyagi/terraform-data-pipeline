resource "aws_s3_bucket" "glue_job_code_bucket" {
  bucket = var.bucket_name
  acl = "private"
  region = var.bucket_region
  tags = var.bucket_tags
}

resource "aws_s3_bucket_object" "war_code" {
  bucket = aws_s3_bucket.glue_job_code_bucket.bucket
  key = "${var.bucket_prefix}/${var.war_script_file_name}"
  source = "${var.source_file_location}/${var.war_script_file_name}"
//  etag   = filemd5(aws_s3_bucket_object.war_code.source)
  etag   = filemd5("${var.source_file_location}/${var.war_script_file_name}")
}

resource "aws_s3_bucket_object" "par_code" {
  bucket = aws_s3_bucket.glue_job_code_bucket.bucket
  key = "${var.bucket_prefix}/${var.par_script_file_name}"
  source = "${var.source_file_location}/${var.par_script_file_name}"
  etag   = filemd5("${var.source_file_location}/${var.par_script_file_name}")
}

resource "aws_s3_bucket_object" "ear_code" {
  bucket = aws_s3_bucket.glue_job_code_bucket.bucket
  key = "${var.bucket_prefix}/${var.ear_script_file_name}"
  source = "${var.source_file_location}/${var.ear_script_file_name}"
  etag   = filemd5("${var.source_file_location}/${var.ear_script_file_name}")
}

//resource "aws_s3_bucket_object" "war_object" {
//  bucket = var.bucket_name
//  key = "${var.bucket_prefix}/${var.war_script_file_name}"
//  source = var.war_source_file_location
//}
//
//output "war_object_source" {
//  value = aws_s3_bucket_object.war_object.source
//}
