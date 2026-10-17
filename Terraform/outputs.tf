output "raw_bucket" {
  value = aws_s3_bucket.lake["raw"].id
}

output "cleansed_bucket" {
  value = aws_s3_bucket.lake["cleansed"].id
}

output "analytics_bucket" {
  value = aws_s3_bucket.lake["analytics"].id
}

output "athena_workgroup" {
  value = aws_athena_workgroup.yt.name
}
