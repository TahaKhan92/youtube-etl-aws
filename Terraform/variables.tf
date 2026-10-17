variable "aws_region" {
  description = "AWS region for every resource"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefix for resource names (lowercase, hyphens only)"
  type        = string
  default     = "yt-etl"
}

# AWS-managed public layer (pandas + pyarrow + awswrangler).
# Easiest way to get it: Lambda console -> any function -> Add layer -> AWS layers
# -> AWSSDKPandas-Python312 -> copy the ARN with the latest version number.
# Format: arn:aws:lambda:<region>:336392948345:layer:AWSSDKPandas-Python312:<version>
variable "pandas_layer_arn" {
  description = "ARN of the AWSSDKPandas-Python312 layer for your region"
  type        = string
}
