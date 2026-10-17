#####################################
# Naming
#####################################
resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  sfx = random_id.suffix.hex

  buckets = {
    raw       = "${var.project}-raw-${local.sfx}"       # Landing area
    cleansed  = "${var.project}-cleansed-${local.sfx}"  # Cleansed / enriched
    analytics = "${var.project}-analytics-${local.sfx}" # Analytics / reporting
    athena    = "${var.project}-athena-${local.sfx}"    # Athena query results
    scripts   = "${var.project}-scripts-${local.sfx}"   # Glue job scripts
  }

  raw_stats_prefix = "youtube/raw_statistics/"
  raw_ref_prefix   = "youtube/raw_statistics_reference_data/"
}

#####################################
# S3 Data Lake (3 zones + helpers)
#####################################
resource "aws_s3_bucket" "lake" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = true # lab only: lets terraform destroy empty them
}

resource "aws_s3_bucket_public_access_block" "lake" {
  for_each                = aws_s3_bucket.lake
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

#####################################
# IAM - Glue role
#####################################
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue-role-${local.sfx}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "lake_rw" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for b in aws_s3_bucket.lake : "${b.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.lake : b.arn]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "lake-rw"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.lake_rw.json
}

#####################################
# IAM - Lambda role
#####################################
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project}-lambda-role-${local.sfx}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  name   = "lake-rw"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lake_rw.json
}

# Lambda writes its Parquet output straight into the Glue Data Catalog
resource "aws_iam_role_policy" "lambda_glue" {
  name = "glue-catalog"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable",
        "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition",
        "glue:BatchCreatePartition", "glue:UpdatePartition", "glue:DeletePartition",
        "glue:BatchDeletePartition"
      ]
      Resource = "*"
    }]
  })
}

#####################################
# Glue Data Catalog
#####################################
resource "aws_glue_catalog_database" "raw" {
  name = "yt_raw"
}

resource "aws_glue_catalog_database" "cleansed" {
  name = "yt_cleansed"
}

resource "aws_glue_catalog_database" "analytics" {
  name = "yt_analytics"
}

#####################################
# Lambda ETL: raw CSV + JSON -> cleansed Parquet (event-driven)
# (Glue Jobs/Crawlers are blocked on this account, so Lambda does the ETL)
#####################################
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../Code/lambda_json_to_parquet.py"
  output_path = "${path.module}/build/lambda_json_to_parquet.zip"
}

resource "aws_lambda_function" "json_to_parquet" {
  function_name    = "${var.project}-json-to-parquet"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_json_to_parquet.lambda_handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 600
  memory_size      = 3008
  layers           = [var.pandas_layer_arn]

  ephemeral_storage {
    size = 2048
  }

  environment {
    variables = {
      S3_CLEANSED_ROOT = "s3://${aws_s3_bucket.lake["cleansed"].id}/youtube/"
      GLUE_DB_CLEANSED = aws_glue_catalog_database.cleansed.name
    }
  }
}

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.json_to_parquet.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lake["raw"].arn
}

resource "aws_s3_bucket_notification" "raw_json" {
  bucket = aws_s3_bucket.lake["raw"].id

  lambda_function {
    lambda_function_arn = aws_lambda_function.json_to_parquet.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = local.raw_ref_prefix
    filter_suffix       = ".json"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.json_to_parquet.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = local.raw_stats_prefix
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

#####################################
# Athena
#####################################
resource "aws_athena_workgroup" "yt" {
  name          = "${var.project}-wg"
  force_destroy = true

  configuration {
    # false = CTAS can write to the analytics bucket via external_location
    enforce_workgroup_configuration = false

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake["athena"].id}/results/"
    }
  }
}
