resource "aws_s3_bucket" "code_bucket" {
  bucket = var.code_bucket_name

  tags = {
    Name        = "Source code"
    Environment = "prod"
  }
}

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = var.ingestion_bucket_name

  tags = {
    Name        = "Data ingested"
    Environment = "prod"
  }
}

resource "aws_s3_object" "extract_function" {
  bucket = var.code_bucket_name
  key    = "extract_function.zip"
  source = "${path.module}/../extract_function.zip"
  etag   = filemd5(data.archive_file.extract_lambda.output_path)
}

resource "aws_s3_object" "layer" {
  bucket = var.code_bucket_name
  key    = "layer.zip"
  source = "${path.module}/../layer.zip"
  etag   = filemd5(data.archive_file.layer.output_path)
}