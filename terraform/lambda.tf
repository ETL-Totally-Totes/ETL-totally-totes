data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract.py"
  output_path      = "${path.module}/../extract_function.zip"
}

data "archive_file" "layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer/"
  output_path      = "${path.module}/../layer.zip"
}

data "archive_file" "utils" {
  type             = "zip"
  output_file_mode = "0666"
  source_file       = "${path.module}/../src/utils/connection.py"
  output_path      = "${path.module}/../utils/python/src/utils.zip"
}


resource "aws_lambda_layer_version" "etl_layer" {
  layer_name          = "etl_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "layer.zip"
}

resource "aws_lambda_layer_version" "utils" {
  layer_name          = "utils"
  compatible_runtimes = [var.python_runtime]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "utils.zip"
}

resource "aws_lambda_function" "extract_handler" {
  function_name = var.extract_lambda_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "extract.saffi"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "extract_function.zip"

  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  timeout = 10

  runtime = "python3.13"
  layers = [aws_lambda_layer_version.etl_layer.arn, aws_lambda_layer_version.utils.arn]
  environment {
    variables = {
      S3_BUCKET_NAME = var.code_bucket_name
    }
  }
}