data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract.py"
  output_path      = "${path.module}/../extract_function.zip"
}

resource "aws_lambda_layer_version" "etl_layer" {
  layer_name          = "etl_layer"
  compatible_runtimes = [var.python_runtime, "python3.13"]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "layer.zip"
  depends_on = [ aws_s3_object.layer]#, null_resource.set_up_zips ]
}

resource "aws_lambda_layer_version" "extract_extras_layer" {
  layer_name          = "extract_extras_layer"
  compatible_runtimes = ["python3.12", "python3.13"]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "extract_extras_layer.zip"
  depends_on = [ aws_s3_object.extract_extras_layer]
}

resource "aws_lambda_layer_version" "load_extras_layer" {
  layer_name          = "load_extras_layer"
  compatible_runtimes = ["python3.12", "python3.13"]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "load_extras_layer.zip"
  depends_on = [ aws_s3_object.load_extras_layer]
}


resource "aws_lambda_layer_version" "utils" {
  layer_name          = "utils"
  compatible_runtimes = [var.python_runtime, "python3.13"]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "utils.zip"
  depends_on = [ aws_s3_object.utils]
}

resource "aws_lambda_function" "extract_handler" {
  function_name = var.extract_lambda_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "extract.extract_handler"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "extract_function.zip"

  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  timeout = 720

  runtime = "python3.13" #do not change runtime. For some reason this is the only one compatible with psycopg2
  layers = [aws_lambda_layer_version.etl_layer.arn, 
            aws_lambda_layer_version.utils.arn,
            aws_lambda_layer_version.extract_extras_layer.arn,
            "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:2"]
            
  environment {
    variables = {
      BUCKET = var.ingestion_bucket_name
      PG_USERNAME=jsondecode(data.aws_secretsmanager_secret_version.database_secret.secret_string)["username"],
      PG_DATABASE=jsondecode(data.aws_secretsmanager_secret_version.database_secret.secret_string)["database"],
      PG_PASSWORD=jsondecode(data.aws_secretsmanager_secret_version.database_secret.secret_string)["password"],
      PG_HOST=jsondecode(data.aws_secretsmanager_secret_version.database_secret.secret_string)["host"]
    }
  }
}


# ------------------------------
# Tranform lambda tf code
# ------------------------------

data "archive_file" "transform_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/transform.py"
  output_path      = "${path.module}/../transform_function.zip"
}
#SHARING LAYERS AND ROLE WITH EXTRACT LAMBDA
resource "aws_lambda_function" "transform_handler" {
  function_name = var.transform_lambda_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "transform.transform_handler"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "transform_function.zip"

  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  timeout = 240 #TO CHANGE IF NEEDED

  runtime = var.python_runtime
  layers = [aws_lambda_layer_version.etl_layer.arn, 
            aws_lambda_layer_version.utils.arn,
            "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:17"]

            
  environment {
    variables = {
      BUCKET = var.ingestion_bucket_name
      TRANSFORM_BUCKET = var.transformation_bucket_name

    }
  }
}


# ------------------------------
# Tranform lambda tf code
# ------------------------------

data "archive_file" "load_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/load.py"
  output_path      = "${path.module}/../load_function.zip"
}
#SHARING LAYERS AND ROLE WITH EXTRACT LAMBDA
resource "aws_lambda_function" "load_handler" {
  function_name = var.load_lambda_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "load.load_handler"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "load_function.zip"

  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  timeout = 300 #TO CHANGE IF NEEDED

  runtime = "python3.13"
  layers = [aws_lambda_layer_version.etl_layer.arn, 
            aws_lambda_layer_version.utils.arn,
            aws_lambda_layer_version.load_extras_layer.arn,
            aws_lambda_layer_version.extract_extras_layer.arn,
            "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:2"
            ]

            
  environment {
    variables = {
      TRANSFORM_BUCKET = var.transformation_bucket_name
      PG_CONNECTION=jsondecode(data.aws_secretsmanager_secret_version.warehouse_secret.secret_string)["PG_CONNECTION"],
    }
  }
}