data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract.py"
  output_path      = "${path.module}/../extract_function.zip"
}

# resource "null_resource" "set_up_zips" {
#     provisioner "local-exec" {
#       command = <<EOT
#         cd ../
#         echo "Cleaning up..."
#         rm -rf python 
#         rm -rf util_layer.zip
#         rm -rf layer.zip

#         echo "Creating dependencies"
#         pip install -r requirements.txt --platform manylinux2014_x86_64 --only-binary=:all: -t python/
#         zip -r layer.zip python/

#         rm -rf python 

#         echo "Creating utils"
#         mkdir python
#         cp -r src python
#         zip -r util_layer.zip python
#         cd terraform
#       EOT
#     }
# }


resource "aws_lambda_layer_version" "etl_layer" {
  layer_name          = "etl_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "layer.zip"
  depends_on = [ aws_s3_object.layer]#, null_resource.set_up_zips ]
}

resource "aws_lambda_layer_version" "utils" {
  layer_name          = "utils"
  compatible_runtimes = [var.python_runtime]
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "utils.zip"
  depends_on = [ aws_s3_object.utils]#, null_resource.set_up_zips ]
}

resource "aws_lambda_function" "extract_handler" {
  function_name = var.extract_lambda_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "extract.extract_handler"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = "extract_function.zip"

  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  timeout = 120

  runtime = "python3.13"
  layers = [aws_lambda_layer_version.etl_layer.arn, 
            aws_lambda_layer_version.utils.arn]
            
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
  timeout = 120 #TO CHANGE IF NEEDED

  runtime = var.python_runtime
  layers = [aws_lambda_layer_version.etl_layer.arn, 
            aws_lambda_layer_version.utils.arn]
            
  environment {
    variables = {
      BUCKET = var.transformation_bucket_name
    }
  }
}