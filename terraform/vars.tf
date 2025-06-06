variable "ingestion_bucket_name" {
  type    = string
  default = "our-totally-totes-ingestion"
}

variable "code_bucket_name" {
  type    = string
  default = "our-totally-totes-code-src"
}

variable "extract_lambda_name" {
  type    = string
  default = "extract_handler"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

# ------------------------------
# Tranform lambda tf code
# ------------------------------

variable "transformation_bucket_name" {
  type    = string
  default = "our-totally-totes-processed"
}

variable "transform_lambda_name" {
  type    = string
  default = "transform_handler"
}