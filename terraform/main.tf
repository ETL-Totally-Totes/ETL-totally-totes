terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
    Project_name = "ETL-totally-totes",
    terraform_managed = "true",
    repo_link = "https://github.com/ETL-Totally-Totes/ETL-totally-totes"
    }
  }
}

terraform {
  backend "s3" {
    bucket = "etl-totally-totes-backend"
    key    = "terraform/.terraform/terraform.tfstate"
    region = "eu-west-2"
  }
}