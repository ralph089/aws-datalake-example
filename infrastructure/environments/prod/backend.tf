terraform {
  backend "s3" {
    bucket         = "terraform-state-aws-glue-etl-prod"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks-aws-glue-etl"
    encrypt        = true
  }
}