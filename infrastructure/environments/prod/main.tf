terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# External module that provides S3, Glue jobs, and Catalog
module "data_platform" {
  source = "git::https://github.com/your-org/terraform-aws-data-platform.git?ref=v1.0.0"
  
  environment = var.environment
  project     = var.project_name
  
  # Configuration for the external module
  enable_glue_catalog = true
  enable_s3_buckets   = true
  medallion_layers    = ["bronze", "silver", "gold"]
  
  glue_jobs = {
    customer_import = {
      script_location = "s3://glue-scripts-${var.environment}/jobs/customer_import.py"
      glue_version    = "5.0"
      python_version  = "3.11"
      max_capacity    = 2
      timeout         = 60
    }
    sales_etl = {
      script_location = "s3://glue-scripts-${var.environment}/jobs/sales_etl.py"
      glue_version    = "5.0"
      python_version  = "3.11"
      max_capacity    = 4
      timeout         = 120
    }
    inventory_sync = {
      script_location = "s3://glue-scripts-${var.environment}/jobs/inventory_sync.py"
      glue_version    = "5.0"
      python_version  = "3.11"
      max_capacity    = 2
      timeout         = 60
    }
    product_catalog = {
      script_location = "s3://glue-scripts-${var.environment}/jobs/product_catalog.py"
      glue_version    = "5.0"
      python_version  = "3.11"
      max_capacity    = 2
      timeout         = 60
    }
  }
}

# Secrets Manager for API credentials
module "secrets" {
  source = "../../modules/secrets-manager"
  
  environment = var.environment
  secrets = {
    "api/credentials" = {
      description = "External API credentials"
      secret_string = jsonencode({
        base_url     = var.api_base_url
        bearer_token = var.api_bearer_token
      })
    }
  }
}

# EventBridge for scheduling and S3 triggers
module "eventbridge" {
  source = "../../modules/eventbridge"
  
  environment = var.environment
  
  schedules = {
    daily_customer_import = {
      schedule_expression = "cron(0 2 * * ? *)"  # 2 AM daily
      target_arn         = module.data_platform.glue_job_arns["customer_import"]
    }
    weekly_inventory_sync = {
      schedule_expression = "cron(0 3 ? * MON *)"  # Monday 3 AM
      target_arn         = module.data_platform.glue_job_arns["inventory_sync"]
    }
  }
  
  s3_event_rules = {
    sales_file_trigger = {
      bucket_name = module.data_platform.s3_buckets["bronze"]
      prefix      = "sales/"
      suffix      = ".csv"
      target_arn  = module.data_platform.glue_job_arns["sales_etl"]
    }
  }
}

# Dead Letter Queue for failed jobs
module "dlq" {
  source = "../../modules/sqs-dlq"
  
  environment = var.environment
  queues = {
    glue_dlq = {
      message_retention_seconds = 1209600  # 14 days
      visibility_timeout_seconds = 300
    }
  }
}

# SNS for notifications
module "notifications" {
  source = "../../modules/sns-notifications"
  
  environment = var.environment
  topics = {
    job_notifications = {
      display_name = "Glue Job Notifications"
      subscriptions = [
        {
          protocol = "email"
          endpoint = var.notification_email
        }
      ]
    }
  }
}

# S3 bucket for Glue scripts (separate from data lake)
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "glue-scripts-${var.environment}"
  
  tags = {
    Environment = var.environment
    Purpose     = "Glue Job Scripts"
  }
}

resource "aws_s3_bucket_versioning" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  versioning_configuration {
    status = "Enabled"
  }
}