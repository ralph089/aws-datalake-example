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
    simple_etl = {
      script_location = "s3://${local.glue_scripts_bucket_name}/jobs/simple_etl.py"
      glue_version    = "5.0"
      python_version  = "3.11"
      max_capacity    = 2
      timeout         = 60
    }
    api_to_lake = {
      script_location = "s3://${local.glue_scripts_bucket_name}/jobs/api_to_lake.py"
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
    daily_simple_etl = {
      schedule_expression = "cron(0 2 * * ? *)"  # 2 AM daily
      target_arn         = module.data_platform.glue_job_arns["simple_etl"]
    }
    daily_api_to_lake = {
      schedule_expression = "cron(0 3 * * ? *)"  # 3 AM daily
      target_arn         = module.data_platform.glue_job_arns["api_to_lake"]
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

locals {
  common_lambda_env = {
    ENVIRONMENT = var.environment
    AWS_REGION  = var.aws_region
    LOG_LEVEL   = "INFO"
  }
  
  # Use provided bucket name or create auto-generated name
  glue_scripts_bucket_name = var.glue_scripts_bucket != null ? var.glue_scripts_bucket : "glue-scripts-${var.environment}"
}

# Data Lake to API Lambda
module "lambda_data_lake_to_api" {
  source = "../../modules/lambda"
  
  function_name = "${var.environment}-data-lake-to-api"
  environment   = var.environment
  handler      = "handler.lambda_handler"
  runtime      = "python3.13"
  timeout      = 300  # 5 minutes for data processing
  memory_size  = 1024  # More memory for data processing
  
  deployment_s3_bucket = var.lambda_artifacts_bucket
  deployment_s3_key    = "${var.environment}/data_lake_to_api/latest/deployment.zip"
  
  environment_variables = merge(
    local.common_lambda_env,
    {
      ATHENA_DATABASE    = "glue_catalog"
      API_SECRET_NAME    = "${var.environment}/api/credentials"
      S3_DATA_BUCKET     = module.data_platform.s3_buckets["silver"]
      ATHENA_WORKGROUP   = "primary"
    }
  )
  
  custom_policies = [
    {
      Effect = "Allow"
      Action = [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ]
      Resource = ["*"]
    },
    {
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        module.data_platform.s3_bucket_arns["silver"],
        "${module.data_platform.s3_bucket_arns["silver"]}/*"
      ]
    },
    {
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ]
      Resource = [
        "arn:aws:s3:::aws-athena-query-results-*/*"
      ]
    },
    {
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = [
        module.secrets.secret_arns["api/credentials"]
      ]
    },
    {
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ]
      Resource = ["*"]
    }
  ]
  
  external_permissions = {
    eventbridge = {
      principal = "events.amazonaws.com"
    }
    apigateway = {
      principal = "apigateway.amazonaws.com"
    }
  }
  
  alarm_sns_topic_arn = module.notifications.topic_arns["job_notifications"]
  log_retention_days  = 14
  
  tags = {
    Purpose = "Data Lake to API"
  }
}

# SNS Glue Trigger Lambda (using generic module)
module "lambda_sns_glue_trigger" {
  source = "../../modules/lambda"
  
  function_name = "${var.environment}-sns-glue-trigger"
  environment   = var.environment
  handler      = "handler.lambda_handler" 
  runtime      = "python3.13"
  timeout      = 60
  memory_size  = 256  # Minimal memory for triggering jobs
  
  deployment_s3_bucket = var.lambda_artifacts_bucket
  deployment_s3_key    = "${var.environment}/sns_glue_trigger/latest/deployment.zip"
  
  environment_variables = merge(
    local.common_lambda_env,
    {
      GLUE_JOB_MAPPINGS = join(",", [
        for prefix, job in {
          "data/"     = "${var.environment}-simple_etl"
          "api/"      = "${var.environment}-api_to_lake"
        } : "${prefix}:${job}"
      ])
    }
  )
  
  custom_policies = [
    {
      Effect = "Allow"
      Action = [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns"
      ]
      Resource = [
        "arn:aws:glue:*:*:job/*"
      ]
    }
  ]
  
  external_permissions = {
    sns = {
      principal  = "sns.amazonaws.com"
      source_arn = var.external_sns_topic_arn
    }
  }
  
  alarm_sns_topic_arn = module.notifications.topic_arns["job_notifications"]
  log_retention_days  = 14
  
  tags = {
    Purpose = "SNS to Glue Job Trigger"
  }
}

# SNS Topic Subscription for the generic SNS trigger lambda
resource "aws_sns_topic_subscription" "sns_glue_trigger" {
  topic_arn = var.external_sns_topic_arn
  protocol  = "lambda"
  endpoint  = module.lambda_sns_glue_trigger.function_arn
  
  filter_policy = jsonencode({
    "eventSource": ["aws:s3"]
  })
}

# S3 bucket for Glue scripts (separate from data lake)
resource "aws_s3_bucket" "glue_scripts" {
  count  = var.glue_scripts_bucket == null ? 1 : 0
  bucket = local.glue_scripts_bucket_name
  
  tags = {
    Environment = var.environment
    Purpose     = "Glue Job Scripts"
  }
}

resource "aws_s3_bucket_versioning" "glue_scripts" {
  count  = var.glue_scripts_bucket == null ? 1 : 0
  bucket = aws_s3_bucket.glue_scripts[0].id
  versioning_configuration {
    status = "Enabled"
  }
}