variable "aws_region" {
  type        = string
  description = "AWS region for the platform."
  default     = "ap-south-1"
}

variable "project_name" {
  type        = string
  description = "Lowercase resource prefix."
  default     = "pulsestream"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project_name))
    error_message = "project_name must be a lowercase AWS-safe prefix."
  }
}

variable "environment" {
  type        = string
  description = "Deployment environment."
  default     = "dev"
  validation {
    condition     = contains(["dev", "stage", "prod"], var.environment)
    error_message = "environment must be dev, stage or prod."
  }
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC for MSK Serverless."
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "At least two private subnets in different AZs."
  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "Supply at least two private subnets."
  }
}

variable "stream_client_security_group_ids" {
  type        = list(string)
  description = "Security groups allowed to reach MSK using IAM authentication."
  default     = []
}

variable "lambda_package_path" {
  type        = string
  description = "Package created by scripts/build_lambda_package.py."
  default     = "../../dist/pulsestream-lambdas.zip"
}

variable "alarm_topic_arn" {
  type        = string
  description = "Optional SNS topic for alarm delivery."
  default     = ""
}
