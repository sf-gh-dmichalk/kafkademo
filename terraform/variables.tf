variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "prefix" {
  description = "Naming prefix for all resources"
  type        = string
  default     = "dmichalk-of-kafka"
}

variable "vpc_cidr" {
  description = "CIDR block for the demo VPC"
  type        = string
  default     = "10.42.0.0/16"
}

variable "kafka_version" {
  description = "MSK Kafka version"
  type        = string
  default     = "3.6.0"
}

variable "kafka_username" {
  description = "SASL/SCRAM username for MSK"
  type        = string
  default     = "dmichalk-of-kafka"
}

variable "kafka_password" {
  description = "SASL/SCRAM password for MSK"
  type        = string
  sensitive   = true
  default     = "D3m0-Kafka-2026!"
}

variable "ec2_key_pair_name" {
  description = "EC2 key pair name for SSH access (leave empty to skip)"
  type        = string
  default     = ""
}
