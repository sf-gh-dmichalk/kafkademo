output "vpc_id" {
  description = "VPC ID — for DCP agent context"
  value       = aws_vpc.main.id
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = aws_msk_cluster.main.arn
}

output "msk_bootstrap_brokers_scram" {
  description = "MSK bootstrap brokers (SASL/SCRAM, port 9096, private)"
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_scram
}

output "msk_zookeeper_connect" {
  description = "MSK ZooKeeper connection string"
  value       = aws_msk_cluster.main.zookeeper_connect_string
}

output "msk_security_group_id" {
  description = "MSK security group ID"
  value       = aws_security_group.msk.id
}

output "dcp_agent_instance_id" {
  description = "DCP agent EC2 instance ID"
  value       = aws_instance.dcp_agent.id
}

output "dcp_agent_public_ip" {
  description = "DCP agent EC2 public IP (for SSH)"
  value       = aws_instance.dcp_agent.public_ip
}

output "dcp_agent_private_ip" {
  description = "DCP agent EC2 private IP (in VPC)"
  value       = aws_instance.dcp_agent.private_ip
}

output "scram_secret_arn" {
  description = "Secrets Manager ARN for MSK SCRAM credentials"
  value       = aws_secretsmanager_secret.msk_scram.arn
}

output "scram_username" {
  description = "SCRAM username for Kafka auth"
  value       = var.kafka_username
}
