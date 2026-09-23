# --------------------------------------------------------------------------
# iam.tf — IAM roles for EC2 DCP agent
# --------------------------------------------------------------------------

resource "aws_iam_role" "dcp_agent" {
  name = "${var.prefix}-dcp-agent"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  tags = { Name = "${var.prefix}-dcp-agent-role" }
}

resource "aws_iam_instance_profile" "dcp_agent" {
  name = "${var.prefix}-dcp-agent"
  role = aws_iam_role.dcp_agent.name
}

# MSK access for Kafka CLI testing
resource "aws_iam_role_policy" "dcp_agent_msk" {
  name = "msk-access"
  role = aws_iam_role.dcp_agent.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster"
        ]
        Resource = [aws_msk_cluster.main.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:CreateTopic",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:AlterGroup"
        ]
        Resource = ["${aws_msk_cluster.main.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kafka:GetBootstrapBrokers", "kafka:DescribeCluster"]
        Resource = [aws_msk_cluster.main.arn]
      }
    ]
  })
}

# Secrets Manager read for SCRAM credentials
resource "aws_iam_role_policy" "dcp_agent_secrets" {
  name = "secrets-read"
  role = aws_iam_role.dcp_agent.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [aws_secretsmanager_secret.msk_scram.arn]
    }]
  })
}

# KMS decrypt for SCRAM secret
resource "aws_iam_role_policy" "dcp_agent_kms" {
  name = "kms-decrypt"
  role = aws_iam_role.dcp_agent.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = [aws_kms_key.msk.arn]
    }]
  })
}
