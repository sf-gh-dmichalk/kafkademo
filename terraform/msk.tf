# --------------------------------------------------------------------------
# msk.tf — MSK cluster, configuration, KMS, SCRAM secrets
# --------------------------------------------------------------------------

# ---- KMS key for MSK encryption + SCRAM secrets ----

resource "aws_kms_key" "msk" {
  description             = "KMS key for MSK cluster encryption and SCRAM secrets"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = { Name = "${var.prefix}-msk-kms" }
}

# ---- MSK cluster configuration ----

resource "aws_msk_configuration" "main" {
  name           = "${var.prefix}-config"
  kafka_versions = [var.kafka_version]

  server_properties = <<-PROPS
    auto.create.topics.enable=true
    default.replication.factor=1
    min.insync.replicas=1
    num.io.threads=8
    num.network.threads=5
    num.partitions=3
    num.replica.fetchers=2
    socket.request.max.bytes=104857600
    unclean.leader.election.enable=true
    log.retention.hours=24
  PROPS
}

# ---- MSK cluster (smallest possible, SCRAM + TLS) ----

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.prefix}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type  = "kafka.t3.small"
    client_subnets = aws_subnet.private[*].id
    security_groups = [aws_security_group.msk.id]

    connectivity_info {
      public_access {
        type = "DISABLED"
      }
    }

    storage_info {
      ebs_storage_info {
        volume_size = 10
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn

    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      scram = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = false
        log_group = ""
      }
    }
  }

  tags = { Name = "${var.prefix}-msk" }
}

# ---- SASL/SCRAM credentials via Secrets Manager ----

resource "aws_secretsmanager_secret" "msk_scram" {
  name       = "AmazonMSK_${var.prefix}_scram"
  kms_key_id = aws_kms_key.msk.key_id

  tags = { Name = "${var.prefix}-scram-secret" }
}

resource "aws_secretsmanager_secret_version" "msk_scram" {
  secret_id = aws_secretsmanager_secret.msk_scram.id
  secret_string = jsonencode({
    username = var.kafka_username
    password = var.kafka_password
  })
}

resource "aws_msk_scram_secret_association" "main" {
  cluster_arn     = aws_msk_cluster.main.arn
  secret_arn_list = [aws_secretsmanager_secret.msk_scram.arn]

  depends_on = [aws_secretsmanager_secret_version.msk_scram]
}
