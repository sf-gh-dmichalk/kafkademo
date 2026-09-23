# --------------------------------------------------------------------------
# ec2.tf — DCP agent + Kafka CLI EC2 instance
# --------------------------------------------------------------------------

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "dcp_agent" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = "t3.micro"
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.dcp.id]
  iam_instance_profile        = aws_iam_instance_profile.dcp_agent.name
  key_name                    = var.ec2_key_pair_name != "" ? var.ec2_key_pair_name : null
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
  }

  user_data = base64encode(<<-USERDATA
    #!/bin/bash
    set -euo pipefail
    dnf update -y
    dnf install -y docker java-17-amazon-corretto-headless
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user
    # Kafka CLI tools
    cd /opt
    curl -sL https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz | tar xz
    ln -sf /opt/kafka_2.13-3.6.0/bin/* /usr/local/bin/
    # DCP credentials directory
    mkdir -p /etc/dcp
    chmod 700 /etc/dcp
    echo "Setup complete" > /tmp/setup-complete
  USERDATA
  )

  tags = { Name = "${var.prefix}-dcp-agent" }
}
