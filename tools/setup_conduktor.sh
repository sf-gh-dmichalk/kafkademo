#!/usr/bin/env bash
# setup_conduktor.sh — Install Conduktor Console on the DCP agent EC2
#
# Usage:
#   ./setup_conduktor.sh <ec2-public-ip> <bootstrap-brokers> <scram-user> <scram-password>
#
# Prerequisites:
#   - EC2 with Docker installed (user_data handles this)
#   - EC2 Instance Connect or SSH key for access
#   - Port 8080 open in security group

set -euo pipefail

EC2_IP="${1:?Usage: $0 <ec2-public-ip> <bootstrap-brokers> <scram-user> <scram-password>}"
BROKERS="${2:?Provide bootstrap brokers}"
SCRAM_USER="${3:?Provide SCRAM username}"
SCRAM_PASS="${4:?Provide SCRAM password}"

echo "=== Deploying Conduktor Console to ${EC2_IP} ==="

# Push SSH key via EC2 Instance Connect
aws ec2-instance-connect send-ssh-public-key \
  --profile isolated --region us-east-1 \
  --instance-id "$(aws ec2 describe-instances --profile isolated --region us-east-1 \
    --filters "Name=ip-address,Values=${EC2_IP}" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text)" \
  --instance-os-user ec2-user \
  --ssh-public-key file://${HOME}/.ssh/id_ed25519.pub > /dev/null

ssh -o StrictHostKeyChecking=no ec2-user@"$EC2_IP" bash -s <<REMOTE
set -euo pipefail

# Install docker-compose plugin if not present
if ! sudo docker compose version &>/dev/null; then
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  sudo curl -fSL "https://github.com/docker/compose/releases/latest/download/docker-compose-\$(uname -s)-\$(uname -m)" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

cat > /home/ec2-user/conduktor-compose.yml <<'COMPOSE'
services:
  conduktor-console:
    image: conduktor/conduktor-console:latest
    ports:
      - "8080:8080"
    environment:
      CDK_IN_CONF_FILE: /opt/conduktor/platform-config.yaml
    volumes:
      - conduktor_data:/var/conduktor
      - /home/ec2-user/conduktor-platform-config.yaml:/opt/conduktor/platform-config.yaml:ro
    restart: unless-stopped

volumes:
  conduktor_data:
COMPOSE

cat > /home/ec2-user/conduktor-platform-config.yaml <<EOF
organization:
  name: "of-kafka-demo"

clusters:
  - id: dmichalk-of-kafka-msk
    name: "Cold Chain MSK"
    bootstrapServers: "${BROKERS}"
    properties: |
      security.protocol=SASL_SSL
      sasl.mechanism=SCRAM-SHA-512
      sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${SCRAM_USER}" password="${SCRAM_PASS}";

auth:
  demo-users:
    - email: admin@conduktor.io
      password: Admin123!
      groups:
        - ADMIN
EOF

sudo docker compose -f /home/ec2-user/conduktor-compose.yml down 2>/dev/null || true
sudo docker compose -f /home/ec2-user/conduktor-compose.yml up -d

echo ""
echo "=== Conduktor Console starting ==="
echo "URL: http://${EC2_IP}:8080"
echo "Login: admin@conduktor.io / admin"
REMOTE

echo ""
echo "=== Done ==="
echo "Conduktor Console: http://${EC2_IP}:8080"
echo "Login: admin@conduktor.io / admin"
