#!/usr/bin/env bash
# ssh_ec2.sh — Push key and SSH to the DCP agent EC2 in one step
#
# Usage:
#   ./tools/ssh_ec2.sh

set -euo pipefail

INSTANCE_ID="i-0fe39c3540183d4d5"
REGION="us-east-1"
PROFILE="isolated"
USER="ec2-user"

IP=$(aws ec2 describe-instances --profile "$PROFILE" --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

aws ec2-instance-connect send-ssh-public-key \
  --profile "$PROFILE" --region "$REGION" \
  --instance-id "$INSTANCE_ID" \
  --instance-os-user "$USER" \
  --ssh-public-key file://${HOME}/.ssh/id_ed25519.pub > /dev/null

exec ssh -o StrictHostKeyChecking=no "$USER@$IP" "$@"
