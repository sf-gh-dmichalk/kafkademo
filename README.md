# Kafka Openflow Demo — Cold Chain Monitoring

A grocery retailer's freezers and refrigerators report temperature telemetry every 30 seconds from IoT sensors across 50 stores. When a unit drifts out of safe range, the downstream system needs to know immediately — not tomorrow morning when someone opens a ticket.

**Kafka → DCP → Openflow → Dynamic Iceberg Tables → Openflow → DCP → Kafka**

## Scenario

**Flow 1 — Ingest + Enrich (Kafka → Snowflake)**

IoT gateways at each store publish temperature readings to an MSK topic (`cold_chain.sensor_readings`). Each message is a JSON payload:

```json
{
  "sensor_id": "FRZ-4421-003",
  "store_id": 4421,
  "equipment_type": "FREEZER",
  "zone": "FROZEN_FOODS",
  "temperature_f": -2.4,
  "set_point_f": 0.0,
  "door_open": false,
  "reading_ts": "2026-09-22T14:32:08Z"
}
```

Openflow reads from this topic via a DCP tunnel into the Kafka VPC and lands rows into `OF_KAFKA.INGEST.SENSOR_READINGS_RAW` (Iceberg table).

Three dynamic Iceberg tables process the raw data:

- **`SENSOR_READINGS_CLEAN`** — Deduplicates, casts types, adds `deviation_f` (actual minus set point), flags `is_alarm` when deviation exceeds threshold (>5°F for freezers, >3°F for refrigerators)
- **`EQUIPMENT_STATUS`** — Rolling 5-minute window per sensor: avg temp, max deviation, alarm count, door-open count. This is the "current state" table — one row per sensor, always fresh
- **`ALARM_EVENTS`** — Captures alarm transitions (entered alarm state, exited alarm state) with duration. Only rows where `is_alarm` flipped from the previous reading. This is the CDC-friendly table — new rows = new events

**Flow 2 — Alert Publish (Snowflake → Kafka)**

Openflow reads from `OF_KAFKA.INGEST.ALARM_EVENTS` (CDC via streams or table polling) and publishes each new alarm event to MSK topic `cold_chain.alarm_events`. Downstream consumers (store alerting dashboards, dispatch systems, compliance logging) subscribe to this topic.

The round-trip tells the full story: raw sensor data flows in, Snowflake computes whether something is wrong, and alarm events flow back out to the operational systems that need to act.

## Architecture

```
┌─────────────┐         ┌──────────────────┐
│  IoT Sensors │───────▶│  AWS MSK Topic    │
│  (50 stores) │        │  sensor_readings  │
└─────────────┘         └────────┬─────────┘
                                 │ (private, SCRAM+TLS)
                                 ▼
                        ┌──────────────────┐         ┌──────────────────────┐
                        │  DCP Agent (EC2) │◀──443──▶│  Snowflake DCP Proxy │
                        │  Docker, in VPC  │         │  OF_KAFKA_DCP        │
                        └────────┬─────────┘         └──────────┬───────────┘
                                 │ 9096                         │
                                 ▼                              ▼
                        ┌──────────────────┐         ┌──────────────────────┐
                        │  MSK Brokers     │         │  Openflow Runtime    │
                        │  kafka.t3.small  │         │  OF_KAFKA_RUNTIME    │
                        └──────────────────┘         └──────────┬───────────┘
                                                                │
                                                     ┌──────────────────────┐
                                                     │ SENSOR_READINGS_RAW  │
                                                     │ (Iceberg table)      │
                                                     └──────────┬───────────┘
                                                                │
                                          ┌─────────────────────┼─────────────────────┐
                                          ▼                     ▼                     ▼
                                ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
                                │ SENSOR_READINGS  │ │ EQUIPMENT_STATUS │ │ ALARM_EVENTS     │
                                │ _CLEAN (DT)      │ │ (DT)             │ │ (DT)             │
                                └──────────────────┘ └──────────────────┘ └────────┬─────────┘
                                                                                   │
                                                                        ┌──────────┴───────────┐
                                                                        │  Openflow → DCP      │
                                                                        │  → MSK alarm_events  │
                                                                        └──────────────────────┘
```

## Prerequisites

- Snowflake account with Openflow enabled (ORGADMIN must accept Openflow ToS once per org)
- `OPENFLOW_ADMIN` role with `CREATE OPENFLOW DEPLOYMENT ON ACCOUNT` and `CREATE COMPUTE POOL ON ACCOUNT`
- AWS account (`isolated` profile, account `913524911227`) with Contributor access
- Terraform >= 1.5 installed locally
- AWS CLI configured with `isolated` profile

## Setup — step by step

### Phase 0: AWS infrastructure (Terraform)

```bash
cd terraform
terraform init
terraform apply
```

Creates (~20 min for MSK):
- VPC with 2 public subnets (EC2), 2 private subnets (MSK brokers), NAT gateway
- MSK Provisioned cluster (`dmichalk-of-kafka-msk`): 2x kafka.t3.small, SASL/SCRAM + TLS, KMS encryption
- SCRAM credentials in Secrets Manager
- EC2 instance (`t3.micro`): DCP agent + Kafka CLI tools, Amazon Linux 2023
- IAM role for EC2 with MSK + Secrets Manager + KMS access
- Security groups for MSK (port 9096 from VPC) and EC2 (SSH + all outbound)

Outputs: bootstrap brokers, VPC ID, EC2 public IP, SCRAM secret ARN.

### Phase 1: Snowflake objects (SQL, run in Snowsight)

**`00_account_setup.sql`** — Run as ACCOUNTADMIN

Creates two roles (`OF_KAFKA_ADMIN`, `OF_KAFKA_RUNTIME_ROLE`), grants them to SYSADMIN and DMICHALK, and sets account-level privileges. Safe to re-run.

**`01_demo_objects.sql`** — Starts as SYSADMIN, switches to OF_KAFKA_ADMIN

Creates the `OF_KAFKA` database (Iceberg v3, Snowflake-managed storage), `OF_KAFKA_WH` warehouse (SMALL), `INGEST` and `OPENFLOW` schemas, event table `DEMO_EVENTS`. Hands ownership to `OF_KAFKA_ADMIN`.

### Phase 2: DCP agent setup (EC2 + Snowflake)

**In Snowsight (as ACCOUNTADMIN):**
```sql
CREATE DATA CONNECTIVITY PROXY IF NOT EXISTS OF_KAFKA_DCP;
ALTER DATA CONNECTIVITY PROXY OF_KAFKA_DCP SET ENABLED = TRUE;
SELECT SYSTEM$GENERATE_DATA_CONNECTIVITY_PROXY_BOOTSTRAP_TOKEN('OF_KAFKA_DCP', 7);
```
Copy the token.

**On EC2 (SSH to dcp_agent_public_ip from terraform output):**
```bash
# Write bootstrap token to file
sudo mkdir -p /etc/dcp
echo '<paste token here>' | sudo tee /etc/dcp/credentials > /dev/null
sudo chmod 644 /etc/dcp/credentials

# Pull and run DCP agent
sudo docker pull snowflakedb/dcp-client:latest
sudo docker run -d --name dcp-agent \
  --restart unless-stopped \
  -v /etc/dcp/credentials:/etc/dcp-agent/secrets/dcp-bootstrap-token:ro \
  snowflakedb/dcp-client:latest

# Check logs (should show "control session refreshed")
sudo docker logs dcp-agent 2>&1 | tail -10
```

**Verify in Snowsight:**
```sql
DESCRIBE DATA CONNECTIVITY PROXY OF_KAFKA_DCP;
-- status should show HEALTHY, reachable_destinations lists broker endpoints
```

### Phase 3: Openflow deployment + runtime (SQL)

**`02_openflow_deployment.sql`** — Starts as OPENFLOW_ADMIN, then OF_KAFKA_ADMIN + ACCOUNTADMIN

Creates the shared `OF_DEPLOYMENT` (no-op if it exists from another demo). Creates the `NR_KAFKA` network rule with `MODE = DATA_CONNECTIVITY_PROXY_EGRESS` pointing to MSK broker endpoints (from terraform output). Creates `OF_KAFKA_EAI` and links it to the DCP proxy. Creates `OF_KAFKA_RUNTIME` on the shared deployment.

**Before running:** replace `<MSK_BROKER_*>` placeholders with actual broker hostnames from:
```bash
cd terraform && terraform output msk_bootstrap_brokers_scram
```

**`03_connector_grants.sql`** — Run as OF_KAFKA_ADMIN

Grants the runtime role access to database, schemas, and warehouse.

### Phase 4: Openflow connectors (Openflow UI)

**Connector 1 — Kafka HP Ingest (MSK → Snowflake)**

Template: **Apache Kafka** ("Kafka High Performance" in parameter contexts)

Uses Snowpipe Streaming — serverless, no warehouse needed.

| Parameter | Value |
|-----------|-------|
| Bootstrap Servers | `b-1.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096,b-2.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096` |
| Topic | `cold_chain.sensor_readings` |
| Consumer Group | `of_kafka_ingest` |
| Security Protocol | `SASL_SSL` |
| SASL Mechanism | `SCRAM-SHA-512` |
| SASL Username | `dmichalk-of-kafka` |
| SASL Password | `D3m0-Kafka-2026!` |
| Destination Database | `OF_KAFKA` |
| Destination Schema | `INGEST` |
| Snowflake Role | `OF_KAFKA_RUNTIME_ROLE` |
| Snowflake Authentication | `SNOWFLAKE_MANAGED` |

**Connector 2 — Kafka Sink / CDC (Snowflake → MSK)**

Template: **Snowflake to Kafka without mTLS encryption** ("Apache Kafka Sink (SASL)" in parameter contexts)

Has 3 parameter contexts — Source (Snowflake connection), Ingestion (Snowflake CDC connection), Destination (Kafka broker). The source table is configured on the processor canvas, not in parameter contexts.

*Kafka Sink SASL Source Parameters (Snowflake connection for metadata):*

| Parameter | Value |
|-----------|-------|
| Snowflake Role | `OF_KAFKA_RUNTIME_ROLE` |
| Snowflake Warehouse | `OF_KAFKA_WH` |
| Source Database | `OF_KAFKA` |
| Source Schema | `INGEST` |
| Snowflake Authentication | `SNOWFLAKE_MANAGED` |

*Kafka Sink SASL Ingestion Parameters (Snowflake CDC source):*

| Parameter | Value |
|-----------|-------|
| Snowflake FQN Stream Name | `OF_KAFKA.INGEST.ALARM_EVENTS_STREAM` |
| Snowflake Role | `OF_KAFKA_RUNTIME_ROLE` |
| Snowflake Authentication | `SNOWFLAKE_MANAGED` |

*Kafka Sink SASL Destination Parameters (Kafka broker):*

| Parameter | Value |
|-----------|-------|
| Bootstrap Servers | `b-1.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096,b-2.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096` |
| Topic | `cold_chain.alarm_events` |
| Security Protocol | `SASL_SSL` |
| SASL Mechanism | `SCRAM-SHA-512` |
| SASL Username | `dmichalk-of-kafka` |
| SASL Password | `D3m0-Kafka-2026!` |

### Phase 5: Dynamic table pipeline (SQL)

**`04_dynamic_tables.sql`** — Run as OF_KAFKA_ADMIN

Creates the three dynamic Iceberg tables and a stream on `ALARM_EVENTS` for the Kafka Sink connector's CDC reads. Target lag: 1 minute for CLEAN, 2 minutes for STATUS and ALARMS.

### Phase 6: Produce sensor data

**On EC2** (or re-run anytime to add more data):
```bash
python3 produce_sensor_data.py
```

Produces 6,000 JSON messages (50 stores × 6 sensors × 20 readings) with 3 alarm scenarios:

- Store 4421: Freezer FRZ-4421-003 compressor failing — temp climbing from -10°F to +18°F
- Store 6010: Walk-in cooler door stuck open — temp spiking to 52°F
- Store 7345: Refrigeration unit cycling — intermittent alarms every few minutes

The HP connector ingests them into `SENSOR_READINGS_RAW` (schema-evolved flat columns with FLOAT types). Dynamic tables refresh within 1-2 minutes. Alarm events flow back to `cold_chain.alarm_events` via the Sink connector.

## Reset and teardown

**`05_reset.sql`** — Drops the dynamic tables. Re-run `04_dynamic_tables.sql` to rebuild. Keeps connector data.

**`99_teardown.sql`** — Drops everything Snowflake-side (DCP proxy, runtime, database, warehouse, roles). The shared `OF_DEPLOYMENT` is left in place for other demos.

**Terraform destroy:**
```bash
cd terraform
terraform destroy
```

Removes all AWS resources (MSK cluster, VPC, EC2, IAM, KMS, Secrets Manager, NAT gateway).

## Repo structure

```
terraform/
├── main.tf                 Provider config (isolated profile, us-east-1)
├── variables.tf            Region, prefix (dmichalk-of-kafka), SCRAM creds
├── vpc.tf                  VPC, public/private subnets, IGW, NAT, SGs
├── msk.tf                  MSK Provisioned (kafka.t3.small, SCRAM+TLS, KMS)
├── ec2.tf                  DCP agent EC2 (t3.micro, Amazon Linux 2023, Docker)
├── iam.tf                  IAM role for EC2 (MSK + Secrets Manager + KMS)
└── outputs.tf              Bootstrap brokers, VPC ID, EC2 IP, SCRAM secret

snowflake/
├── 00_account_setup.sql    Roles + privileges (ACCOUNTADMIN)
├── 01_demo_objects.sql     Database, warehouse, schemas, event table
├── 02_openflow_deployment.sql  DCP proxy + EAI + shared deployment + runtime
├── 03_connector_grants.sql     Runtime role grants
├── 04_dynamic_tables.sql   Dynamic Iceberg tables (clean, status, alarms) + stream
├── 05_reset.sql            Rebuild dynamic table layer only
└── 99_teardown.sql         Drop everything (except shared deployment)

tools/
└── produce_sensor_data.py  Python Kafka producer (6K sensor readings, 3 alarm scenarios)
```

## Snowflake objects

**Roles** (2 — self-contained, no shared globals)

- `OF_KAFKA_ADMIN` — owns everything in this demo
- `OF_KAFKA_RUNTIME_ROLE` — execute-as identity for the runtime

**Infrastructure**

- `OF_KAFKA` — Database (Iceberg v3, Snowflake-managed storage)
- `INGEST` — Schema for connector + dynamic table objects
- `OPENFLOW` — Schema for runtime object
- `OF_KAFKA_WH` — Warehouse (SMALL)
- `OF_KAFKA_DCP` — Data Connectivity Proxy (DCP tunnel to Kafka VPC)
- `OF_KAFKA_EAI` — External access integration (MSK egress via DCP)
- `DEMO_EVENTS` — Event table for runtime telemetry

**Openflow**

- `OF_DEPLOYMENT` — Shared gen 2 deployment (owned by OPENFLOW_ADMIN)
- `OF_KAFKA_RUNTIME` — Demo-specific runtime (Small/S1, 1 node)

**Dynamic Iceberg tables**

- `SENSOR_READINGS_RAW` — Raw Kafka messages (created by connector or seed script)
- `SENSOR_READINGS_CLEAN` — Deduplicated, typed, with deviation + alarm flag
- `EQUIPMENT_STATUS` — Rolling 5-min window, one row per sensor (current state)
- `ALARM_EVENTS` — Alarm transitions with duration (CDC source for outbound Kafka)
- `ALARM_EVENTS_STREAM` — Stream on ALARM_EVENTS for Kafka Sink connector CDC

## AWS resources (Terraform-managed)

- `dmichalk-of-kafka-msk` — MSK Provisioned (2x kafka.t3.small, SCRAM+TLS, KMS)
- `dmichalk-of-kafka-vpc` — VPC with 2 public + 2 private subnets, NAT gateway
- `dmichalk-of-kafka-dcp-agent` — EC2 t3.micro running DCP Docker agent
- `dmichalk-of-kafka-msk-sg` — Security group (port 9096 from VPC)
- `dmichalk-of-kafka-dcp-sg` — Security group (SSH + all outbound)
- `dmichalk-of-kafka-msk-kms` — KMS key for MSK + SCRAM encryption
- `AmazonMSK_dmichalk-of-kafka_scram` — Secrets Manager SCRAM credentials

## Demo narrative

> "Your stores have 300 freezers and refrigerators. Each one has a sensor reporting temperature every 30 seconds. That's 600,000 readings a day hitting Kafka. Right now your CMMS checks once an hour. If a compressor fails at 2 AM, you lose $40K in product before anyone notices."

Store #4421's walk-in freezer (FRZ-4421-003) starts drifting at 2:14 AM. By 2:18 the deviation crosses 5°F. Snowflake's dynamic table catches the transition and creates an alarm event. Openflow publishes it back to Kafka within 2 minutes. The store's dispatch system picks it up and pages the on-call tech. Total time from drift to page: under 5 minutes. The hourly poll would have caught it at 3:00 AM — 42 minutes of product exposure eliminated.

Meanwhile, Store 6010's walk-in cooler door is stuck open (temperature at 52°F) — that's a different alarm pattern (rapid spike vs. gradual drift). And Store 7345 has an intermittent compressor cycling issue that creates repeated short-duration alarms, which the `EQUIPMENT_STATUS` table exposes as a high alarm-count pattern even when no single alarm lasts long.

Three failure modes, one pipeline, two Kafka topics, zero manual steps.
