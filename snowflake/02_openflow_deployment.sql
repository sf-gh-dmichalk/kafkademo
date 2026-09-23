/* =============================================================================
   02_openflow_deployment.sql — Shared deployment + DCP + demo runtime
   -----------------------------------------------------------------------------
   Deployment uses OPENFLOW_ADMIN (shared, survives demo teardown).
   DCP proxy + EAI + runtime use OF_KAFKA_ADMIN (demo-specific).

   PREREQUISITES:
     1. MSK cluster ACTIVE (terraform apply)
     2. DCP agent Docker running on EC2 (see README Phase 2)
     3. Replace <MSK_BROKER_*> placeholders with terraform output values

   IF NOT EXISTS everywhere — fully re-runnable, no-op on second run.
   ============================================================================= */

/* --- Shared deployment (OPENFLOW_ADMIN) --- */
USE ROLE OPENFLOW_ADMIN;

CREATE OPENFLOW DEPLOYMENT IF NOT EXISTS OF_DEPLOYMENT
    COMMENT = 'Shared Openflow deployment for demos';

SELECT SYSTEM$WAIT_FOR_OPENFLOW_DEPLOYMENT_STATUS(
    600, 'ACTIVE', 'OF_DEPLOYMENT');

/* --- DCP proxy (ACCOUNTADMIN) ---
   The DCP proxy object is Snowflake's side of the tunnel.
   The DCP agent (Docker on EC2 in the Kafka VPC) is the AWS side.
   Together they let Openflow reach private MSK brokers.          */
USE ROLE ACCOUNTADMIN;

CREATE DATA CONNECTIVITY PROXY IF NOT EXISTS OF_KAFKA_DCP;
ALTER DATA CONNECTIVITY PROXY OF_KAFKA_DCP SET ENABLED = TRUE;

/* Generate a bootstrap token — run interactively, copy to EC2:
   SELECT SYSTEM$GENERATE_DATA_CONNECTIVITY_PROXY_BOOTSTRAP_TOKEN('OF_KAFKA_DCP', 7);
   Then on EC2: docker run ... with the token (see README Phase 2). */

/* Wait for DCP agent to connect:
   DESCRIBE DATA CONNECTIVITY PROXY OF_KAFKA_DCP;
   -- status should show HEALTHY                                    */

/* --- Network rule: MSK brokers via DCP tunnel ---
   MODE = DATA_CONNECTIVITY_PROXY_EGRESS routes through the DCP agent.
   UPDATE the VALUE_LIST with your actual MSK broker hostnames from:
     terraform output msk_bootstrap_brokers_scram                   */
USE ROLE OF_KAFKA_ADMIN;
USE DATABASE OF_KAFKA;

CREATE OR REPLACE NETWORK RULE OF_KAFKA.OPENFLOW.NR_KAFKA
    MODE = DATA_CONNECTIVITY_PROXY_EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'b-1.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096',
        'b-2.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096'
    );

/* --- EAI (standard — DCP linkage is on the proxy, not the EAI) --- */
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION OF_KAFKA_EAI
    ALLOWED_NETWORK_RULES = (OF_KAFKA.OPENFLOW.NR_KAFKA)
    ENABLED = TRUE
    COMMENT = 'Openflow Kafka connector — outbound to MSK via DCP';

/* --- Link EAI to DCP proxy --- */
USE ROLE ACCOUNTADMIN;
ALTER DATA CONNECTIVITY PROXY OF_KAFKA_DCP
    SET EXTERNAL_ACCESS_INTEGRATIONS = (OF_KAFKA_EAI);

GRANT USAGE ON INTEGRATION OF_KAFKA_EAI TO ROLE OF_KAFKA_RUNTIME_ROLE;

/* --- Demo-specific runtime on the shared deployment --- */
USE ROLE OF_KAFKA_ADMIN;

GRANT CREATE OPENFLOW RUNTIME ON SCHEMA OF_KAFKA.OPENFLOW TO ROLE OF_KAFKA_ADMIN;

CREATE OPENFLOW RUNTIME IF NOT EXISTS OF_KAFKA.OPENFLOW.OF_KAFKA_RUNTIME
    IN DEPLOYMENT OF_DEPLOYMENT
    NODE_TYPE = SMALL
    NODE_TYPE_TIER = 'S1'
    MIN_NODES = 1
    MAX_NODES = 1
    EXECUTE_AS_ROLE = OF_KAFKA_RUNTIME_ROLE
    EXTERNAL_ACCESS_INTEGRATIONS = (OF_KAFKA_EAI)
    DISPLAY_NAME = 'OF_KAFKA_RUNTIME'
    COMMENT = 'Runtime for Kafka cold chain connectors';

SELECT SYSTEM$WAIT_FOR_OPENFLOW_RUNTIME_STATUS(
    600, 'ACTIVE', 'OF_KAFKA.OPENFLOW.OF_KAFKA_RUNTIME');

SHOW OPENFLOW RUNTIMES IN SCHEMA OF_KAFKA.OPENFLOW;
