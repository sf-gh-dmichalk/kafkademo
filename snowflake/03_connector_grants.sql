/* =============================================================================
   03_connector_grants.sql — Run BEFORE starting the connectors
   -----------------------------------------------------------------------------
   Run as OF_KAFKA_ADMIN.

   After this: install + start both Kafka connectors in Openflow UI,
   wait for rows in SENSOR_READINGS_RAW, then run 04_dynamic_tables.sql.

   Connector 1 (Consumer — MSK → Snowflake):
     Bootstrap:  (from terraform output bootstrap_brokers_sasl_scram)
     Topic:      cold_chain.sensor_readings
     Group:      of_kafka_ingest
     Auth:       SASL/SCRAM (user: dmichalk-of-kafka)
     Database:   OF_KAFKA
     Schema:     INGEST
     Role:       OF_KAFKA_RUNTIME_ROLE
     Warehouse:  OF_KAFKA_WH

   Connector 2 (Producer — Snowflake → MSK):
     Bootstrap:  (from terraform output bootstrap_brokers_sasl_scram)
     Topic:      cold_chain.alarm_events
     Auth:       SASL/SCRAM (user: dmichalk-of-kafka)
     Source:     OF_KAFKA.INGEST.ALARM_EVENTS
     Role:       OF_KAFKA_RUNTIME_ROLE
     Warehouse:  OF_KAFKA_WH
   ============================================================================= */

USE ROLE OF_KAFKA_ADMIN;
USE DATABASE OF_KAFKA;
USE SCHEMA INGEST;
USE WAREHOUSE OF_KAFKA_WH;

/* These are all idempotent — safe to re-run */
GRANT USAGE ON DATABASE OF_KAFKA
    TO ROLE OF_KAFKA_RUNTIME_ROLE;

GRANT USAGE ON SCHEMA OF_KAFKA.INGEST
    TO ROLE OF_KAFKA_RUNTIME_ROLE;

GRANT CREATE TABLE, CREATE DYNAMIC TABLE, CREATE STAGE,
      CREATE SEQUENCE, CREATE CORTEX SEARCH SERVICE
    ON SCHEMA OF_KAFKA.INGEST
    TO ROLE OF_KAFKA_RUNTIME_ROLE;

GRANT USAGE, OPERATE ON WAREHOUSE OF_KAFKA_WH
    TO ROLE OF_KAFKA_RUNTIME_ROLE;
