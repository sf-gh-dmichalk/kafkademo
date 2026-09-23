/* =============================================================================
   99_teardown.sql — drop everything
   -----------------------------------------------------------------------------
   DESTRUCTIVE. Run each statement individually — some may no-op if objects
   are already gone. Every statement uses IF EXISTS or is wrapped to tolerate
   missing objects.

   STEP 0 (MANUAL): Stop and delete both Kafka connectors in Openflow UI.
   ============================================================================= */

USE ROLE OF_KAFKA_ADMIN;
USE DATABASE OF_KAFKA;

/* --- Stream + Dynamic tables (drop in dependency order) --- */
DROP STREAM IF EXISTS INGEST.ALARM_EVENTS_STREAM;
DROP DYNAMIC TABLE IF EXISTS INGEST.ALARM_EVENTS;
DROP DYNAMIC TABLE IF EXISTS INGEST.EQUIPMENT_STATUS;
DROP DYNAMIC TABLE IF EXISTS INGEST.SENSOR_READINGS_CLEAN;

/* --- Raw tables --- */
DROP ICEBERG TABLE IF EXISTS INGEST.SENSOR_READINGS_RAW;

/* --- Openflow runtime: suspend → terminate → drop ---
   Run these one at a time. Skip any that error with "does not exist".
   The shared OF_DEPLOYMENT is LEFT IN PLACE for other demos. */
ALTER OPENFLOW RUNTIME OF_KAFKA.OPENFLOW.OF_KAFKA_RUNTIME SUSPEND;
SELECT SYSTEM$WAIT_FOR_STABLE_OPENFLOW_RUNTIMES(300, 'OF_KAFKA.OPENFLOW.OF_KAFKA_RUNTIME');

ALTER OPENFLOW RUNTIME OF_KAFKA.OPENFLOW.OF_KAFKA_RUNTIME TERMINATE;
SELECT SYSTEM$WAIT_FOR_STABLE_OPENFLOW_RUNTIMES(300, 'OF_KAFKA.OPENFLOW.OF_KAFKA_RUNTIME');

DROP OPENFLOW RUNTIME IF EXISTS OF_KAFKA.OPENFLOW.OF_KAFKA_RUNTIME;

/* --- DCP proxy + integrations + event table (ACCOUNTADMIN) --- */
USE ROLE ACCOUNTADMIN;
ALTER DATA CONNECTIVITY PROXY OF_KAFKA_DCP SET ENABLED = FALSE;
DROP DATA CONNECTIVITY PROXY IF EXISTS OF_KAFKA_DCP;
ALTER ACCOUNT UNSET EVENT_TABLE;
ALTER ACCOUNT UNSET LOG_LEVEL;
ALTER ACCOUNT UNSET TRACE_LEVEL;
DROP INTEGRATION IF EXISTS OF_KAFKA_EAI;

/* --- Database (takes schemas, network rules, stages with it) --- */
DROP DATABASE IF EXISTS OF_KAFKA;

/* --- Warehouse --- */
DROP WAREHOUSE IF EXISTS OF_KAFKA_WH;

/* --- Roles --- */
USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS OF_KAFKA_ADMIN;
DROP ROLE IF EXISTS OF_KAFKA_RUNTIME_ROLE;

/* --- Verify nothing remains --- */
USE ROLE ACCOUNTADMIN;
SHOW DATABASES  LIKE 'OF_KAFKA';
SHOW WAREHOUSES LIKE 'OF_KAFKA%';
SHOW ROLES      LIKE 'OF_KAFKA%';
