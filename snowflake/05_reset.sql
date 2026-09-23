/* =============================================================================
   05_reset.sql — reset the dynamic table layer, keep connector data
   -----------------------------------------------------------------------------
   Run as OF_KAFKA_ADMIN. Safe to run repeatedly.

   DROPS: dynamic tables (CLEAN, STATUS, ALARMS)
   KEEPS: SENSOR_READINGS_RAW — no re-ingest needed

   After this, re-run 04_dynamic_tables.sql.
   ============================================================================= */

USE ROLE OF_KAFKA_ADMIN;
USE DATABASE OF_KAFKA;
USE SCHEMA INGEST;
USE WAREHOUSE OF_KAFKA_WH;

/* Reassert grants on connector-owned objects */
USE ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA OF_KAFKA.INGEST TO ROLE OF_KAFKA_ADMIN;
USE ROLE OF_KAFKA_ADMIN;

/* Sanity check */
SELECT COUNT(*) AS READINGS_INGESTED FROM SENSOR_READINGS_RAW;

/* Stream */
DROP STREAM IF EXISTS ALARM_EVENTS_STREAM;

/* Dynamic tables */
DROP DYNAMIC TABLE IF EXISTS ALARM_EVENTS;
DROP DYNAMIC TABLE IF EXISTS EQUIPMENT_STATUS;
DROP DYNAMIC TABLE IF EXISTS SENSOR_READINGS_CLEAN;

/* Raw seed table (drop only if you want to re-seed) */
-- DROP ICEBERG TABLE IF EXISTS SENSOR_READINGS_RAW;
