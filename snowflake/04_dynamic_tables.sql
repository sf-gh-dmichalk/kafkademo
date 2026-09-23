/* =============================================================================
   04_dynamic_tables.sql — Dynamic Iceberg tables for cold chain pipeline
   -----------------------------------------------------------------------------
   Run as OF_KAFKA_ADMIN after the Kafka consumer connector is running
   and SENSOR_READINGS_RAW has data.

   Three dynamic tables + one stream:
     1. SENSOR_READINGS_CLEAN  — deduplicate, compute deviation + alarm flag
     2. EQUIPMENT_STATUS       — rolling 30-min window, one row per sensor
     3. ALARM_EVENTS           — alarm state transitions (CDC for outbound Kafka)
     4. ALARM_EVENTS_STREAM    — stream for Kafka Sink connector

   IMPORTANT: No CURRENT_TIMESTAMP() in CLEAN or ALARM_EVENTS — keeps them
   INCREMENTAL so streams work. EQUIPMENT_STATUS uses CURRENT_TIMESTAMP in
   the WHERE clause so it's FULL refresh (no stream needed on it).
   ============================================================================= */

USE ROLE OF_KAFKA_ADMIN;
USE DATABASE OF_KAFKA;
USE SCHEMA INGEST;
USE WAREHOUSE OF_KAFKA_WH;

/* --- Reassert grants on connector-owned objects --- */
USE ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA OF_KAFKA.INGEST TO ROLE OF_KAFKA_ADMIN;
USE ROLE OF_KAFKA_ADMIN;

/* --- 1. SENSOR_READINGS_CLEAN ---
   Schema-evolved columns from the HP connector are already typed
   (sensor_id, store_id, temperature_f, etc. are real columns).
   Deduplicates by sensor_id + reading_ts, computes deviation, flags alarms.
   Threshold: freezers >5°F deviation, refrigerators >3°F deviation.
   INCREMENTAL refresh — no CURRENT_TIMESTAMP().
   ----------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC ICEBERG TABLE SENSOR_READINGS_CLEAN
    TARGET_LAG = '1 minute'
    WAREHOUSE = OF_KAFKA_WH
    REFRESH_MODE = INCREMENTAL
    AS
SELECT
    sensor_id, store_id, equipment_type, zone,
    temperature_f, set_point_f, door_open, reading_ts,
    temperature_f - set_point_f AS deviation_f,
    CASE
        WHEN equipment_type = 'FREEZER'      AND ABS(temperature_f - set_point_f) > 5 THEN TRUE
        WHEN equipment_type = 'REFRIGERATOR' AND ABS(temperature_f - set_point_f) > 3 THEN TRUE
        ELSE FALSE
    END AS is_alarm
FROM SENSOR_READINGS_RAW
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sensor_id, reading_ts
    ORDER BY kafkaMetadata:offset DESC
) = 1;

/* --- 2. EQUIPMENT_STATUS ---
   Rolling 30-min window per sensor. One row per sensor = current state.
   Uses CURRENT_TIMESTAMP in WHERE → FULL refresh (that's fine, no stream needed).
   ----------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC ICEBERG TABLE EQUIPMENT_STATUS
    TARGET_LAG = '2 minutes'
    WAREHOUSE = OF_KAFKA_WH
    AS
SELECT
    sensor_id, store_id, equipment_type, zone,
    COUNT(*) AS reading_count,
    ROUND(AVG(temperature_f), 1) AS avg_temp_f,
    ROUND(MIN(temperature_f), 1) AS min_temp_f,
    ROUND(MAX(temperature_f), 1) AS max_temp_f,
    ROUND(AVG(deviation_f), 1) AS avg_deviation_f,
    ROUND(MAX(ABS(deviation_f)), 1) AS max_abs_deviation_f,
    SUM(CASE WHEN is_alarm THEN 1 ELSE 0 END) AS alarm_count,
    SUM(CASE WHEN door_open THEN 1 ELSE 0 END) AS door_open_count,
    MAX(reading_ts) AS last_reading_ts,
    CASE
        WHEN SUM(CASE WHEN is_alarm THEN 1 ELSE 0 END) > 0 THEN 'ALARM'
        WHEN MAX(ABS(deviation_f)) > 2 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS status
FROM SENSOR_READINGS_CLEAN
WHERE reading_ts >= DATEADD('minute', -30, CURRENT_TIMESTAMP())
GROUP BY sensor_id, store_id, equipment_type, zone;

/* --- 3. ALARM_EVENTS ---
   Detects alarm state transitions using LAG().
   INCREMENTAL refresh — no CURRENT_TIMESTAMP() so stream works.
   ----------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC ICEBERG TABLE ALARM_EVENTS
    TARGET_LAG = '2 minutes'
    WAREHOUSE = OF_KAFKA_WH
    REFRESH_MODE = INCREMENTAL
    AS
WITH readings_with_lag AS (
    SELECT sensor_id, store_id, equipment_type, zone,
        temperature_f, set_point_f, deviation_f, is_alarm, reading_ts,
        LAG(is_alarm) OVER (PARTITION BY sensor_id ORDER BY reading_ts) AS prev_is_alarm,
        LAG(reading_ts) OVER (PARTITION BY sensor_id ORDER BY reading_ts) AS prev_reading_ts
    FROM SENSOR_READINGS_CLEAN
)
SELECT
    sensor_id, store_id, equipment_type, zone,
    reading_ts AS event_ts,
    CASE
        WHEN is_alarm AND (prev_is_alarm = FALSE OR prev_is_alarm IS NULL) THEN 'ALARM_ENTERED'
        WHEN NOT is_alarm AND prev_is_alarm THEN 'ALARM_CLEARED'
    END AS event_type,
    temperature_f, set_point_f, deviation_f,
    DATEDIFF('second', prev_reading_ts, reading_ts) AS seconds_since_prev
FROM readings_with_lag
WHERE (is_alarm AND (prev_is_alarm = FALSE OR prev_is_alarm IS NULL))
   OR (NOT is_alarm AND prev_is_alarm);

/* --- 4. Stream on ALARM_EVENTS for the Kafka Sink connector --- */
CREATE STREAM IF NOT EXISTS ALARM_EVENTS_STREAM
    ON DYNAMIC TABLE ALARM_EVENTS
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream for outbound Kafka sink connector';

/* --- Verify --- */
SHOW DYNAMIC TABLES IN SCHEMA OF_KAFKA.INGEST;
SHOW STREAMS IN SCHEMA OF_KAFKA.INGEST;
