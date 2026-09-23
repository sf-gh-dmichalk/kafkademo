/* =============================================================================
   04_dynamic_tables.sql — Dynamic Iceberg tables for cold chain pipeline
   -----------------------------------------------------------------------------
   Run as OF_KAFKA_ADMIN after the Kafka consumer connector is running
   and SENSOR_READINGS_RAW has data.

   Three dynamic tables:
     1. SENSOR_READINGS_CLEAN  — deduplicate, cast, compute deviation + alarm
     2. EQUIPMENT_STATUS       — rolling 5-min window, one row per sensor
     3. ALARM_EVENTS           — alarm state transitions (CDC for outbound Kafka)
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
   Deduplicates by sensor_id + reading_ts, casts JSON fields,
   computes deviation from set point, flags alarms.
   Threshold: freezers >5°F deviation, refrigerators >3°F deviation.
   Target lag: 1 minute.
   ----------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC ICEBERG TABLE SENSOR_READINGS_CLEAN
    TARGET_LAG = '1 minute'
    WAREHOUSE = OF_KAFKA_WH
    AS
SELECT
    record_content:sensor_id::STRING           AS sensor_id,
    record_content:store_id::INT               AS store_id,
    record_content:equipment_type::STRING       AS equipment_type,
    record_content:zone::STRING                 AS zone,
    record_content:temperature_f::FLOAT         AS temperature_f,
    record_content:set_point_f::FLOAT           AS set_point_f,
    record_content:door_open::BOOLEAN           AS door_open,
    record_content:reading_ts::TIMESTAMP_NTZ    AS reading_ts,
    temperature_f - set_point_f                 AS deviation_f,
    CASE
        WHEN equipment_type = 'FREEZER'      AND ABS(temperature_f - set_point_f) > 5 THEN TRUE
        WHEN equipment_type = 'REFRIGERATOR' AND ABS(temperature_f - set_point_f) > 3 THEN TRUE
        ELSE FALSE
    END                                         AS is_alarm,
    CURRENT_TIMESTAMP()                         AS processed_at
FROM SENSOR_READINGS_RAW
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY record_content:sensor_id::STRING,
                 record_content:reading_ts::TIMESTAMP_NTZ
    ORDER BY record_metadata:CreateTime DESC
) = 1;

/* --- 2. EQUIPMENT_STATUS ---
   Rolling 5-minute window per sensor. One row per sensor = current state.
   Target lag: 2 minutes.
   ----------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC ICEBERG TABLE EQUIPMENT_STATUS
    TARGET_LAG = '2 minutes'
    WAREHOUSE = OF_KAFKA_WH
    AS
SELECT
    sensor_id,
    store_id,
    equipment_type,
    zone,
    COUNT(*)                                             AS reading_count_5min,
    ROUND(AVG(temperature_f), 1)                         AS avg_temp_f,
    ROUND(MIN(temperature_f), 1)                         AS min_temp_f,
    ROUND(MAX(temperature_f), 1)                         AS max_temp_f,
    ROUND(AVG(deviation_f), 1)                           AS avg_deviation_f,
    ROUND(MAX(ABS(deviation_f)), 1)                      AS max_abs_deviation_f,
    SUM(CASE WHEN is_alarm THEN 1 ELSE 0 END)           AS alarm_count_5min,
    SUM(CASE WHEN door_open THEN 1 ELSE 0 END)          AS door_open_count_5min,
    MAX(reading_ts)                                      AS last_reading_ts,
    CASE
        WHEN SUM(CASE WHEN is_alarm THEN 1 ELSE 0 END) > 0 THEN 'ALARM'
        WHEN MAX(ABS(deviation_f)) > 2                      THEN 'WARNING'
        ELSE 'NORMAL'
    END                                                  AS status,
    CURRENT_TIMESTAMP()                                  AS refreshed_at
FROM SENSOR_READINGS_CLEAN
WHERE reading_ts >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
GROUP BY sensor_id, store_id, equipment_type, zone;

/* --- 3. ALARM_EVENTS ---
   Detects alarm state transitions: when is_alarm flips from FALSE→TRUE
   (alarm entered) or TRUE→FALSE (alarm cleared). Each transition is a
   new row — this table is the CDC source for the outbound Kafka connector.
   Target lag: 2 minutes.
   ----------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC ICEBERG TABLE ALARM_EVENTS
    TARGET_LAG = '2 minutes'
    WAREHOUSE = OF_KAFKA_WH
    AS
WITH readings_with_lag AS (
    SELECT
        sensor_id,
        store_id,
        equipment_type,
        zone,
        temperature_f,
        set_point_f,
        deviation_f,
        is_alarm,
        reading_ts,
        LAG(is_alarm) OVER (
            PARTITION BY sensor_id ORDER BY reading_ts
        ) AS prev_is_alarm,
        LAG(reading_ts) OVER (
            PARTITION BY sensor_id ORDER BY reading_ts
        ) AS prev_reading_ts
    FROM SENSOR_READINGS_CLEAN
)
SELECT
    sensor_id,
    store_id,
    equipment_type,
    zone,
    reading_ts                                           AS event_ts,
    CASE
        WHEN is_alarm AND (prev_is_alarm = FALSE OR prev_is_alarm IS NULL)
            THEN 'ALARM_ENTERED'
        WHEN NOT is_alarm AND prev_is_alarm
            THEN 'ALARM_CLEARED'
    END                                                  AS event_type,
    temperature_f,
    set_point_f,
    deviation_f,
    DATEDIFF('second', prev_reading_ts, reading_ts)      AS seconds_since_prev,
    CURRENT_TIMESTAMP()                                  AS detected_at
FROM readings_with_lag
WHERE (is_alarm AND (prev_is_alarm = FALSE OR prev_is_alarm IS NULL))
   OR (NOT is_alarm AND prev_is_alarm);

/* --- Verify --- */
SHOW DYNAMIC TABLES IN SCHEMA OF_KAFKA.INGEST;
