/* =============================================================================
   05_seed_data.sql — Generate sample sensor readings directly into the raw table
   -----------------------------------------------------------------------------
   Run as OF_KAFKA_ADMIN.

   Instead of pushing through Kafka (which requires a producer client),
   this inserts synthetic sensor data directly into SENSOR_READINGS_RAW
   to simulate what the Kafka consumer connector would land.

   50 stores × 6 sensors each = 300 sensors.
   20 readings per sensor at 30-second intervals = 6,000 rows.

   Three alarm scenarios baked in:
     - Store 4421, FRZ-4421-003: compressor failing, temp climbing -2→+18°F
     - Store 6010, REF-6010-002: door stuck open, rapid spike to 52°F
     - Store 7345, FRZ-7345-001: intermittent cycling, alarm every few minutes
   ============================================================================= */

USE ROLE OF_KAFKA_ADMIN;
USE DATABASE OF_KAFKA;
USE SCHEMA INGEST;
USE WAREHOUSE OF_KAFKA_WH;

/* --- Create the raw table matching Kafka connector schema --- */
CREATE ICEBERG TABLE IF NOT EXISTS SENSOR_READINGS_RAW (
    record_content  VARIANT,
    record_metadata VARIANT
)
COMMENT = 'Raw sensor readings — populated by Kafka connector or seed script';

/* --- Generate normal readings for 300 sensors × 20 intervals --- */
INSERT INTO SENSOR_READINGS_RAW (record_content, record_metadata)
WITH stores AS (
    SELECT store_id
    FROM (VALUES
        (4421),(4590),(5102),(5234),(5501),(6010),(6223),(7101),(7345),(8002),
        (3101),(3205),(3310),(3422),(3538),(3641),(3755),(3869),(3972),(4088),
        (4191),(4299),(4405),(4512),(4628),(4733),(4847),(4956),(5068),(5173),
        (5289),(5394),(5508),(5613),(5727),(5834),(5948),(6055),(6169),(6274),
        (6388),(6493),(6607),(6714),(6828),(6935),(7049),(7156),(7268),(7377)
    ) AS t(store_id)
),
sensors_per_store AS (
    SELECT
        s.store_id,
        eq.sensor_num,
        eq.equipment_type,
        eq.zone,
        eq.set_point_f
    FROM stores s
    CROSS JOIN (VALUES
        (1, 'FREEZER',      'FROZEN_FOODS',  0.0),
        (2, 'FREEZER',      'ICE_CREAM',    -5.0),
        (3, 'FREEZER',      'MEAT_FREEZER', -10.0),
        (4, 'REFRIGERATOR', 'DAIRY',         36.0),
        (5, 'REFRIGERATOR', 'PRODUCE',       38.0),
        (6, 'REFRIGERATOR', 'DELI',          34.0)
    ) AS eq(sensor_num, equipment_type, zone, set_point_f)
),
sensor_list AS (
    SELECT
        store_id,
        CASE WHEN equipment_type = 'FREEZER' THEN 'FRZ' ELSE 'REF' END
            || '-' || store_id || '-' || LPAD(sensor_num, 3, '0')
            AS sensor_id,
        equipment_type,
        zone,
        set_point_f
    FROM sensors_per_store
),
intervals AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS interval_num
    FROM TABLE(GENERATOR(ROWCOUNT => 20))
),
normal_readings AS (
    SELECT
        sl.sensor_id,
        sl.store_id,
        sl.equipment_type,
        sl.zone,
        sl.set_point_f,
        ROUND(sl.set_point_f + UNIFORM(-1.5, 1.5, RANDOM()), 1) AS temperature_f,
        FALSE AS door_open,
        DATEADD('second', -i.interval_num * 30,
                CURRENT_TIMESTAMP()) AS reading_ts,
        i.interval_num
    FROM sensor_list sl
    CROSS JOIN intervals i
    WHERE NOT (sl.store_id = 4421 AND sl.sensor_id = 'FRZ-4421-003')
      AND NOT (sl.store_id = 6010 AND sl.sensor_id = 'REF-6010-002')
      AND NOT (sl.store_id = 7345 AND sl.sensor_id = 'FRZ-7345-001')
),

/* --- Alarm scenario 1: Store 4421 compressor failing (gradual climb) --- */
alarm_4421 AS (
    SELECT
        'FRZ-4421-003' AS sensor_id,
        4421 AS store_id,
        'FREEZER' AS equipment_type,
        'MEAT_FREEZER' AS zone,
        -10.0 AS set_point_f,
        ROUND(-10.0 + (i.interval_num * 1.4) + UNIFORM(-0.3, 0.3, RANDOM()), 1)
            AS temperature_f,
        FALSE AS door_open,
        DATEADD('second', -i.interval_num * 30, CURRENT_TIMESTAMP()) AS reading_ts,
        i.interval_num
    FROM intervals i
),

/* --- Alarm scenario 2: Store 6010 door stuck open (rapid spike) --- */
alarm_6010 AS (
    SELECT
        'REF-6010-002' AS sensor_id,
        6010 AS store_id,
        'REFRIGERATOR' AS equipment_type,
        'PRODUCE' AS zone,
        38.0 AS set_point_f,
        CASE
            WHEN i.interval_num < 10 THEN ROUND(38.0 + UNIFORM(-1.0, 1.0, RANDOM()), 1)
            ELSE ROUND(38.0 + ((i.interval_num - 9) * 1.8) + UNIFORM(-0.5, 0.5, RANDOM()), 1)
        END AS temperature_f,
        CASE WHEN i.interval_num >= 10 THEN TRUE ELSE FALSE END AS door_open,
        DATEADD('second', -i.interval_num * 30, CURRENT_TIMESTAMP()) AS reading_ts,
        i.interval_num
    FROM intervals i
),

/* --- Alarm scenario 3: Store 7345 intermittent cycling --- */
alarm_7345 AS (
    SELECT
        'FRZ-7345-001' AS sensor_id,
        7345 AS store_id,
        'FREEZER' AS equipment_type,
        'FROZEN_FOODS' AS zone,
        0.0 AS set_point_f,
        CASE
            WHEN MOD(i.interval_num, 4) IN (0, 1)
                THEN ROUND(0.0 + UNIFORM(-1.0, 1.0, RANDOM()), 1)
            ELSE ROUND(0.0 + UNIFORM(6.0, 9.0, RANDOM()), 1)
        END AS temperature_f,
        FALSE AS door_open,
        DATEADD('second', -i.interval_num * 30, CURRENT_TIMESTAMP()) AS reading_ts,
        i.interval_num
    FROM intervals i
),

all_readings AS (
    SELECT * FROM normal_readings
    UNION ALL SELECT * FROM alarm_4421
    UNION ALL SELECT * FROM alarm_6010
    UNION ALL SELECT * FROM alarm_7345
)
SELECT
    OBJECT_CONSTRUCT(
        'sensor_id',      sensor_id,
        'store_id',        store_id,
        'equipment_type',  equipment_type,
        'zone',            zone,
        'temperature_f',   temperature_f,
        'set_point_f',     set_point_f,
        'door_open',       door_open,
        'reading_ts',      reading_ts::STRING
    ) AS record_content,
    OBJECT_CONSTRUCT(
        'CreateTime', CURRENT_TIMESTAMP()::STRING,
        'topic',      'cold_chain.sensor_readings',
        'partition',  MOD(ABS(HASH(sensor_id)), 3),
        'offset',     ROW_NUMBER() OVER (ORDER BY reading_ts, sensor_id)
    ) AS record_metadata
FROM all_readings
ORDER BY reading_ts, sensor_id;

/* --- Verify --- */
SELECT COUNT(*) AS total_readings FROM SENSOR_READINGS_RAW;
SELECT
    record_content:equipment_type::STRING AS equipment_type,
    COUNT(*) AS cnt
FROM SENSOR_READINGS_RAW
GROUP BY 1;
