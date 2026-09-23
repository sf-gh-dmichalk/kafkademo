#!/usr/bin/env python3
"""
produce_sensor_data.py — Generate cold chain sensor readings to MSK topic.

Produces 6,000 JSON messages (50 stores × 6 sensors × 20 readings at 30s intervals)
with 3 baked-in alarm scenarios:
  - Store 4421: Freezer compressor failing (gradual climb -10°F → +18°F)
  - Store 6010: Walk-in cooler door stuck open (rapid spike to 52°F)
  - Store 7345: Intermittent compressor cycling (alarm every few minutes)

Usage:
  python3 produce_sensor_data.py [--brokers BOOTSTRAP_SERVERS] [--topic TOPIC]

Requires: confluent-kafka (pip3 install confluent-kafka)
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone, timedelta

STORES = [
    4421, 4590, 5102, 5234, 5501, 6010, 6223, 7101, 7345, 8002,
    3101, 3205, 3310, 3422, 3538, 3641, 3755, 3869, 3972, 4088,
    4191, 4299, 4405, 4512, 4628, 4733, 4847, 4956, 5068, 5173,
    5289, 5394, 5508, 5613, 5727, 5834, 5948, 6055, 6169, 6274,
    6388, 6493, 6607, 6714, 6828, 6935, 7049, 7156, 7268, 7377,
]

SENSORS_PER_STORE = [
    (1, "FREEZER",      "FROZEN_FOODS",  0.0),
    (2, "FREEZER",      "ICE_CREAM",    -5.0),
    (3, "FREEZER",      "MEAT_FREEZER", -10.0),
    (4, "REFRIGERATOR", "DAIRY",         36.0),
    (5, "REFRIGERATOR", "PRODUCE",       38.0),
    (6, "REFRIGERATOR", "DELI",          34.0),
]

ALARM_SCENARIOS = {
    ("4421", "FRZ-4421-003"),
    ("6010", "REF-6010-002"),
    ("7345", "FRZ-7345-001"),
}

NUM_INTERVALS = 20
INTERVAL_SECS = 30


def sensor_id(store_id, num, eq_type):
    prefix = "FRZ" if eq_type == "FREEZER" else "REF"
    return f"{prefix}-{store_id}-{num:03d}"


def normal_reading(set_point):
    return round(set_point + random.uniform(-1.5, 1.5), 1)


def alarm_4421_reading(interval):
    """Compressor failing: gradual climb from -10°F."""
    return round(-10.0 + (interval * 1.4) + random.uniform(-0.3, 0.3), 1)


def alarm_6010_reading(interval):
    """Door stuck open: normal first 10, then rapid spike."""
    if interval < 10:
        return round(38.0 + random.uniform(-1.0, 1.0), 1)
    return round(38.0 + ((interval - 9) * 1.8) + random.uniform(-0.5, 0.5), 1)


def alarm_7345_reading(interval):
    """Intermittent cycling: alarm every other pair of readings."""
    if interval % 4 in (0, 1):
        return round(0.0 + random.uniform(-1.0, 1.0), 1)
    return round(0.0 + random.uniform(6.0, 9.0), 1)


def generate_readings():
    now = datetime.now(timezone.utc)
    messages = []

    for store_id in STORES:
        for num, eq_type, zone, set_point in SENSORS_PER_STORE:
            sid = sensor_id(store_id, num, eq_type)
            is_alarm_sensor = (str(store_id), sid) in ALARM_SCENARIOS

            for i in range(NUM_INTERVALS):
                ts = now - timedelta(seconds=(NUM_INTERVALS - 1 - i) * INTERVAL_SECS)

                if is_alarm_sensor and sid == "FRZ-4421-003":
                    temp = alarm_4421_reading(i)
                    door = False
                elif is_alarm_sensor and sid == "REF-6010-002":
                    temp = alarm_6010_reading(i)
                    door = i >= 10
                elif is_alarm_sensor and sid == "FRZ-7345-001":
                    temp = alarm_7345_reading(i)
                    door = False
                else:
                    temp = normal_reading(set_point)
                    door = False

                msg = {
                    "sensor_id": sid,
                    "store_id": store_id,
                    "equipment_type": eq_type,
                    "zone": zone,
                    "temperature_f": temp,
                    "set_point_f": set_point,
                    "door_open": door,
                    "reading_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                messages.append((sid, msg))

    random.shuffle(messages)
    return messages


def delivery_report(err, msg):
    if err:
        print(f"  FAILED: {err}")


def main():
    parser = argparse.ArgumentParser(description="Produce cold chain sensor data to MSK")
    parser.add_argument("--brokers", default=(
        "b-1.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096,"
        "b-2.dmichalkofkafkamsk.34p6ti.c2.kafka.us-east-1.amazonaws.com:9096"
    ))
    parser.add_argument("--topic", default="cold_chain.sensor_readings")
    parser.add_argument("--username", default="dmichalk-of-kafka")
    parser.add_argument("--password", default="D3m0-Kafka-2026!")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": args.brokers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": args.username,
        "sasl.password": args.password,
        "linger.ms": 50,
        "batch.num.messages": args.batch_size,
    }

    producer = Producer(conf)
    messages = generate_readings()
    total = len(messages)

    print(f"Producing {total} messages to {args.topic}...")
    start = time.time()

    for i, (key, msg) in enumerate(messages, 1):
        producer.produce(
            args.topic,
            key=key.encode("utf-8"),
            value=json.dumps(msg).encode("utf-8"),
            callback=delivery_report,
        )
        if i % args.batch_size == 0:
            producer.flush()
            elapsed = time.time() - start
            rate = i / elapsed if elapsed > 0 else 0
            print(f"  {i}/{total} sent ({rate:.0f} msg/s)")

    producer.flush()
    elapsed = time.time() - start
    print(f"Done: {total} messages in {elapsed:.1f}s ({total/elapsed:.0f} msg/s)")


if __name__ == "__main__":
    main()
