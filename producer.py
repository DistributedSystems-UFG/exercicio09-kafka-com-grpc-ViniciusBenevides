# ---------------------------------------------------------------------------
# producer.py  –  (1) Sensor simulator
#
# Simulates multiple temperature sensors. Each sensor starts at a random
# baseline and drifts over time. A new Kafka event is published to
# TOPIC_RAW only when the temperature change since the last published
# reading exceeds THRESHOLD (significant-variation rule).
#
# Usage:
#   python3 producer.py
# ---------------------------------------------------------------------------

import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from const import BROKER_ADDR, BROKER_PORT, TOPIC_RAW

SENSORS   = ['sensor-A', 'sensor-B', 'sensor-C']
THRESHOLD = 0.5   # minimum change (°C) required to publish an event
INTERVAL  = 2.0   # seconds between sensor polling cycles

BROKER = BROKER_ADDR + ':' + BROKER_PORT

producer = KafkaProducer(bootstrap_servers=[BROKER])

# Initialise each sensor at a random baseline temperature
current_temp = {s: round(random.uniform(18.0, 32.0), 2) for s in SENSORS}
last_published = dict(current_temp)   # last value that was actually sent

print(f'[sensor] Publishing to topic "{TOPIC_RAW}" (threshold={THRESHOLD}°C)')
print(f'[sensor] Initial temperatures: {current_temp}\n')

while True:
    for sensor_id in SENSORS:
        # Simulate a small random walk
        delta = random.uniform(-1.5, 1.5)
        new_temp = round(current_temp[sensor_id] + delta, 2)
        current_temp[sensor_id] = new_temp

        if abs(new_temp - last_published[sensor_id]) >= THRESHOLD:
            event = {
                'sensor_id':   sensor_id,
                'temperature': new_temp,
                'timestamp':   datetime.now(timezone.utc).isoformat(),
            }
            producer.send(
                TOPIC_RAW,
                value=json.dumps(event).encode('utf-8'),
                key=sensor_id.encode('utf-8'),
            )
            last_published[sensor_id] = new_temp
            print(f'[sensor] {sensor_id}: {new_temp:+.2f}°C  (delta {delta:+.2f})')

    producer.flush()
    time.sleep(INTERVAL)
