# ---------------------------------------------------------------------------
# processor.py  –  (2) Consumer / Producer  (intermediate pipeline node)
#
# Consumes raw sensor readings from TOPIC_RAW.
# Maintains a per-sensor sliding window of the last WINDOW_HOURS hours.
# After each new reading it publishes a ProcessedReading snapshot
# (avg / min / max over the window) to TOPIC_PROCESSED.
#
# Usage:
#   python3 processor.py
# ---------------------------------------------------------------------------

import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
from const import BROKER_ADDR, BROKER_PORT, TOPIC_RAW, TOPIC_PROCESSED

WINDOW_HOURS = 2   # size of the sliding average window

BROKER = BROKER_ADDR + ':' + BROKER_PORT

consumer = KafkaConsumer(
    TOPIC_RAW,
    bootstrap_servers=[BROKER],
    auto_offset_reset='earliest',
    group_id='processor-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)

producer = KafkaProducer(bootstrap_servers=[BROKER])

# sensor_id -> deque of (datetime, float) sorted by time
windows: dict[str, deque] = defaultdict(deque)

print(f'[processor] Consuming "{TOPIC_RAW}" → publishing to "{TOPIC_PROCESSED}"')
print(f'[processor] Sliding window: {WINDOW_HOURS} hour(s)\n')


def publish_snapshot(sensor_id: str) -> None:
    readings = windows[sensor_id]
    temps = [t for _, t in readings]
    avg   = round(sum(temps) / len(temps), 2)

    snapshot = {
        'sensor_id':    sensor_id,
        'avg_temp':     avg,
        'min_temp':     round(min(temps), 2),
        'max_temp':     round(max(temps), 2),
        'sample_count': len(temps),
        'window_start': readings[0][0].isoformat(),
        'window_end':   readings[-1][0].isoformat(),
        'timestamp':    datetime.now(timezone.utc).isoformat(),
    }
    producer.send(
        TOPIC_PROCESSED,
        value=json.dumps(snapshot).encode('utf-8'),
        key=sensor_id.encode('utf-8'),
    )
    producer.flush()
    print(
        f'[processor] {sensor_id}: avg={avg:.2f}°C '
        f'[{snapshot["min_temp"]:.2f}, {snapshot["max_temp"]:.2f}] '
        f'n={len(temps)}'
    )


for msg in consumer:
    event     = msg.value
    sensor_id = event['sensor_id']
    temp      = event['temperature']
    ts        = datetime.fromisoformat(event['timestamp'])

    # Append new reading
    windows[sensor_id].append((ts, temp))

    # Evict readings that have fallen outside the time window
    cutoff = ts - timedelta(hours=WINDOW_HOURS)
    while windows[sensor_id] and windows[sensor_id][0][0] < cutoff:
        windows[sensor_id].popleft()

    publish_snapshot(sensor_id)
