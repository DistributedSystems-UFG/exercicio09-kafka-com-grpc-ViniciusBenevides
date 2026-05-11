# ---------------------------------------------------------------------------
# service.py  –  (3) Consumer + gRPC Web Service  (final pipeline node)
#
# Two responsibilities run concurrently:
#   A) Kafka consumer thread  – reads processed snapshots from TOPIC_PROCESSED
#      and stores them in an in-memory database (dict).
#   B) gRPC server            – answers client queries from the in-memory DB.
#
# Usage:
#   python3 service.py
# ---------------------------------------------------------------------------

import json
import threading
import logging
from concurrent import futures
from collections import defaultdict
from datetime import datetime, timezone

import grpc
from kafka import KafkaConsumer

import TemperatureService_pb2      as pb2
import TemperatureService_pb2_grpc as pb2_grpc
from const import BROKER_ADDR, BROKER_PORT, TOPIC_PROCESSED, GRPC_PORT

BROKER = BROKER_ADDR + ':' + BROKER_PORT

# ---------------------------------------------------------------------------
# In-memory database
#   latest  : sensor_id -> latest ProcessedReading dict
#   history : sensor_id -> list of ProcessedReading dicts (all time)
# ---------------------------------------------------------------------------
db_lock = threading.Lock()
latest:  dict[str, dict] = {}
history: dict[str, list] = defaultdict(list)


# ---------------------------------------------------------------------------
# A) Kafka consumer thread
# ---------------------------------------------------------------------------

def kafka_consumer_loop() -> None:
    consumer = KafkaConsumer(
        TOPIC_PROCESSED,
        bootstrap_servers=[BROKER],
        auto_offset_reset='earliest',
        group_id='service-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )
    print(f'[service/kafka] Consuming "{TOPIC_PROCESSED}"...')

    for msg in consumer:
        snapshot  = msg.value
        sensor_id = snapshot['sensor_id']

        with db_lock:
            latest[sensor_id] = snapshot
            history[sensor_id].append(snapshot)

        print(
            f'[service/kafka] stored {sensor_id}: '
            f'avg={snapshot["avg_temp"]:.2f}°C  n={snapshot["sample_count"]}'
        )


# ---------------------------------------------------------------------------
# B) gRPC servicer
# ---------------------------------------------------------------------------

def _to_pb(snap: dict) -> pb2.ProcessedReading:
    return pb2.ProcessedReading(
        sensor_id    = snap['sensor_id'],
        avg_temp     = snap['avg_temp'],
        min_temp     = snap['min_temp'],
        max_temp     = snap['max_temp'],
        sample_count = snap['sample_count'],
        window_start = snap['window_start'],
        window_end   = snap['window_end'],
        timestamp    = snap['timestamp'],
    )


class TemperatureServicer(pb2_grpc.TemperatureServiceServicer):

    def GetLatestReading(self, request, context):
        with db_lock:
            snap = latest.get(request.sensor_id)
        if snap is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f'No data for sensor "{request.sensor_id}"')
            return pb2.ProcessedReading()
        return _to_pb(snap)

    def GetHistory(self, request, context):
        with db_lock:
            snaps = list(history.get(request.sensor_id, []))
        result = pb2.ReadingHistory()
        for snap in snaps:
            result.readings.append(_to_pb(snap))
        return result

    def ListSensors(self, request, context):
        with db_lock:
            ids = list(latest.keys())
        return pb2.SensorList(sensor_ids=ids)

    def GetOverallStats(self, request, context):
        with db_lock:
            snaps = list(history.get(request.sensor_id, []))
        if not snaps:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f'No data for sensor "{request.sensor_id}"')
            return pb2.OverallStats()

        all_avgs  = [s['avg_temp']     for s in snaps]
        all_mins  = [s['min_temp']     for s in snaps]
        all_maxs  = [s['max_temp']     for s in snaps]
        all_counts= [s['sample_count'] for s in snaps]

        return pb2.OverallStats(
            sensor_id     = request.sensor_id,
            global_avg    = round(sum(all_avgs) / len(all_avgs), 2),
            global_min    = round(min(all_mins), 2),
            global_max    = round(max(all_maxs), 2),
            total_samples = sum(all_counts),
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def serve() -> None:
    # Start Kafka consumer in a daemon thread
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()

    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_TemperatureServiceServicer_to_server(TemperatureServicer(), server)
    server.add_insecure_port(f'[::]:{GRPC_PORT}')
    server.start()
    print(f'[service/grpc] Listening on port {GRPC_PORT}')
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
