# ---------------------------------------------------------------------------
# client.py  –  (4) gRPC client
#
# Demonstrates all four RPC methods exposed by service.py.
# Run this after the full pipeline is up (producer → processor → service).
#
# Usage:
#   python3 client.py [sensor_id]
#   python3 client.py              # uses 'sensor-A' by default
# ---------------------------------------------------------------------------

import sys
import logging

import grpc

import TemperatureService_pb2      as pb2
import TemperatureService_pb2_grpc as pb2_grpc
from const import GRPC_HOST, GRPC_PORT

DEFAULT_SENSOR = 'sensor-A'


def run(sensor_id: str) -> None:
    target = f'{GRPC_HOST}:{GRPC_PORT}'
    print(f'[client] Connecting to gRPC server at {target}\n')

    with grpc.insecure_channel(target) as channel:
        stub = pb2_grpc.TemperatureServiceStub(channel)

        # --- 1. List all sensors known to the service ---
        print('=== ListSensors ===')
        resp = stub.ListSensors(pb2.EmptyMessage())
        if resp.sensor_ids:
            print(f'  Known sensors: {list(resp.sensor_ids)}')
        else:
            print('  (no sensors registered yet)')

        # --- 2. Latest processed reading ---
        print(f'\n=== GetLatestReading ({sensor_id}) ===')
        try:
            resp = stub.GetLatestReading(pb2.SensorRequest(sensor_id=sensor_id))
            print(
                f'  avg={resp.avg_temp:.2f}°C  '
                f'min={resp.min_temp:.2f}°C  '
                f'max={resp.max_temp:.2f}°C  '
                f'samples={resp.sample_count}'
            )
            print(f'  window : {resp.window_start}  →  {resp.window_end}')
            print(f'  produced: {resp.timestamp}')
        except grpc.RpcError as e:
            print(f'  ERROR: {e.details()}')

        # --- 3. Full history ---
        print(f'\n=== GetHistory ({sensor_id}) ===')
        try:
            resp = stub.GetHistory(pb2.SensorRequest(sensor_id=sensor_id))
            readings = resp.readings
            if readings:
                print(f'  {len(readings)} snapshot(s) stored')
                for r in readings[-3:]:   # show last 3 to keep output short
                    print(
                        f'    [{r.timestamp[:19]}]  '
                        f'avg={r.avg_temp:.2f}°C  n={r.sample_count}'
                    )
                if len(readings) > 3:
                    print(f'    ... (showing last 3 of {len(readings)})')
            else:
                print('  (no history yet)')
        except grpc.RpcError as e:
            print(f'  ERROR: {e.details()}')

        # --- 4. Overall aggregated stats ---
        print(f'\n=== GetOverallStats ({sensor_id}) ===')
        try:
            resp = stub.GetOverallStats(pb2.SensorRequest(sensor_id=sensor_id))
            print(
                f'  global_avg={resp.global_avg:.2f}°C  '
                f'global_min={resp.global_min:.2f}°C  '
                f'global_max={resp.global_max:.2f}°C  '
                f'total_samples={resp.total_samples}'
            )
        except grpc.RpcError as e:
            print(f'  ERROR: {e.details()}')


if __name__ == '__main__':
    logging.basicConfig()
    sensor = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SENSOR
    run(sensor)
