# Run this script whenever protos/TemperatureService.proto changes.
# Requires: pip install grpcio-tools

import subprocess, sys, os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

subprocess.run(
    [
        sys.executable, '-m', 'grpc_tools.protoc',
        '-I', 'protos',
        '--python_out', '.',
        '--grpc_python_out', '.',
        'protos/TemperatureService.proto',
    ],
    check=True,
)
print('Proto files regenerated.')
