"""P4 worker restart probe: exact encoder output survives Redis queue and a new connection.

This verifies broker-held bytes across a consumer restart; it does not restart the shared Redis
server or claim Redis AOF/fsync durability. Only unique synthetic queue keys are created/deleted.
"""
import os
from pathlib import Path
import socket
import subprocess
import time

BASE = Path(__file__).resolve().parent
OUT = BASE / 'results/push-ably'

class Redis:
    def __init__(self):
        self.socket = socket.create_connection(('127.0.0.1', 16391))
        self.stream = self.socket.makefile('rb')
        assert self.command(b'SELECT', b'13') == b'OK'
    def command(self, *args):
        encoded = b'*' + str(len(args)).encode() + b'\r\n'
        encoded += b''.join(b'$' + str(len(arg)).encode() + b'\r\n' + arg + b'\r\n' for arg in args)
        self.socket.sendall(encoded)
        line = self.stream.readline()
        kind, data = line[:1], line[1:-2]
        if kind == b':': return int(data)
        if kind == b'+': return data
        if kind == b'$':
            length = int(data)
            if length < 0: return None
            value = self.stream.read(length)
            assert self.stream.read(2) == b'\r\n'
            return value
        raise AssertionError(line)
    def close(self):
        self.stream.close()
        self.socket.close()

for variant, executable in [('baseline', BASE / 'baseline-binaries/p4-envelope-worker-restart'), ('after', BASE / 'target-p4-baseline/release/examples/batch_envelope_audit')]:
    with (OUT / f'p4-redis-{variant}.csv').open('w') as result:
        for payload_bytes, recipients in [(256, 1000), (65536, 100)]:
            encoded_file = OUT / f'p4-{variant}-{payload_bytes}-encoded.json'
            subprocess.run([str(executable), 'emit', str(payload_bytes), str(recipients), str(encoded_file)], check=True)
            encoded = encoded_file.read_bytes()
            for rep in range(7):
                key = f'p4-audit:{os.getpid()}:{variant}:{payload_bytes}:{rep}'.encode()
                producer = Redis()
                start = time.perf_counter_ns()
                assert producer.command(b'RPUSH', key, encoded) == 1
                producer.close()
                consumer = Redis()
                assert consumer.command(b'LLEN', key) == 1
                restored = consumer.command(b'LPOP', key)
                assert consumer.command(b'LLEN', key) == 0
                consumer.close()
                elapsed_us = (time.perf_counter_ns() - start) // 1000
                assert restored == encoded
                restored_file = OUT / f'p4-{variant}-{payload_bytes}-restored.json'
                restored_file.write_bytes(restored)
                subprocess.run([str(executable), 'verify', str(payload_bytes), str(recipients), str(restored_file)], check=True)
                result.write(f'p4redis,payload_bytes={payload_bytes},recipients={recipients},rep={rep},us={elapsed_us},queue_bytes={len(encoded)},roundtrip_bytes={2*len(encoded)},recovered_jobs={recipients}\n')
                result.flush()
    print('p4redis', variant, 'complete', flush=True)
