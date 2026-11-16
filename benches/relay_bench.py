#!/usr/bin/env python3
"""Sustained TCP relay comparison: Flux buffer sweep versus socat."""
import asyncio, os, socket, subprocess, time, statistics
from pathlib import Path

FLUX = os.environ.get('FLUX_BIN', str(Path(__file__).resolve().parents[1] / 'target' / 'release' / 'flux'))
CONCURRENCY = 8
MIB_PER_CLIENT = 32
CHUNK = 64 * 1024
ROUNDS = int(os.environ.get("FLUX_BENCH_ROUNDS", "3"))
BUFFERS = (8192, 32768, 65536, 262144)


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

async def echo(reader, writer):
    writer.get_extra_info('socket').setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        while data := await reader.read(CHUNK):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def wait_ready(port, process):
    for _ in range(200):
        if process.poll() is not None:
            raise RuntimeError(f'proxy exited ({process.returncode}): {process.stderr.read().decode()}')
        try:
            _, w = await asyncio.open_connection('127.0.0.1', port)
            w.close(); await w.wait_closed(); return
        except OSError:
            await asyncio.sleep(.025)
    raise RuntimeError('proxy did not start')

async def one_client(port, byte_count):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    payload = b'F' * CHUNK
    async def send_all():
        remaining = byte_count
        while remaining:
            size = min(CHUNK, remaining)
            writer.write(payload[:size])
            await writer.drain()
            remaining -= size
        writer.write_eof()
    async def receive_all():
        received = 0
        while received < byte_count:
            data = await reader.read(min(CHUNK, byte_count - received))
            if not data: raise RuntimeError('unexpected EOF')
            if data != payload[:len(data)]: raise RuntimeError('echo mismatch')
            received += len(data)
        return received
    start = time.perf_counter()
    sent, received = await asyncio.gather(send_all(), receive_all())
    elapsed = time.perf_counter() - start
    assert received == byte_count
    writer.close(); await writer.wait_closed()
    return elapsed

async def run_round(label, command, echo_port):
    port = free_port()
    proc = subprocess.Popen(command(port, echo_port), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        await wait_ready(port, proc)
        await one_client(port, 2 * 1024 * 1024)
        start = time.perf_counter()
        durations = await asyncio.gather(*(one_client(port, MIB_PER_CLIENT * 1024 * 1024) for _ in range(CONCURRENCY)))
        wall = time.perf_counter() - start
        return CONCURRENCY * MIB_PER_CLIENT / wall, statistics.median(durations), wall
    finally:
        proc.terminate()
        try: proc.wait(timeout=3)
        except subprocess.TimeoutExpired: proc.kill(); proc.wait()

async def main():
    echo_server = await asyncio.start_server(echo, '127.0.0.1', 0)
    echo_port = echo_server.sockets[0].getsockname()[1]
    specs = {f'Flux {b // 1024} KiB': lambda p, e, b=b: [FLUX, 'proxy', 'tcp', f'127.0.0.1:{p}', f'127.0.0.1:{e}', '--buffer-size', str(b)] for b in BUFFERS}
    specs['socat'] = lambda p, e: ['socat', f'TCP-LISTEN:{p},bind=127.0.0.1,reuseaddr,fork,nodelay', f'TCP:127.0.0.1:{e},nodelay']
    results = {name: [] for name in specs}
    try:
        for i in range(ROUNDS):
            order = list(specs) if i % 2 == 0 else list(reversed(specs))
            for name in order:
                mbps, median, wall = await run_round(name, specs[name], echo_port)
                results[name].append(mbps)
                print(f'round {i+1} {name}: {mbps:.1f} MiB/s; median client {median:.3f}s; wall {wall:.3f}s', flush=True)
    finally:
        echo_server.close(); await echo_server.wait_closed()
    print('\nMedian throughput across rounds:')
    for name, values in results.items():
        print(f'{name}: {statistics.median(values):.1f} MiB/s; runs=' + ','.join(f'{v:.1f}' for v in values))
    print(f'Workload: continuous full-duplex echo; {CONCURRENCY} concurrent clients x {MIB_PER_CLIENT} MiB each; {ROUNDS} rounds; payload verified; client-to-proxy useful payload counted once.')

asyncio.run(main())
