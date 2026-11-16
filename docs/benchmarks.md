# Benchmarks

## TCP relay comparison

Run the sustained loopback comparison with:

```sh
cargo build --release
python3 benches/relay_bench.py
```

The script uses one local Python echo server and compares Flux buffer sizes (8, 32, 64, and 256 KiB) with socat. It suppresses expected disconnect errors during socket cleanup and pauses briefly after each proxy process exits. Each round uses eight concurrent clients, each sending and receiving 32 MiB continuously. The script verifies every returned byte and reports useful client payload MiB/s. Three rounds run by default; set `FLUX_BENCH_ROUNDS` to increase the count. Tune load with `FLUX_BENCH_CONCURRENCY`, `FLUX_BENCH_MIB_PER_CLIENT`, and `FLUX_BENCH_CHUNK_KIB`. For long runs on a host reporting socket buffer exhaustion, lower concurrency/payload and raise `FLUX_BENCH_COOLDOWN`.

A user-reported run on 2026-11-17 measured median throughput of 1,662.8 MiB/s (8 KiB), 1,837.0 MiB/s (32 KiB), 1,817.3 MiB/s (64 KiB), 1,776.6 MiB/s (256 KiB), and 1,623.7 MiB/s (socat). Flux's 32 KiB setting led that run by about 13%. These are loopback measurements, not network benchmarks or general performance claims. The benchmark host's exact model and memory size were not captured; the OS kernel reported Darwin 25.1.0 on ARM64.

The Python echo server can limit throughput. For useful comparisons, record the exact machine model, OS/kernel, power mode, background load, command versions, and complete run output. Repeat on physical interfaces and include slower receivers before drawing production conclusions. Use Instruments Time Profiler on macOS or `perf` on Linux to identify CPU hotspots before changing the data path.

## Lower-load repeat

A second user run on 2026-11-17 used four concurrent clients sending 16 MiB each for seven rounds, with a one-second process cooldown. Median useful throughput was 972.7 MiB/s (8 KiB), 1,034.5 MiB/s (32 KiB), 1,052.9 MiB/s (64 KiB), 935.7 MiB/s (256 KiB), and 988.1 MiB/s (socat). The 64 KiB Flux setting led socat by about 6.6% in this run. Individual results varied substantially (for example, Flux 64 KiB ranged from 887.9 to 1,268.8 MiB/s), so this small local benchmark does not establish a stable 64 KiB advantage over 32 KiB. Keep the 32 KiB default until repeated, controlled runs show a meaningful win.
