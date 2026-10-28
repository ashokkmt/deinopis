# DEINOPIS Benchmark Results

This report summarizes the latest complete Methods A–D runs captured on 2026-10-28. They use synthetic data on one developer machine. These measurements are a project snapshot, not a production capacity guarantee.

## Test machine

| Item | Recorded value |
|---|---|
| Device | Apple M2 |
| Operating system | macOS, Darwin 24.6.0 |
| Architecture | arm64 |
| Go | go1.27.2 |
| Source revision | `747ff08059e909afa7658dd900bfc0fa47f9d1ef` |
| Capture date | 2026-10-28 UTC |

RAM and storage details were not recorded.

## Results at a glance

| Method | Outcome | Main result |
|---|---|---|
| A — pressure matrix | **Passed** | 10,000 events in 39.742 s; 251.6 events/s; 10,000 normalized; none quarantined or pending |
| B — Go benchmarks | **Passed** | Pipeline: 3.941–4.013 ms/op at 1–8 workers; decoder, pack and normalizer benchmarks passed |
| C — stdin | **Completed with a counter discrepancy** | 10,000 accepted and normalized; NDJSON has 10,000 lines; final `Delivered` counter is 9,999 |
| D — HTTP | **Passed** | 10,000 emitted, acknowledged, accepted, normalized and delivered; sender elapsed 63.857 s (156.6 events/s) |

## Method A — pressure matrix

`TestRigorousPressureMatrix` passed for 10,000 events in **39.7425 seconds**, or **251.6 events/s**. All events normalized, with zero quarantined or pending.

| Measurement | Result |
|---|---:|
| Events | 10,000 |
| Elapsed | 39.7425 s |
| Throughput | 251.6 events/s |
| p50 latency | 10.6117 s |
| p95 latency | 35.8959 s |
| p99 latency | 38.9502 s |
| p99.9 latency | 39.6644 s |
| Maximum latency | 39.7354 s |
| Normalized / quarantined / pending | 10,000 / 0 / 0 |

These latency values belong to this saturated, eight-producer test run; they are not service-level objectives or sustained-capacity claims. [Raw pressure report](results/benchmarks/pressure-10k-20261028T022632Z/pressure.txt).

## Method B — Go component benchmarks

All benchmark packages reported `PASS`. Pipeline end-to-end results by worker count:

| Workers | Time/op | Bytes/op | Allocs/op |
|---:|---:|---:|---:|
| 1 | 4.013 ms | 22,395 | 441 |
| 4 | 4.000 ms | 22,396 | 441 |
| 8 | 3.941 ms | 22,394 | 441 |

### Detection

| Input | Time/op | Throughput | Bytes/op | Allocs/op |
|---|---:|---:|---:|---:|
| Syslog detection | 6,626 ns | 15.69 MB/s | 705 | 11 |
| CEF detection | 4,305 ns | 22.77 MB/s | 800 | 13 |
| LEEF detection | 4,913 ns | 16.89 MB/s | 5,572 | 26 |
| XML detection | 2,968 ns | 41.10 MB/s | 995 | 15 |
| CSV detection | 4,868 ns | 18.90 MB/s | 618 | 9 |
| JSON detection | 5,746 ns | 21.75 MB/s | 633 | 8 |

### Decoding

| Input | Time/op | Throughput | Bytes/op | Allocs/op |
|---|---:|---:|---:|---:|
| JSON, 256 B | 2,480 ns | 92.73 MB/s | 1,432 | 33 |
| JSON, 1 KiB | 10,869 ns | 99.83 MB/s | 6,755 | 154 |
| JSON, 8 KiB | 20,865 ns | 387.06 MB/s | 8,839 | 17 |
| Syslog RFC 5424 | 1,198 ns | 100.17 MB/s | 2,698 | 25 |
| Syslog RFC 3164 | 1,226 ns | 61.99 MB/s | 1,250 | 20 |
| Syslog wrapping CEF | 4,339 ns | 34.11 MB/s | 7,204 | 70 |
| Syslog wrapping LEEF | 3,109 ns | 38.60 MB/s | 6,816 | 64 |
| LEEF 1.0 tab delimiter | 1,300 ns | 70.78 MB/s | 2,624 | 47 |
| LEEF 2.0 literal delimiter | 1,428 ns | 86.12 MB/s | 2,712 | 50 |
| LEEF 2.0 hex delimiter | 1,120 ns | 95.57 MB/s | 1,928 | 45 |
| XML, one document | 7,530 ns | 46.61 MB/s | 8,832 | 176 |
| XML, 10 elements | 23,866 ns | 40.60 MB/s | 29,480 | 632 |
| XML, 100 elements | 225,455 ns | 40.62 MB/s | 274,171 | 5,951 |
| CEF | 3,281 ns | 39.02 MB/s | 3,061 | 54 |
| CSV | 1,922 ns | 47.86 MB/s | 6,090 | 35 |

### Pack matching and normalization

| Operation | Time/op | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| Match 1 pack | 85.04 ns | 8 | 1 |
| Match 10 packs | 732.1 ns | 80 | 10 |
| Match 100 packs | 7,130 ns | 800 | 100 |
| Match 1,000 packs | 76,745 ns | 8,005 | 1,000 |
| Extract direct nested key | 7.107 ns | 0 | 0 |
| Extract 4 nested levels | 79.31 ns | 64 | 1 |
| Normalize JSON payment | 2,501 ns | 3,031 | 44 |
| Validate normalized event | 280.4 ns | 464 | 4 |
| Normalize and validate | 2,783 ns | 3,127 | 48 |

Raw reports: [pipeline benchmarks](results/benchmarks/go-benchmarks-20261028T022719Z/pipeline.txt) · [decoding, pack matching, and normalization](results/benchmarks/go-benchmarks-20261028T022719Z/parsing.txt).

## Method C — stdin ingestion

`logsim` emitted and acknowledged 10,000 records. `dino` accepted and normalized all 10,000, reported zero quarantined and pending records, and the loss audit was valid. The normalized NDJSON file contains **10,000 lines**. The final CLI `Delivered` counter was **9,999**, one lower than both the normalized count and output line count; this discrepancy is recorded rather than treated as a 10,000-delivery result.

| Measurement | Result |
|---|---:|
| Producer emitted / acknowledged / errors | 10,000 / 10,000 / 0 |
| Collector accepted / normalized | 10,000 / 10,000 |
| Quarantined / pending | 0 / 0 |
| Delivered counter | 9,999 |
| NDJSON output lines | 10,000 |
| Elapsed | 72.570 s |
| Elapsed-time throughput | 137.8 events/s |
| Loss audit | Valid |

The elapsed rate is the whole `logsim | dino` command, including generator, SQLite, parsing, normalization, and file output. The one-event discrepancy in the CLI counter merits investigation before using that counter to compare runs. [Collector log](results/benchmarks/stdin-10k-20261028T023009Z-865eba06/dino.txt) · [producer log](results/benchmarks/stdin-10k-20261028T023009Z-865eba06/logsim.txt) · [elapsed time](results/benchmarks/stdin-10k-20261028T023009Z-865eba06/duration.txt).

## Method D — HTTP ingestion

The sender reported 10,000 emitted, 10,000 acknowledged, and zero errors. Final API stats reported all 10,000 accepted, normalized, and delivered, with no quarantine, pending work, or delivery failures; the loss audit was valid. The normalized file has 10,000 lines.

| Measurement | Result |
|---|---:|
| Producer emitted / acknowledged / errors | 10,000 / 10,000 / 0 |
| Accepted / normalized / delivered | 10,000 / 10,000 / 10,000 |
| Quarantined / pending | 0 / 0 |
| Delivery pending / failed | 0 / 0 |
| NDJSON output lines | 10,000 |
| Sender elapsed | 63.857 s |
| Sender throughput | 156.6 events/s |
| Collector-reported EPS | 139.78 |
| Collector uptime at final stats | 72.18 s |
| Reported Go heap allocation | 6,908,000 bytes |
| Goroutines | 20 |
| Loss audit | Valid |

Sender throughput uses 10,000 acknowledged requests divided by the recorded sender duration. Collector EPS is measured over process uptime and is a separate metric. `unmapped_ratio=1` means every normalized event contained at least one `ocsf.unmapped` entry; it does not mean normalization failed. [Sender log](results/benchmarks/http-10k-20261028T024232Z-69197/sender.txt) · [final stats](results/benchmarks/http-10k-20261028T024232Z-69197/final-stats.json) · [collector log](results/benchmarks/http-10k-20261028T024232Z-69197/dino.txt).

## Interpretation and limits

- All measurements use synthetic workloads on one Apple M2 system. They do not establish production capacity, latency guarantees, or performance on other hardware.
- Method C produced the expected file and normalized-record counts, but its final `Delivered` counter is short by one. Do not present that counter as fully delivered until the accounting discrepancy is understood.
- No Merkle batching, integrity verification, witness, Anvil, or chain operation was measured by these ingestion benchmarks.
- Method details and rerun commands are in [BENCHMARKING.md](BENCHMARKING.md). Reports are kept locally under [`results/benchmarks/`](results/benchmarks/); that directory is ignored by default. To include a reviewed raw run in GitHub, force-add only that run directory as described in the benchmark guide.
