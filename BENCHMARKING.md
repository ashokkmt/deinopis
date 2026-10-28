# DEINOPIS benchmarking

The four methods below use benchmark/test paths that exist in this checkout:

| Method | What it measures | Current status |
|---|---|---|
| A — pressure matrix | Concurrent, mixed-format writes through the Go pipeline and SQLite | Available; opt-in test, 10,000-event run is supported |
| B — Go benchmarks | Pipeline worker scaling and decoder/pack/normalizer CPU/allocation cost | Available; benchmark functions exist in the listed packages |
| C — stdin CLI | Synthetic generator, OS pipe, CLI framing, SQLite, and normalized output | Available; measures one producer and local file output |
| D — HTTP intake | Local HTTP listener, producer acknowledgements, pipeline drain, and output counts | Available; measures a sequential HTTP sender, not concurrent maximum throughput |

These methods exercise the collector and its decoder/pipeline paths. They do **not** measure Merkle hashing, witness intake, Anvil/registry throughput, or production blockchain cost. Do not use their results as integrity-capacity or multi-node claims. They all use synthetic records.

This checkout currently has no `results/` directory or benchmark report files. Each command below creates a unique report directory under root `results/benchmarks/`, so repeat runs do not overwrite earlier results. The old report links and machine-specific figures were not present as files in this checkout and are not asserted here. **No benchmark is run by these instructions or by this documentation update.**

## Before each run

- Use Go 1.25+ and keep the machine otherwise idle. Build from this repository root.
- Method D's Bash sender uses `curl` and `jq`; PowerShell uses `Invoke-RestMethod`. Methods C and D bind local ports `18080` and `19090`; stop other processes using those ports first.
- For Methods C and D on a fresh checkout, build the embedded UI before building `dino`:

  ```sh
  npm ci --prefix web
  npm run build --prefix web
  mkdir -p bin
  go build -o bin/dino ./cmd/dino
  go build -o bin/logsim ./cmd/logsim
  ```

- Choose a fresh run ID for every run. Record the commit, dirty status, Go version, OS/CPU, RAM, storage, active packs/sinks, start/end UTC, and exact command beside the output. Do not put secrets or real logs in a benchmark report.
- The pressure harness uses its own temporary SQLite database. Methods C and D use a fresh report-specific database and output path.
- Report accepted, normalized, quarantined, pending, producer acknowledgements, output counts, errors, and elapsed time together. `accepted = normalized + quarantined + pending` is lifecycle accounting, not exactly-once external delivery.

To upload reports to the GitHub repository after a run, review the files and commit/push that run directory:

```sh
git status --short results/benchmarks
git add 'results/benchmarks/<run-directory>'
git commit -m "Add DEINOPIS benchmark report"
git push
```

`results/` is not ignored by `.gitignore`. Do not add generated reports until you have reviewed them for secrets or real data. These instructions do not add a GitHub Actions artifact workflow; the checked-in reports become available on GitHub when you commit and push them.

## Method A — concurrent all-format pressure matrix

This runs `TestRigorousPressureMatrix` from `test/pressure/pressure_test.go`: eight producers submit a mixed decoder workload to a durable SQLite-backed pipeline. `DEINOPIS_BENCH_EVENTS=10000` requests 10,000 events. It does not exercise a network listener.

Bash (macOS/Linux):

```sh
set -o pipefail
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
run="results/benchmarks/pressure-10k-$run_id"
mkdir -p "$run"
{
  printf 'commit='; git rev-parse HEAD
  printf 'status:\n'; git status --short
  go version
  uname -a
  date -u +%FT%TZ
} > "$run/environment.txt"
DEINOPIS_RIGOROUS_BENCH=1 DEINOPIS_BENCH_EVENTS=10000 \
  go test -count=1 -timeout=10m -v -run '^TestRigorousPressureMatrix$' ./test/pressure \
  2>&1 | tee "$run/pressure.txt"
```

PowerShell:

```powershell
$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$run = "results/benchmarks/pressure-10k-$runId"
New-Item -ItemType Directory -Force $run | Out-Null
"commit=$(git rev-parse HEAD)" | Set-Content "$run/environment.txt"
git status --short | Add-Content "$run/environment.txt"
go version | Add-Content "$run/environment.txt"
Get-Date -AsUTC -Format o | Add-Content "$run/environment.txt"
$env:DEINOPIS_RIGOROUS_BENCH = '1'
$env:DEINOPIS_BENCH_EVENTS = '10000'
go test -count=1 -timeout=10m -v -run '^TestRigorousPressureMatrix$' ./test/pressure 2>&1 |
  Tee-Object "$run/pressure.txt"
$testExit = $LASTEXITCODE
Remove-Item Env:DEINOPIS_RIGOROUS_BENCH, Env:DEINOPIS_BENCH_EVENTS
if ($testExit -ne 0) { throw "Pressure benchmark failed with exit code $testExit" }
```

Pass only if the report contains `events=10000`, `normalized=10000`, `quarantined=0`, `pending=0`, and the test passes. This is a saturated, single-store run, not a sustained EPS promise.

## Method B — fixed-work Go benchmarks

`BenchmarkPipeline_E2E` compares 1, 4, and 8 workers. `-benchtime=10000x` means 10,000 iterations **per sub-benchmark**, not one shared 10,000-event workload. The decoder, pack, and normalizer benchmark functions are in the named packages.

Bash (macOS/Linux):

```sh
set -o pipefail
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
run="results/benchmarks/go-benchmarks-$run_id"
mkdir -p "$run"
{
  printf 'commit='; git rev-parse HEAD
  printf 'status:\n'; git status --short
  go version
  uname -a
  date -u +%FT%TZ
} > "$run/environment.txt"
go test -count=1 -run '^$' -bench '^BenchmarkPipeline_E2E$' \
  -benchtime=10000x -benchmem ./test/pressure 2>&1 | tee "$run/pipeline.txt"
go test -count=1 -run '^$' -bench . -benchmem \
  ./internal/decode ./internal/packs ./internal/normalize 2>&1 | tee "$run/parsing.txt"
```

PowerShell:

```powershell
$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$run = "results/benchmarks/go-benchmarks-$runId"
New-Item -ItemType Directory -Force $run | Out-Null
"commit=$(git rev-parse HEAD)" | Set-Content "$run/environment.txt"
git status --short | Add-Content "$run/environment.txt"
go version | Add-Content "$run/environment.txt"
Get-Date -AsUTC -Format o | Add-Content "$run/environment.txt"
go test -count=1 -run '^$' -bench '^BenchmarkPipeline_E2E$' -benchtime=10000x -benchmem ./test/pressure 2>&1 |
  Tee-Object "$run/pipeline.txt"
if ($LASTEXITCODE -ne 0) { throw 'Pipeline benchmark failed' }
go test -count=1 -run '^$' -bench . -benchmem ./internal/decode ./internal/packs ./internal/normalize 2>&1 |
  Tee-Object "$run/parsing.txt"
if ($LASTEXITCODE -ne 0) { throw 'Parsing benchmarks failed' }
```

Go benchmark calibration, toolchain, and hardware affect `ns/op`; compare runs only with their environment metadata.

## Method C — CLI over stdin

This sends 10,000 synthetic application logs through `logsim | dino -stdin`, including the OS pipe, line framing, SQLite, parser matching, and NDJSON file output. It is a single-producer path, not a maximum-throughput test.

Bash (macOS/Linux):

```sh
set -o pipefail
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
run="results/benchmarks/stdin-10k-$run_id"
mkdir -p "$run/sources" "$run/sinks"
{
  printf 'commit='; git rev-parse HEAD
  printf 'status:\n'; git status --short
  go version
  uname -a
  date -u +%FT%TZ
} > "$run/environment.txt"
TIMEFORMAT='elapsed_seconds=%3R'
{ time {
./bin/logsim --source app --scenario normal --format stdout --rate 0 --count 10000 \
  2> "$run/logsim.txt" |
  ./bin/dino -stdin -stdin-framing line -db "$run/deinopis.db" \
    -packs-dir packs -sources-dir "$run/sources" -sinks-dir "$run/sinks" \
    -workers 4 -syslog-udp none -syslog-tcp none -syslog-tls none \
    -http none -ui none -inbox none -output-file "$run/normalized.ndjson" -stdout=false \
    2>&1 | tee "$run/dino.txt"
} ; } 2> "$run/duration.txt"
```

PowerShell:

```powershell
$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$run = "results/benchmarks/stdin-10k-$runId"
New-Item -ItemType Directory -Force "$run/sources", "$run/sinks" | Out-Null
"commit=$(git rev-parse HEAD)" | Set-Content "$run/environment.txt"
git status --short | Add-Content "$run/environment.txt"
go version | Add-Content "$run/environment.txt"
Get-Date -AsUTC -Format o | Add-Content "$run/environment.txt"
$elapsed = Measure-Command {
  .\bin\logsim.exe --source app --scenario normal --format stdout --rate 0 --count 10000 2> "$run/logsim.txt" |
    .\bin\dino.exe -stdin -stdin-framing line -db "$run/deinopis.db" `
      -packs-dir packs -sources-dir "$run/sources" -sinks-dir "$run/sinks" `
      -workers 4 -syslog-udp none -syslog-tcp none -syslog-tls none `
      -http none -ui none -inbox none -output-file "$run/normalized.ndjson" -stdout=false 2>&1 |
      Tee-Object "$run/dino.txt"
}
"elapsed_seconds=$($elapsed.TotalSeconds)" | Set-Content "$run/duration.txt"
if ($LASTEXITCODE -ne 0) { throw "stdin benchmark failed with exit code $LASTEXITCODE" }
```

Check that `logsim.txt` reports `emitted=10000`, and `dino.txt` reports `Accepted: 10000`, `Normalized: 10000`, `Quarantined: 0`, `Pending: 0`, and a passing loss audit. Do not attribute Method A results to this method.

## Method D — live HTTP intake

This runs the real local HTTP ingestion listener and sends 10,000 sequential app JSON requests. `acked=10000` means the producer received successful HTTP responses; it does not prove a downstream sink committed 10,000 events. Use two terminals and the **same new run path** in both.

Terminal 1 — Bash (macOS/Linux):

```sh
run="results/benchmarks/http-10k-REPLACE-WITH-UNIQUE-ID"
mkdir -p "$run/sources" "$run/sinks"
{
  printf 'commit='; git rev-parse HEAD
  printf 'status:\n'; git status --short
  go version
  uname -a
  date -u +%FT%TZ
} > "$run/environment.txt"
./bin/dino -db "$run/deinopis.db" -packs-dir packs \
  -sources-dir "$run/sources" -sinks-dir "$run/sinks" -workers 4 \
  -syslog-udp none -syslog-tcp none -syslog-tls none \
  -http 127.0.0.1:18080 -ui 127.0.0.1:19090 -inbox none \
  -output-file "$run/normalized.ndjson" -stdout=false 2>&1 | tee "$run/dino.txt"
```

Terminal 2 — Bash (macOS/Linux):

```sh
set -o pipefail
run="results/benchmarks/http-10k-REPLACE-WITH-UNIQUE-ID"
TIMEFORMAT='sender_elapsed_seconds=%3R'
{ time {
./bin/logsim --source app --scenario normal --format http-post \
  --target http://127.0.0.1:18080/api/v1/ingest --rate 0 --count 10000 \
  2>&1 | tee "$run/sender.txt"
} ; } 2> "$run/duration.txt"
until [ "$(curl -fsS http://127.0.0.1:19090/api/v1/stats | jq -r .pending)" = "0" ]; do sleep 1; done
curl -fsS http://127.0.0.1:19090/api/v1/stats | tee "$run/final-stats.json"
```

PowerShell Terminal 1:

```powershell
$run = 'results/benchmarks/http-10k-REPLACE-WITH-UNIQUE-ID'
New-Item -ItemType Directory -Force "$run/sources", "$run/sinks" | Out-Null
"commit=$(git rev-parse HEAD)" | Set-Content "$run/environment.txt"
git status --short | Add-Content "$run/environment.txt"
go version | Add-Content "$run/environment.txt"
Get-Date -AsUTC -Format o | Add-Content "$run/environment.txt"
.\bin\dino.exe -db "$run/deinopis.db" -packs-dir packs `
  -sources-dir "$run/sources" -sinks-dir "$run/sinks" -workers 4 `
  -syslog-udp none -syslog-tcp none -syslog-tls none `
  -http 127.0.0.1:18080 -ui 127.0.0.1:19090 -inbox none `
  -output-file "$run/normalized.ndjson" -stdout=false 2>&1 | Tee-Object "$run/dino.txt"
```

PowerShell Terminal 2:

```powershell
$run = 'results/benchmarks/http-10k-REPLACE-WITH-UNIQUE-ID'
$elapsed = Measure-Command {
  .\bin\logsim.exe --source app --scenario normal --format http-post `
    --target http://127.0.0.1:18080/api/v1/ingest --rate 0 --count 10000 2>&1 |
    Tee-Object "$run/sender.txt"
}
"sender_elapsed_seconds=$($elapsed.TotalSeconds)" | Set-Content "$run/duration.txt"
if ($LASTEXITCODE -ne 0) { throw "HTTP sender failed with exit code $LASTEXITCODE" }
do {
  $stats = Invoke-RestMethod http://127.0.0.1:19090/api/v1/stats
  if ($stats.pending -ne 0) { Start-Sleep -Seconds 1 }
} while ($stats.pending -ne 0)
$stats | ConvertTo-Json -Depth 8 | Set-Content "$run/final-stats.json"
$stats | Select-Object accepted, normalized, quarantined, pending, delivered, delivery_pending, delivery_failed
```

Pass only if the sender reports `emitted=10000 acked=10000 errors=0` and final stats report `accepted=10000 normalized=10000 quarantined=0 pending=0`. Save producer time and final drain state separately. Stop DEINOPIS with Ctrl+C after retaining the report directory. This burst is not a sustained one-billion-events/day validation; that requires a measured multi-node soak with real broker/sink behavior, bounded backlog, and recovery evidence.
