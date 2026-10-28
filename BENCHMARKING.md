# DEINOPIS benchmarking

The four methods below use benchmark/test paths that exist in this checkout:

| Method | What it measures | Current status |
|---|---|---|
| A — pressure matrix | Concurrent, mixed-format writes through the Go pipeline and SQLite | Available; opt-in test, 10,000-event run is supported |
| B — Go benchmarks | Pipeline worker scaling and decoder/pack/normalizer CPU/allocation cost | Available; benchmark functions exist in the listed packages |
| C — stdin CLI | Synthetic generator, OS pipe, CLI framing, SQLite, and normalized output | Available; measures one producer and local file output |
| D — HTTP intake | Local HTTP listener, producer acknowledgements, pipeline drain, and output counts | Available; measures a sequential HTTP sender, not concurrent maximum throughput |

These methods exercise the collector and its decoder/pipeline paths. They do **not** measure Merkle hashing, witness intake, Anvil/registry throughput, or production blockchain cost. Do not use their results as integrity-capacity or multi-node claims. They all use synthetic records.

Each command below creates a unique report directory under root `results/benchmarks/`, so repeat runs do not overwrite earlier results. Existing reports may be from older runs; this guide does not treat them as results from the corrected commands. **No benchmark is run by these instructions or by this documentation update.**

## Before each run

- Use Go 1.25+ and keep the machine otherwise idle. Build from this repository root.
- Method D's Bash commands use `curl` and Python 3's standard JSON library; no `jq` is needed. Method C does not open a network port. The revised Method D chooses two free loopback ports automatically.
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

Generated benchmark data is ignored by default so databases, NDJSON output, and copied parser packs are not accidentally added to Git. To publish a reviewed synthetic report, force-add only the selected run directory:

```sh
git status --short --ignored results/benchmarks
git add -f 'results/benchmarks/<run-directory>'
git commit -m "Add DEINOPIS benchmark report"
git push
```

Review the selected run for secrets or real data before force-adding it. The root `BENCHMARK_RESULTS.md` is a separate summary; its raw-report links resolve on GitHub only when you also force-add those referenced report files. These instructions do not add a GitHub Actions artifact workflow.

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

## Method C — CLI over stdin (one terminal)

This method uses **one terminal** and feeds records automatically. `logsim` writes 10,000 records to standard output; the pipe passes them directly into `dino -stdin`; when `logsim` finishes, the pipe closes and `dino` drains its queue and exits. Do not run `dino -stdin` by itself for this benchmark: by itself it waits for its input stream to close. Run the entire block below once from the repository root. The `bash` wrapper works from the default macOS zsh terminal too.

```sh
bash <<'BASH'
set -euo pipefail
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
run="results/benchmarks/stdin-10k-$run_id"
mkdir -p "$run/sources" "$run/sinks"
cp -R packs "$run/packs"
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
      -packs-dir "$run/packs" -sources-dir "$run/sources" -sinks-dir "$run/sinks" \
      -workers 4 -syslog-udp none -syslog-tcp none -syslog-tls none \
      -http none -ui none -inbox none -output-file "$run/normalized.ndjson" -stdout=false \
      2>&1 | tee "$run/dino.txt"
} ; } 2> "$run/duration.txt"

grep -q 'emitted=10000' "$run/logsim.txt"
grep -Eq '^Accepted:[[:space:]]+10000$' "$run/dino.txt"
grep -Eq '^Normalized:[[:space:]]+10000$' "$run/dino.txt"
grep -Eq '^Quarantined:[[:space:]]+0$' "$run/dino.txt"
grep -Eq '^Pending:[[:space:]]+0$' "$run/dino.txt"
grep -q 'Loss Audit:  invariant satisfied' "$run/dino.txt"
test "$(wc -l < "$run/normalized.ndjson" | tr -d ' ')" = 10000
printf 'PASS: 10,000 accepted and normalized; see %s\n' "$run"
BASH
```

No input is typed. If you accidentally started `dino -stdin` alone, it waits until stdin reaches end-of-file; on macOS/Linux that is Ctrl+D. For the benchmark, run the full producer-to-consumer pipeline above instead. Its private `packs/` copy avoids the previous lock error if another `dino` has the repository's packs open. It also checks both process results, all final counters, the loss audit, and output line count before printing `PASS`.

## Method D — local HTTP intake (two terminals)

This method deliberately uses **two terminals**. Terminal 1 runs `dino` and waits for HTTP requests. Terminal 2 sends the 10,000 records and waits for accepted records to reach the output file. Terminal 1 appearing idle is expected: it is the server waiting for terminal 2. Do not type log lines into terminal 1.

Build `dino` and `logsim` first as described under [Before each run](#before-each-run). Run both blocks from the repository root. Terminal 1 selects free loopback ports and writes its unique report directory to `data/.deinopis-http-benchmark-current`; terminal 2 reads that directory and its ports. Run only one Method D session at a time.

### Terminal 1 — start the collector and leave it running

Paste this entire block into terminal 1 and leave it open. It prints the selected report directory and then runs `dino` in the foreground.

```sh
bash <<'BASH'
set -euo pipefail
root="$PWD"
mkdir -p "$root/data" "$root/results/benchmarks"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
run="$root/results/benchmarks/http-10k-$run_id"
mkdir -p "$run/sources" "$run/sinks"
cp -R "$root/packs" "$run/packs"
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'; }
ingest_port="$(free_port)"
ui_port="$(free_port)"
while [ "$ui_port" = "$ingest_port" ]; do ui_port="$(free_port)"; done
printf '%s\n' "$run" > "$root/data/.deinopis-http-benchmark-current"
printf '%s %s\n' "$ingest_port" "$ui_port" > "$run/ports.txt"
trap 'rm -f "$root/data/.deinopis-http-benchmark-current"' EXIT
{
  printf 'commit='; git -C "$root" rev-parse HEAD
  printf 'status:\n'; git -C "$root" status --short
  go version
  uname -a
  printf 'ingest_port=%s\nui_port=%s\n' "$ingest_port" "$ui_port"
  date -u +%FT%TZ
} > "$run/environment.txt"
printf 'Collector is running. Report folder: %s\n' "$run"
printf 'Waiting for terminal 2 to send records to 127.0.0.1:%s\n' "$ingest_port"
"$root/bin/dino" -db "$run/deinopis.db" -packs-dir "$run/packs" \
  -sources-dir "$run/sources" -sinks-dir "$run/sinks" -workers 4 \
  -syslog-udp none -syslog-tcp none -syslog-tls none \
  -http "127.0.0.1:$ingest_port" -ui "127.0.0.1:$ui_port" -inbox none \
  -output-file "$run/normalized.ndjson" -stdout=false </dev/tty 2>&1 | tee "$run/dino.txt"
BASH
```

The `</dev/tty` redirect is required because this block itself is fed to Bash through a here-document. Without it, `dino` sees the finished script input as piped stdin, treats it as log input, reaches EOF, and exits before terminal 2 can send HTTP requests.

### Terminal 2 — send the 10,000 HTTP records and wait for completion

After terminal 1 prints “Collector is running,” paste this entire block into terminal 2. It reads the run folder and ports created by terminal 1.

```sh
bash <<'BASH'
set -euo pipefail
root="$PWD"
run="$(cat "$root/data/.deinopis-http-benchmark-current")"
read -r ingest_port ui_port < "$run/ports.txt"
ready=false
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$ui_port/api/v1/health" >/dev/null; then ready=true; break; fi
  sleep 0.5
done
if [ "$ready" != true ]; then echo "dino is not ready; inspect $run/dino.txt" >&2; exit 1; fi
TIMEFORMAT='sender_elapsed_seconds=%3R'
{ time {
  "$root/bin/logsim" --source app --scenario normal --format http-post \
    --target "http://127.0.0.1:$ingest_port/api/v1/ingest" --rate 0 --count 10000 \
    2>&1 | tee "$run/sender.txt"
} ; } 2> "$run/duration.txt"
grep -q 'emitted=10000 acked=10000 errors=0' "$run/sender.txt"

# A zero pending count alone is not completion; wait for every record to be normalized and delivered.
complete=false
for _ in $(seq 1 180); do
  curl -fsS "http://127.0.0.1:$ui_port/api/v1/stats" > "$run/final-stats.json" || true
  if python3 - "$run/final-stats.json" <<'PY'
import json, sys
try:
    with open(sys.argv[1], encoding='utf-8') as f:
        s = json.load(f)
    ok = (s.get('accepted') == 10000 and s.get('normalized') == 10000
          and s.get('quarantined') == 0 and s.get('pending') == 0
          and s.get('delivered') == 10000 and s.get('delivery_pending') == 0
          and s.get('delivery_failed') == 0 and s.get('loss_audit', {}).get('valid') is True)
except (OSError, ValueError):
    ok = False
sys.exit(0 if ok else 1)
PY
  then complete=true; break; fi
  sleep 1
done
if [ "$complete" != true ]; then echo "Timed out; inspect $run/final-stats.json and dino.txt" >&2; exit 1; fi
test "$(wc -l < "$run/normalized.ndjson" | tr -d ' ')" = 10000
printf 'PASS: 10,000 accepted, normalized, and delivered. Report: %s\n' "$run"
BASH
```

After terminal 2 prints `PASS`, return to terminal 1 and press Ctrl+C. The final accounting is written to `dino.txt`. The terminal-2 timer measures the sender only; its report stores the later drain counters separately. If terminal 2 waits at the health check, terminal 1 did not start successfully; inspect its displayed error and `dino.txt`. If terminal 2 times out at the completion gate, inspect `final-stats.json` before calling the run successful.

**Method D pass criteria:** sender reports `emitted=10000 acked=10000 errors=0`; final stats show 10,000 accepted, normalized, and delivered; quarantine/pending/delivery failures are zero; the loss audit is valid; and the NDJSON file has 10,000 lines. The private pack copy prevents the prior lock conflict. This sequential burst is not a sustained capacity or multi-node soak result.
