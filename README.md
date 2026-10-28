# DEINOPIS — Universal Log Pre-processing Framework

DEINOPIS accepts heterogeneous logs, keeps their original bytes in a local SQLite store, and converts recognized records into a versioned, OCSF-based JSON event with a link back to the original. Unknown, malformed, or ambiguous records are quarantined for inspection and replay after a parser pack is updated. This is a log preprocessor, not a SIEM, search engine, or production security boundary.

The project includes a Go CLI/server, an embedded web management UI, YAML parser/source/sink definitions, and a synthetic log generator (`logsim`). It ships a reviewed set of local YAML parser packs and supports offline authoring, validation, testing, activation, and rollback. Configured cloud, broker, and output adapters retain their own network behavior. See [security guidance](SECURITY.md) before connecting real log sources.

## Get started from source

You need Git, Go 1.25 or newer, and Node.js/npm to **build**. The released CLI does not need Go, Node.js, npm, or a separate SQLite installation at runtime. From a terminal:

```sh
git clone https://github.com/ashokkmt/deinopis.git
cd deinopis
npm ci --prefix web
npm run build --prefix web
mkdir -p bin
go build -o bin/dino ./cmd/dino
./bin/dino --version
./bin/dino -syslog-udp none -syslog-tcp none -syslog-tls none -http 127.0.0.1:8080 -ui 127.0.0.1:9090 -inbox none -stdout=false
```

On Windows PowerShell, replace the last three commands with:

```powershell
New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin/dino.exe ./cmd/dino
.\bin\dino.exe --version
.\bin\dino.exe -syslog-udp none -syslog-tcp none -syslog-tls none -http 127.0.0.1:8080 -ui 127.0.0.1:9090 -inbox none -stdout=false
```

In another terminal, open `http://127.0.0.1:9090` for the UI or send a sample event:

```sh
curl -X POST http://127.0.0.1:8080/api/v1/ingest -H 'Content-Type: application/json' --data-binary @testdata/sources/app-custom-json/valid_after.json
```

The raw database defaults to `data/deinopis.db`; normalized NDJSON defaults to `data/output/normalized.ndjson`. Stop the foreground server with Ctrl+C. `web/dist/` is generated and must be built before compiling the CLI on a fresh clone. A routine local build reports a `dev-<commit>` version; no release-version flag is needed.

Native listeners default to loopback (`127.0.0.1`) so they are not remotely reachable by default. If you bind to a public/interface address, review [SECURITY.md](SECURITY.md), set the available ingestion/admin tokens, and put the plain-HTTP management UI behind a trusted TLS proxy.

For an entirely local, one-shot example without network listeners:

```sh
./bin/dino -file testdata/sources/network-device-firewall/valid_session.log -syslog-udp none -syslog-tcp none -syslog-tls none -http none -ui none -inbox none -stdout=false
```

On Windows, use `.\bin\dino.exe` in place of `./bin/dino`. These source-build commands keep the executable in `bin/`; downloaded release archives contain the `dino` executable at the archive root, so run `./dino` (or `.\dino.exe`) from the extracted release folder.

## Run a GitHub Release binary

Choose the archive for your OS and CPU from [GitHub Releases](https://github.com/ashokkmt/deinopis/releases): `deinopis-vX.Y.Z-linux-amd64.tar.gz`, `linux-arm64`, `darwin-amd64`, `darwin-arm64`, `windows-amd64`, or `windows-arm64`. Download its `SHA256SUMS` too and compare the archive hash before extracting. Each archive includes the binary, the reviewed local parser YAML set in `packs/`, and the `schemas/`, `sources/`, and `sinks/` configuration directories; keep the directories beside the binary and run from the extracted directory because configuration paths are relative by default.

On Linux/macOS, for example:

```sh
mkdir deinopis-release
tar -xzf deinopis-vX.Y.Z-linux-amd64.tar.gz -C deinopis-release
cd deinopis-release
./dino --version
./dino -syslog-udp none -syslog-tcp none -syslog-tls none -http 127.0.0.1:8080 -ui 127.0.0.1:9090 -inbox none -stdout=false
```

Replace `vX.Y.Z` and the platform suffix with the asset you downloaded. On macOS use `darwin-*`. On Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force deinopis-release | Out-Null
tar -xzf .\deinopis-vX.Y.Z-windows-amd64.tar.gz -C .\deinopis-release
Set-Location .\deinopis-release
.\dino.exe --version
.\dino.exe -syslog-udp none -syslog-tcp none -syslog-tls none -http 127.0.0.1:8080 -ui 127.0.0.1:9090 -inbox none -stdout=false
```

The UI is embedded in the binary. Cloud, Kafka, and external output integrations still need their respective services and credentials when enabled. The active [GitHub release workflow](.github/workflows/release.yml) runs on version-tag pushes and builds Linux, macOS, and Windows archives for amd64 and arm64, with SHA256 checksums. For a local six-platform build, see [`scripts/build-release.sh`](scripts/build-release.sh) and [`scripts/build-release.ps1`](scripts/build-release.ps1).

## What DEINOPIS handles

| Area | Current capability |
|---|---|
| Intake | Syslog UDP/TCP/TLS; HTTP POST; a watched file inbox; one-shot `-file`; piped stdin; declarative cloud REST pull and Kafka input. UDP is best effort; TLS, cloud, and Kafka require their own configuration/services. |
| Formats | Syslog, JSON, XML, CSV, CEF, LEEF, and proprietary/application text via YAML parser packs. Syslog can wrap another supported payload format. |
| Source packs | Included examples cover firewalls and other network devices, Linux auth, web access, applications, database audit, cloud audit, containers, endpoint IDS, IAM, IoT, and proprietary appliances. A pack provides source matching, extraction, typing, and normalization rules; a new source using an existing decoder can be onboarded without recompiling. |
| Processing | Original bytes and SHA-256 receipt, format detection, pack matching, OCSF-based normalization, `unmapped` source fields, quarantine and replay, raw/event lineage, and lifecycle counters. |
| Output | Stdout and NDJSON file by default; configurable HTTP SIEM, Kafka, Parquet, and NDJSON sinks through source/sink YAML and a SQLite-backed outbox. External delivery is at least once, so downstream systems should deduplicate by event ID. |

`packs/`, `sources/`, `sinks/`, and `schemas/` in this repository are the configuration examples and validation assets. The browser UI offers event, trace, quarantine, parser-pack, configuration, and SIMULATED integrity-demo views; the ingestion HTTP API and the UI/control API use separate listeners.

## CLI commands, formats, and replay

The full, flag-by-flag guide is [CLI.md](CLI.md). It explains every startup argument, default, listener, daemon switch (`-d`, `-status`, `-stop`), verification action, and `packs`/`sources`/`sinks` subcommand. Start with `dino -h` for the binary's own flag list.

The seven current decoder families are **Syslog, JSON, XML, CSV, CEF, LEEF, and declarative proprietary/application text**. Intake transport is separate from decoding: HTTP, a file, Kafka, or stdin can carry a supported format. A decoder parses syntax; a YAML parser pack identifies the source and maps its fields to the normalized event. [CLI.md's format table](CLI.md#decoded-formats) explains each one.

The three YAML directories have distinct jobs: `packs/` say **what a log means**, `sources/` say **where raw logs come from**, and `sinks/` say **where normalized events go**. You can list, inspect, validate, test, apply, and roll back each resource family. [CLI.md](CLI.md#local-parser-and-connection-commands) shows the exact syntax and the current live-apply behavior.

### The three simulator/support commands

- **`logsim`** generates fake firewall, ASA, SSHD, IDS, web-server, app, and database logs. It sends those records to DEINOPIS over one selected transport. It simulates log sources; it does not start a blockchain or verify integrity.
- **`dino-sim`** starts a disposable, loopback-only Anvil chain and deploys/read-checks the registry contract. It generates no logs. It is a contract smoke test; the Dashboard integrity demo starts its own chain and temporary witness state.
- **`dino-witness`** is a separate mTLS service for receiving and retaining original integrity manifests and checking witness queue/history. It is not required for ordinary ingest or the Dashboard synthetic demo, and its current runtime does not publish to an external blockchain.

Build and run commands for these tools and the four local/Docker combinations are in [CLI.md](CLI.md#build-the-command-line-tools) and [CLI.md](CLI.md#run-deinopis-and-logsim). The recommended arrangement is the host `dino` binary with all seven `logsim` containers started by `docker compose -f simulator.yaml up --build`; that sends Syslog UDP/TCP, HTTP, and spool files to the host collector. The CLI guide also documents local and Docker data resets.

Replay is the recovery path for an event DEINOPIS accepted but could not normalize. Its original bytes and quarantine reason remain in the database. After you validate and activate a matching pack, replay processes those stored bytes with the current pack without asking the source to resend them:

```sh
dino replay                         # list quarantined records and their IDs
dino packs validate -f new-device.yaml
dino packs test -f new-device.yaml -sample sample.log
dino packs apply -f new-device.yaml
dino replay QUARANTINE_ID           # replace with an ID from `dino replay`; or: dino replay all
```

These are example placeholders: the pack must actually match a quarantined event. Pack activation currently also requests quarantine reconciliation; check its report before replaying an ID manually. Replay and live pack activation need the running management API. The `-verify` commands compare stored bytes/events to hashes in the same database; they are consistency checks, not independently anchored tamper-proof evidence.

## Integrity simulation

DEINOPIS includes a synthetic-only demonstration of its local Merkle batching, witness manifest, Solidity registry, verification, recovery, and evidence export path. It uses the existing integrity code and contract with disposable collector/witness stores and a fresh Anvil instance bound to loopback. It does not read the normal collector database or real logs. Install [Foundry](https://getfoundry.sh/) so `forge` and `anvil` are available on `PATH` before starting the demo.

To smoke-test the empty local registry:

```sh
mkdir -p bin
go build -o bin/dino-sim ./cmd/dino-sim
./bin/dino-sim
```

The process prints the temporary run directory, chain ID, registry address, and code hash. Press Ctrl+C to stop only the Anvil process it started. This is a **SIMULATED** local-chain result, not public-chain publication, independent custody, or production finality.

To use the full Dashboard demonstration, start the regular server with a fresh test database and an admin token. This keeps the collector data used by the application separate from the simulator's own stores:

```sh
mkdir -p /tmp/deinopis-demo
export DEINOPIS_ADMIN_TOKEN="$(openssl rand -hex 32)"
./bin/dino -db /tmp/deinopis-demo/collector.sqlite \
  -syslog-udp none -syslog-tcp none -syslog-tls none \
  -http none -ui 127.0.0.1:9090 -inbox none \
  -output-file none -stdout=false -admin-token "$DEINOPIS_ADMIN_TOKEN"
```

Open `http://127.0.0.1:9090`, enter the same token under **Configuration → Management API token**, then use **Start synthetic run** and **Verify** on the Dashboard. You can delete one synthetic record or the synthetic tail batch to observe a simulated detection, then use **Export evidence** after verification. The report includes scope and local-chain metadata, first-adverse time, checksums, and explicit claim limits; it excludes raw event payloads. The first adverse observation is retained in the run-scoped witness database across retries, witness-store reopen, and synthetic recovery.

For the local Go, Solidity, and frontend regression gates, run from the repository root with Foundry installed:

```sh
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
staticcheck ./...
npm ci --prefix web
npm run build --prefix web
forge fmt --check
forge test -q
```

The Go simulator/API tests launch local Anvil. The simulation does not provide a periodic verification scheduler, resumable production jobs, independent witness administration, public-chain publishing/finality, or production integrity attestation. See [SECURITY.md](SECURITY.md), [CLI.md](CLI.md), and the detailed production/simulation boundary in [plans/integrity-solution.md](plans/integrity-solution.md#11-how-the-project-will-simulate-this-design).

## Docker and simulation

For the full local demo (DEINOPIS plus seven synthetic source containers), build the embedded UI first. The Dockerfile downloads Go modules during its build; a vendor directory is not required:

```sh
npm ci --prefix web
npm run build --prefix web
docker compose -f docker-compose.yaml up --build
```

Open `http://127.0.0.1:9090`. The Compose file maps ports 1514 UDP/TCP, 8080, and 9090 onto the host; it is a **local demonstration configuration**, not a secure public deployment. Stop with `docker compose -f docker-compose.yaml down`. Docker named volumes retain data after `down`; do not use `down -v` unless you intend to erase it. Docker needs network access to download uncached Go and npm dependencies on the first build.

To run DEINOPIS natively and only the simulator fleet in Docker, start DEINOPIS with `-syslog-udp :1514 -syslog-tcp :1514 -http :8080 -ui :9090 -inbox data/inbox`, then run `docker compose -f simulator.yaml up --build`. That simulator file sends to `host.docker.internal`; it contains seven standard demo sources (firewall, ASA, SSHD, IDS, web server, app, database). Stop with `docker compose -f simulator.yaml down`.

Without Docker, build `logsim` into `bin/` with `mkdir -p bin && go build -o bin/logsim ./cmd/logsim` (Windows: `New-Item -ItemType Directory -Force bin; go build -o bin/logsim.exe ./cmd/logsim`) and run a bounded HTTP test while DEINOPIS is listening:

```sh
./bin/logsim --source app --scenario normal --format http-post --target http://127.0.0.1:8080/api/v1/ingest --rate 5 --count 50
```

`logsim --source` accepts `firewall`, `asa`, `sshd`, `ids`, `webserver`, `app`, `database`, or `all`; it can emit to stdout, syslog UDP/TCP/TLS, HTTP POST, or files. The data is synthetic, not an enterprise source qualification. For the published measurements and repeatable 10,000-event methods, use [BENCHMARKING.md](BENCHMARKING.md).

## Deployment modes and safety

- **Offline/air-gapped:** transfer a matching Release archive and checksum, extract it with its packs/schemas/source/sink directories, and run only local file/stdin/spool or local network transports. The shipped CLI has no Go/Node runtime dependency or required internet call. Build-time dependencies and any externally configured adapter are separate.
- **Connected/server:** run the binary on a supported host and enable only the required ingress and egress integrations. Place network listeners behind a trusted boundary, provide TLS/authentication at the edge, and back up the SQLite database and output assets.
- **Docker:** use the included image/Compose files for local simulation; adapt mounts, networking, secrets, and permissions before a controlled deployment.

Known authentication, raw-data exposure, and delivery-durability gaps are documented in [SECURITY.md](SECURITY.md). Do not expose the default listeners to an untrusted network or use this prototype as the sole evidence store for critical records.

No license file is currently published. Please obtain maintainer permission before reusing or redistributing this source; maintainers should add their chosen license before inviting external contributions.
