# DEINOPIS CLI reference

Run from the repository root or an extracted release folder. Source builds use `./bin/dino`; release archives contain `dino` at the root. The binary embeds the UI and needs no Go or Node.js runtime.

## Start and configure DEINOPIS

Running `dino` starts DEINOPIS in the foreground. Listener defaults bind to loopback. Local parser files are available from `-packs-dir` and work offline.

| Argument | Default | Purpose |
|---|---|---|
| `-db <file>` | `data/deinopis.db` | SQLite database for raw events, normalized events, quarantine, and delivery state. |
| `-packs-dir <dir>` | `packs` | Active YAML parser directory and neutral `.pack-state/` transaction state. |
| `-sources-dir <dir>` | `sources` | YAML source adapter definitions. |
| `-sinks-dir <dir>` | `sinks` | YAML output adapter definitions. |
| `-workers <n>` | `4` | Processing worker count. |
| `-file <file>` | off | Read a one-shot input file. |
| `-stdin` | `false` | Read stdin; piped input is also detected automatically. |
| `-stdin-framing line\|ndjson\|document` | `line` | Choose input framing. |
| `-syslog-udp <address>` | `127.0.0.1:1514` | Syslog UDP listener; `none` disables it. |
| `-syslog-tcp <address>` | `127.0.0.1:1514` | Syslog TCP listener; `none` disables it. |
| `-syslog-tls <address>` | `none` | Syslog TLS listener; requires `-tls-cert` and `-tls-key`. |
| `-tls-cert <file>` / `-tls-key <file>` | unset | TLS certificate and private key. |
| `-tls-client-ca <file>` | unset | CA for client-certificate verification. |
| `-http <address>` | `127.0.0.1:8080` | HTTP ingestion API; `none` disables it. |
| `-http-key <token>` | unset | Require an intake key for HTTP ingestion. |
| `-inbox <dir>` | `data/inbox` | Watched file-drop folder; `none` disables it. |
| `-output-file <file>` | `data/output/normalized.ndjson` | Normalized NDJSON file output; `none` disables it. |
| `-stdout[=true\|false]` | `true` | Print normalized NDJSON to stdout. |
| `-ui <address>` | `127.0.0.1:9090` | Web UI and management API; `none` disables it. Use a trusted TLS proxy before remote exposure. |
| `-admin-token <token>` | unset | Protect configured management write routes. Prefer `DEINOPIS_ADMIN_TOKEN` in the service environment. |
| `-d` | foreground | Start detached and write `data/deinopis.pid` and `data/deinopis.log`. |
| `-stop` / `dino stop` | — | Stop a detached DEINOPIS process. |
| `-status` / `dino status` | — | Show daemon and control API status. |
| `-replay <id\|all\|list>` | — | List quarantine records or replay one/all through the management API. |
| `-replay-all` | `false` | Replay all eligible quarantine records. |
| `-verify <raw-id>` | — | Compare a raw payload with its stored SHA-256. |
| `-verify-event <event-id>` | — | Check normalized-event integrity and raw-event lineage. |
| `-v`, `--version`, `-h` | — | Print version or flag help. |

Example local run:

```sh
./bin/dino -syslog-udp none -syslog-tcp none -syslog-tls none \
  -http 127.0.0.1:8080 -ui 127.0.0.1:9090 -inbox none
```

A one-shot file run should disable long-running inputs:

```sh
./bin/dino -file testdata/sources/network-device-firewall/valid_session.log \
  -syslog-udp none -syslog-tcp none -syslog-tls none -http none -ui none -inbox none
```

## Decoded formats

Ingestion transport and payload format are separate: HTTP, Syslog, the file inbox, stdin, or Kafka can carry supported records. Decoders handle these syntax families; YAML packs then match sources and map fields into normalized events.

| Format | Notes |
|---|---|
| Syslog | RFC-style headers and messages; can wrap a supported payload format. |
| JSON | Structured application and audit records. |
| XML | Structured records parsed as XML. |
| CSV | Delimited records, including generated database audit samples. |
| CEF | Common Event Format security events. |
| LEEF | Log Event Extended Format security events. |
| Proprietary/application text | Source-specific parsing and extraction rules in YAML packs. |

## Local parser and connection commands

```sh
dino packs list
dino packs get network-device-firewall
dino packs validate -f packs/new-device.yaml
dino packs test -f packs/new-device.yaml -sample sample.log
dino packs apply -f packs/new-device.yaml
dino packs rollback

dino sources list
dino sources validate -f sources/cloud.yaml
dino sources test -f sources/cloud.yaml
dino sources apply -f sources/cloud.yaml
dino sources rollback

dino sinks list
dino sinks validate -f sinks/siem.yaml
dino sinks test -f sinks/siem.yaml
dino sinks apply -f sinks/siem.yaml
dino sinks rollback

dino replay
dino replay QUARANTINE_ID
dino replay all
```

Pack validation and testing do not activate YAML. `apply` normally uses the running management API; `--local --packs-dir <dir>` is for a stopped installation with no active directory owner. Local changes load at the next startup. Do not use local mode to bypass a failed authenticated live operation.

## Build the command-line tools

From the repository root, build the collector, source-log generator, local-chain smoke runner, and witness service as needed:

```sh
mkdir -p bin
go build -o bin/dino ./cmd/dino
go build -o bin/logsim ./cmd/logsim
go build -o bin/dino-sim ./cmd/dino-sim
go build -o bin/dino-witness ./cmd/dino-witness
```

`dino` is the collector and processor. `logsim` generates synthetic source records and sends them to a selected collector input. `dino-sim` is a local integrity/Anvil smoke tool. `dino-witness` is a separately configured mTLS service that retains original integrity manifests and exposes queue/history status. The last two are not needed for ordinary log ingestion; the Dashboard integrity demo starts its own disposable witness and Anvil instance.

## `logsim`: generate synthetic source logs

`logsim` is a source generator, not the blockchain simulator. It can generate firewall, ASA, SSHD, IDS, web-server, application, and database records under selectable scenarios, then send them to stdout, Syslog UDP/TCP/TLS, HTTP POST, or files. It does not run DEINOPIS parsing, store events, construct Merkle trees, or start Anvil. The Docker Compose simulator uses seven generators to exercise UDP Syslog, TCP Syslog, HTTP POST, and the watched file inbox. It does **not** configure Kafka, cloud polling, or Syslog TLS ingestors.

For a bounded local source test, run DEINOPIS in one terminal with the desired listener enabled, then in another:

```sh
./bin/logsim --source all --scenario mixed --format http-post \
  --target http://127.0.0.1:8080/api/v1/ingest --rate 5 --count 100
```

`--count 0` runs continuously until stopped. A single `logsim` process selects one output transport; start separate processes or use the Docker simulator Compose file to exercise several transports together.

## Run DEINOPIS and `logsim`

The four common arrangements are below. The recommended arrangement for exercising the configured source generators and multiple ingestors is **local DEINOPIS with Docker `logsim`** (mode 2).

### 1. Both run locally

Start DEINOPIS with local loopback listeners:

```sh
mkdir -p data/inbox data/output
./bin/dino -db data/deinopis.db \
  -syslog-udp 127.0.0.1:1514 -syslog-tcp 127.0.0.1:1514 -syslog-tls none \
  -http 127.0.0.1:8080 -ui 127.0.0.1:9090 -inbox data/inbox \
  -output-file data/output/normalized.ndjson -stdout=false
```

In a second terminal, send a bounded mix of all source types over HTTP:

```sh
./bin/logsim --source all --scenario mixed --format http-post \
  --target http://127.0.0.1:8080/api/v1/ingest --rate 5 --count 500
```

This exercises all synthetic source families over HTTP. The next mode exercises the Compose file's multiple transports at once.

### 2. DEINOPIS local, all `logsim` sources in Docker — recommended

Create the host spool directories and make the inbox writable by the simulator container (the image runs as UID 10001), then start the local collector. The permission change is for this synthetic local run only; restore private permissions after stopping the containers. **Bind Syslog and HTTP to `:1514` and `:8080`** so Docker containers can reach the host through `host.docker.internal`; the UI stays on loopback:

```sh
mkdir -p data/inbox data/output
chmod 0777 data/inbox
./bin/dino -db data/deinopis.db \
  -syslog-udp :1514 -syslog-tcp :1514 -syslog-tls none \
  -http :8080 -ui 127.0.0.1:9090 -inbox data/inbox \
  -output-file data/output/normalized.ndjson -stdout=false
```

In another terminal, build and start the seven Docker source generators:

```sh
docker compose -f simulator.yaml up --build
```

They send firewall/ASA/IDS Syslog UDP, SSHD Syslog TCP, application HTTP POST, and web/database files to the host's `data/inbox`. `--count 0` means the generators keep sending until stopped. Watch `http://127.0.0.1:9090` for processed events. Stop the generators with Ctrl+C or `docker compose -f simulator.yaml down`; stop foreground DEINOPIS with Ctrl+C. Then restore the inbox permissions with `chmod 0700 data/inbox`. This is a local development setup: `:1514` and `:8080` listen on all host interfaces while running. Use a trusted host/firewall, do not expose them to an untrusted network, and stop the process when finished.

If Docker cannot resolve `host.docker.internal`, check Docker Desktop networking; the Compose file also supplies the Linux `host-gateway` mapping. HTTP intake is unauthenticated by default unless `-http-key` is configured; `logsim` does not add that key, so this example is for a trusted local test only.

### 3. DEINOPIS in Docker, `logsim` local

Build the embedded UI, then start only the collector service. The mapped ports let a host `logsim` process connect through loopback:

```sh
npm ci --prefix web
npm run build --prefix web
docker compose -f docker-compose.yaml up --build -d deinopis
```

Send synthetic events from the host:

```sh
./bin/logsim --source all --scenario mixed --format http-post \
  --target http://127.0.0.1:8080/api/v1/ingest --rate 5 --count 500
```

This runs all source generators through the Docker collector's HTTP ingestor. To exercise TCP or UDP instead, use `--format syslog-tcp --target 127.0.0.1:1514` or `--format syslog-udp --target 127.0.0.1:1514`. Host file output is not shared with the container's named `/data` volume; use network ingestion in this arrangement unless you deliberately add a shared mount. Stop the collector with `docker compose -f docker-compose.yaml down`.

### 4. Both DEINOPIS and all `logsim` sources in Docker

Build the UI and start the main service plus all seven source generators on their private Compose network:

```sh
npm ci --prefix web
npm run build --prefix web
docker compose -f docker-compose.yaml up --build -d
```

Open `http://127.0.0.1:9090`. The containers route traffic to the Compose service name `deinopis`; file generators share the collector's `/data/inbox` volume. Follow output with `docker compose -f docker-compose.yaml logs -f deinopis sim-firewall sim-asa sim-sshd sim-ids sim-webserver sim-app sim-database`. Stop containers with `docker compose -f docker-compose.yaml down`. This preserves named volumes and database state. To also erase the Docker database and configuration volumes, use the reset command below.

## Reset local or Docker data

Stop DEINOPIS and all simulator containers before deleting state. From the repository root, this removes the **entire local `data/` directory**, including the SQLite database/WAL, inbox files, logs, and normalized output, then recreates empty inbox/output folders:

```sh
rm -rf -- data
mkdir -p data/inbox data/output
```

For Docker Compose's persistent named volumes, stop the stack and remove its volumes:

```sh
docker compose -f docker-compose.yaml down --volumes
```

This erases the Docker database volume and the named packs/sources/sinks configuration volumes for this Compose project. It does not erase a host `./data/` folder used by `simulator.yaml`; remove that separately with the local reset command. These resets are irreversible. They do not affect parser transaction history in host `packs/.pack-state/` when only local `data/` is removed.

## Integrity support tools

### `dino-witness`: retain original manifests

`dino-witness` is the separately operated manifest receiver/storage service in the production design. It accepts the collector's originals over mutually authenticated TLS, validates and retains first-write manifests, and reports preserved queue/publication history. It is not a source generator, collector, or blockchain node. In this simulation-only runtime it has **no external-chain publisher**; queue entries do not mean that anything was sent to a public chain. Ordinary ingestion and the one-click synthetic demo do not require a separately launched `dino-witness` process.

Build it with the other Go commands above. To inspect an existing witness store, point the read-only status operation at its actual paths; note that opening a previously unused path initializes an empty store, so do not provide a production path by mistake:

```sh
./bin/dino-witness -db data/witness.db -manifest-dir data/manifests -status
```

To run the mTLS service, provide a 32-byte stream ID as 64 lowercase hex characters, the enrolled collector certificate SHA-256 fingerprint, a collector client CA, the witness TLS certificate/key, and a shared witness token through the documented environment variables. Use private certificate/key files and replace the sample values with the enrollment values:

```sh
export DEINOPIS_WITNESS_STREAM_ID='<64-hex-character-stream-id>'
export DEINOPIS_COLLECTOR_CERT_SHA256='<64-hex-character-certificate-fingerprint>'
export DEINOPIS_WITNESS_CLIENT_CA='./certs/collector-ca.pem'
export DEINOPIS_WITNESS_TLS_CERT='./certs/witness.pem'
export DEINOPIS_WITNESS_TLS_KEY='./certs/witness-key.pem'
export DEINOPIS_WITNESS_TOKEN='<private-shared-token>'
./bin/dino-witness -listen 127.0.0.1:9443 \
  -db data/witness.db -manifest-dir data/manifests
```

This example binds only to loopback. A remote collector requires an intentionally reachable TLS endpoint and firewall policy; do not expose it without protecting the service and its storage. This runtime preserves old publication records for inspection but never resends them or publishes new jobs to an external chain.

The witness rejects retired `DEINOPIS_CHAIN_*`, `DEINOPIS_REGISTRY_*`, and `DEINOPIS_PUBLISHER_KEY_FILE` settings before opening its database. Existing publication jobs, attempts, signed bytes, and receipts remain stored for operator review; they are never replayed to a simulator.

### `dino-sim`: deploy the local registry

`dino-sim` serves a different purpose from `logsim`: it starts an isolated local Anvil EVM, deploys the real registry contract, verifies the empty registry, and prints local chain metadata. It creates no source logs and does not ingest them. It is a smoke test for the contract/deployment path, not the full Dashboard simulation, which manages its own synthetic logs, witness store, and Anvil run.

### Dashboard synthetic integrity demo

For the full synthetic workflow, install Foundry so `forge` and `anvil` are on `PATH`, start `dino` with the management UI and a configured admin token, then open the Dashboard. In **Configuration → Management API token**, enter the token used by `--admin-token` or `DEINOPIS_ADMIN_TOKEN`. Use **Start synthetic run**, then **Verify**. The run generates deterministic authentication, web, and application logs in separate temporary collector and witness stores, publishes commitments to its own loopback Anvil chain, and reports local witness receipts and registry readback. Results and buttons are labeled **SIMULATED**.

**Delete one synthetic record** and **Delete synthetic tail batch** affect only that run's temporary collector. **Export evidence** downloads `deinopis-simulated-integrity-evidence.json` after a completed check. It contains run/scope metadata, local registry and witness references, verification and first-adverse times, checksums, and explicit claim limits; raw event payloads are excluded. The first adverse observation is retained in the run-scoped witness database across retries, witness-store reopen, and synthetic recovery.

All demo routes require the configured admin token and accept no normal database/path selector. The demo does not start automatically, does not use the collector's normal database, and does not connect to a public RPC. Same-host witness state is not independent custody; local Anvil readback is not public-chain finality. No production scheduler, resumable async verification jobs, or trusted production integrity UI/API is implemented.

Install Foundry so `anvil` and `forge` are available, then build and run the local registry smoke test:

```sh
mkdir -p bin
go build -o bin/dino-sim ./cmd/dino-sim
./bin/dino-sim
```

It starts only its owned Anvil child on loopback with ephemeral local state, deploys the registry, and stops that child on Ctrl+C. Artifacts remain in the printed temporary run directory. Do not pass operational data or production signing keys to this simulator. This command is not needed for log ingestion and does not publish to a public/test network.

### Test the simulation

Run the local regression gates from the repository root with Foundry installed:

```sh
go test ./internal/simulator ./internal/api -count=1
go test ./... -count=1
go test -race ./... -count=1
forge fmt --check
forge test -q
npm run build --prefix web
```

The simulator and API tests start local Anvil. These commands verify the local simulation and retained code paths only; they do not qualify production chain custody, finality, or release behavior.

## Security and operations

Management, HTTP ingestion, and plaintext Syslog defaults bind to loopback. Configure intake and management authentication before exposing listeners. Store credentials in private environment variables or secret files, not in checked-in YAML or command history. Source and sink adapters keep their own configured network behavior; parser provisioning does not disable them.

Parser transaction records and rollback history are stored under `packs/.pack-state/`. This is operational state; preserve it when backing up parser configuration. See [README.md](README.md), [SECURITY.md](SECURITY.md), and [plans/implementation-phases.md](plans/implementation-phases.md).
