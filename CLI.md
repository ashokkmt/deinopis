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

## Security and operations

Management, HTTP ingestion, and plaintext Syslog defaults bind to loopback. Configure intake and management authentication before exposing listeners. Store credentials in private environment variables or secret files, not in checked-in YAML or command history. Source and sink adapters keep their own configured network behavior; parser provisioning does not disable them.

Parser transaction records and rollback history are stored under `packs/.pack-state/`. This is operational state; preserve it when backing up parser configuration. See [README.md](README.md), [SECURITY.md](SECURITY.md), and [plans/implementation-phases.md](plans/implementation-phases.md).
