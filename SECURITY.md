# Security policy

## Reporting a vulnerability

Please do not publish exploit details, raw logs, credentials, or personal data in a public issue. Use GitHub's **Report a vulnerability** option under this repository's **Security** tab if it is enabled. If private reporting is unavailable, open a minimal issue requesting a private contact channel **without** technical details or secrets. Include the affected release/commit, impact, and reproduction steps only in the private report.

Maintainers have not published a response-time or supported-version guarantee. For a fix, use the newest available release and verify its asset checksum; do not assume an older release receives backports.

## Deployment warning

DEINOPIS is currently a prototype. Native management, HTTP ingestion, and plaintext Syslog listeners default to loopback addresses. Explicitly binding them to a public/interface address changes that boundary: HTTP management is not TLS, HTTP ingestion accepts unauthenticated requests unless `-http-key` is set, and regular event listings remain readable. Use a trusted TLS-terminating proxy and network policy before remote exposure, require tokens where supported, and do not publish demo Compose ports to untrusted hosts.

The management config response no longer returns the configured admin token; raw-event traces, connection probes, and simulated integrity-demo actions require a configured `DEINOPIS_ADMIN_TOKEN` (or `--admin-token`). Demo actions create synthetic data in disposable temporary stores and cannot select the normal collector database. Management JSON bodies are capped at 1 MiB. Sink outbox delivery now waits for each destination's flush, with at-least-once semantics (a destination can receive a duplicate if it accepted a write but the local acknowledgement could not be committed). SQLite WAL uses `synchronous=FULL` for the raw-store commit boundary. File-spool processing resumes files left in `processing/` and deduplicates already-committed offsets. These measures have regression tests, but do not replace live broker, remote SIEM, storage-failure, power-loss, or hosted-release validation.

For a local-only native run, keep the defaults or explicitly use `-http 127.0.0.1:8080 -ui 127.0.0.1:9090` and disable unused Syslog listeners with `-syslog-udp none -syslog-tcp none -syslog-tls none`. Restrict database and file permissions; avoid putting credentials in shell history or checked-in YAML. Source/sink manifests should use `secretRef` and private environment/file secrets. Treat hash verification as a local consistency check, not proof against someone who can replace both data and stored hashes.

## Integrity simulation boundary

The integrity demo is an isolated development feature. It accepts synthetic records only, creates run-specific temporary collector and witness stores, and starts its own Anvil process bound to loopback with ephemeral local chain state. Normal `dino`/`dino-witness` startup does not launch Anvil, connect to a public RPC, or load simulator state. The demo API routes require the configured admin token; the browser token is entered in the Configuration page and is kept in the current tab.

The Dashboard tamper controls can delete records only from that synthetic run. A completed check can export a JSON evidence report with run/scope metadata, local registry/witness references, checksums, and verification history; raw event payloads are excluded. The report is still operational metadata and should be handled accordingly. First-adverse observations persist in the run-scoped witness database and are not cleared by retry or restoring synthetic bytes, but that database is on the same host and shares its administrator boundary.

The simulation demonstrates code paths and the user workflow. It is not an independent witness, trusted snapshot service, production scheduler, public-chain publisher, finality proof, machine-wide attestation, or production tamper-protection claim. The production blockchain deployment remains design-only. Never use the synthetic controls as a repair or reset mechanism for real collector or witness data.

## Parser configuration safety

DEINOPIS provisions reviewed local YAML parsers in native, container, and air-gap packages. Review parser provenance and synthetic fixtures before activation. Parser YAML is declarative and does not execute extensions. Local activation retains strict validation, path containment, symlink rejection, expected-digest conflict checks, transactional writes, and rollback.

Do not rely on this release as the only archive for regulated, forensic, or loss-intolerant logs. Keep an independent source archive and validate delivery and recovery in your own environment.
