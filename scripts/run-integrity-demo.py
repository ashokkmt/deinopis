#!/usr/bin/env python3
"""Build and run the isolated synthetic integrity demo, then save its evidence."""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


def require_command(name: str) -> str:
    path = shutil.which(name)
    if path:
        return path
    raise RuntimeError(f"Required command '{name}' was not found on PATH.")


def run_logged(command: list[str], log_path: Path, *, cwd: Path = ROOT) -> None:
    print("$ " + " ".join(command), flush=True)
    with log_path.open("wb") as log:
        result = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        tail = log_path.read_text(errors="replace").splitlines()[-35:]
        raise RuntimeError(
            f"Command failed ({result.returncode}); see {log_path}:\n" + "\n".join(tail)
        )


def free_loopback_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(url: str, token: str, method: str = "GET") -> dict:
    request = Request(
        url,
        data=b"" if method == "POST" else None,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with urlopen(request, timeout=180) as response:
            return json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"API returned HTTP {exc.code}: {detail}") from exc


def save_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    try:
        go = require_command("go")
        npm = require_command("npm")
        require_command("node")
        if not shutil.which("anvil"):
            foundry_anvil = Path.home() / ".foundry" / "bin" / "anvil"
            if not foundry_anvil.is_file() or not os.access(foundry_anvil, os.X_OK):
                raise RuntimeError("Required command 'anvil' was not found on PATH or at ~/.foundry/bin/anvil.")
            os.environ["PATH"] = str(foundry_anvil.parent) + os.pathsep + os.environ.get("PATH", "")
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = ROOT / "results" / f"simulated-integrity-demo-{stamp}-{secrets.token_hex(3)}"
        runtime = out / "runtime"
        tmp = out / "tmp"
        for directory in (runtime, tmp, runtime / "packs", runtime / "sources", runtime / "sinks"):
            directory.mkdir(parents=True, exist_ok=True)
        out.chmod(0o700)

        print("Building the embedded web UI and an isolated dino binary…", flush=True)
        if not (ROOT / "web" / "node_modules").is_dir():
            run_logged([npm, "ci", "--prefix", "web"], out / "npm-ci.log")
        run_logged([npm, "run", "build", "--prefix", "web"], out / "web-build.log")
        binary = runtime / "dino"
        run_logged([go, "build", "-o", str(binary), "./cmd/dino"], out / "go-build.log")

        port = free_loopback_port()
        base = f"http://127.0.0.1:{port}"
        token = secrets.token_hex(32)
        env = os.environ.copy()
        env["DEINOPIS_ADMIN_TOKEN"] = token
        env.pop("DEINOPIS_VAULT_STREAM_ID", None)
        for key in list(env):
            if key.startswith("DEINOPIS_WITNESS_"):
                env.pop(key, None)
        env.pop("DEINOPIS_DAEMON", None)
        # Keep all demo databases and Anvil state under this uniquely named output folder.
        env["TMPDIR"] = str(tmp)

        server_log = (out / "dino.log").open("wb")
        server = subprocess.Popen(
            [
                str(binary),
                "-db", str(runtime / "collector.sqlite"),
                "-packs-dir", str(runtime / "packs"),
                "-sources-dir", str(runtime / "sources"),
                "-sinks-dir", str(runtime / "sinks"),
                "-syslog-udp", "none", "-syslog-tcp", "none", "-syslog-tls", "none",
                "-http", "none", "-ui", f"127.0.0.1:{port}", "-inbox", "none",
                "-output-file", "none", "-stdout=false",
            ],
            cwd=runtime,
            env=env,
            stdout=server_log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise RuntimeError(f"dino exited during startup; see {out / 'dino.log'}")
                try:
                    with urlopen(base + "/api/v1/health", timeout=2) as response:
                        if response.status == 200:
                            break
                except (URLError, TimeoutError, OSError):
                    time.sleep(0.25)
            else:
                raise RuntimeError(f"dino did not become ready; see {out / 'dino.log'}")

            print("Starting the synthetic run and checking it…", flush=True)
            summary = request_json(base + "/api/v1/integrity/demo", token, "POST")
            save_json(out / "run-summary.json", summary)
            run_id = summary.get("run_id", "")
            if summary.get("mode") != "SIMULATED" or not run_id:
                raise RuntimeError("The demo did not return a SIMULATED run with a run ID.")

            clean = request_json(base + f"/api/v1/integrity/demo/{run_id}/verify", token, "POST")
            save_json(out / "clean-verification.json", clean)
            if clean.get("content", "").upper() != "INTACT" or clean.get("coverage") != "complete":
                raise RuntimeError(f"The clean check did not pass; see {out / 'clean-verification.json'}")

            print("Deleting one synthetic-only record and checking detection…", flush=True)
            request_json(base + f"/api/v1/integrity/demo/{run_id}/tamper", token, "POST")
            tampered = request_json(base + f"/api/v1/integrity/demo/{run_id}/verify", token, "POST")
            save_json(out / "tampered-verification.json", tampered)
            if tampered.get("content", "").upper() != "TAMPERED" or not tampered.get("issues"):
                raise RuntimeError(f"The deletion was not detected; see {out / 'tampered-verification.json'}")

            evidence = request_json(base + f"/api/v1/integrity/demo/{run_id}/evidence", token)
            if evidence.get("mode") != "SIMULATED":
                raise RuntimeError("The exported evidence is missing its SIMULATED label.")
            save_json(out / "evidence.json", evidence)

            issue = tampered["issues"][0]
            result = (
                "DEINOPIS SIMULATED INTEGRITY DEMO\n"
                f"Run: {run_id}\n"
                f"Clean check: {clean.get('content')} ({clean.get('records_checked')} records, "
                f"{clean.get('batches_checked')} batches; coverage {clean.get('coverage')})\n"
                f"After deleting one synthetic record: {tampered.get('content')}\n"
                f"Detected issue: {issue.get('reason')} (sequence {issue.get('sequence', 'n/a')})\n"
                f"First detected: {tampered.get('first_adverse_at_utc', 'see evidence.json')}\n"
                "Scope: disposable local data, local Anvil chain, same-host witness; not production proof.\n"
                "\nFiles: evidence.json, clean-verification.json, tampered-verification.json, run-summary.json\n"
            )
            (out / "RESULT.txt").write_text(result, encoding="utf-8")
            print("\n" + result)
            print(f"Saved demo output to: {out}")
        finally:
            if server.poll() is None:
                server.send_signal(signal.SIGINT)
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.terminate()
                    server.wait(timeout=5)
            server_log.close()
            binary.unlink(missing_ok=True)
            for suffix in ("", "-wal", "-shm"):
                (runtime / f"collector.sqlite{suffix}").unlink(missing_ok=True)

        return 0
    except Exception as exc:
        print(f"Demo failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
