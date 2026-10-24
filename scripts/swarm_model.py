#!/usr/bin/env python3
"""Install the pinned local Ornith model as a persistent macOS llama-server service."""
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import urlopen

import swarm_local

MODEL_REPOSITORY = "mudler/Ornith-1.5-35B-A3B-APEX-MTP-GGUF"
MODEL_REVISION = "4c326713c52d13fdb40983a2a09a46cca8e88739"
MODEL_FILE = "Ornith-1.5-35B-A3B-APEX-MTP-Compact.gguf"
MODEL_SHA256 = "a7373f724cf3693e8a6a5f8209ae9a06ff8071ae2af60659bdbdf8ff6f6c10df"
MODEL_BYTES = 17437861152
SERVICE = "com.nornicdb.swarm-model"


def server_arguments(binary, model, env):
    parsed = urlsplit(env.get("SWARM_LOCAL_MODEL_URL", "http://127.0.0.1:11435"))
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost") or parsed.username or parsed.password or parsed.path not in ("", "/"):
        raise RuntimeError("SWARM_LOCAL_MODEL_URL must be a loopback HTTP URL without credentials or a path.")
    context = int(env.get("SWARM_LOCAL_CONTEXT_WINDOW", "0"))
    if context < 0 or context > 262144:
        raise RuntimeError("SWARM_LOCAL_CONTEXT_WINDOW must be 0 (native) or 1-262144.")
    return [str(binary), "--model", str(model), "--alias", env.get("SWARM_LOCAL_MODEL", "nornic-ornith"),
            "--host", parsed.hostname, "--port", str(parsed.port or 80),
            "--ctx-size", str(context), "--parallel", "1", "--n-gpu-layers", "99",
            "--flash-attn", "on", "--jinja", "--reasoning-format", "deepseek",
            "--spec-type", "draft-mtp", "--sleep-idle-seconds", "-1"]


def verify_model(path):
    if path.stat().st_size != MODEL_BYTES:
        raise RuntimeError("Ornith download size differs from the pinned artifact; no service started.")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != MODEL_SHA256:
        raise RuntimeError("Ornith checksum differs from the pinned artifact; no service started.")


def ensure_model(root):
    models = root / "models"
    models.mkdir(parents=True, exist_ok=True)
    path = models / MODEL_FILE
    stamp = models / "ornith-verified.json"
    if path.exists() and stamp.exists():
        recorded = json.loads(stamp.read_text())
        if recorded == {"size": path.stat().st_size, "mtime_ns": path.stat().st_mtime_ns, "sha256": MODEL_SHA256}:
            return path
    if not path.exists():
        partial = path.with_suffix(path.suffix + ".part")
        if not partial.exists() or partial.stat().st_size != MODEL_BYTES:
            print(f"Downloading {MODEL_FILE} ({MODEL_BYTES} bytes).", flush=True)
            subprocess.run(["curl", "--fail", "--location", "--show-error", "--retry", "3", "--continue-at", "-", "--output", str(partial),
                            f"https://huggingface.co/{MODEL_REPOSITORY}/resolve/{MODEL_REVISION}/{MODEL_FILE}"], check=True)
        verify_model(partial)
        partial.rename(path)
    else:
        verify_model(path)
    stamp.write_text(json.dumps({"size": path.stat().st_size, "mtime_ns": path.stat().st_mtime_ns, "sha256": MODEL_SHA256}) + "\n")
    return path


def setup():
    if sys.platform != "darwin":
        raise RuntimeError("The persistent local Metal model service is macOS-only; use the Windows/NVIDIA setup on Windows.")
    binary = shutil.which("llama-server")
    if not binary:
        raise RuntimeError("Install llama.cpp first: brew install llama.cpp. See docs/development/swarm-local.md.")
    root, _, _ = swarm_local.paths()
    env = swarm_local.private_env(root)
    model = ensure_model(root)
    logs = root / "logs"
    logs.mkdir(exist_ok=True)
    agent = Path.home() / "Library" / "LaunchAgents" / f"{SERVICE}.plist"
    agent.parent.mkdir(parents=True, exist_ok=True)
    settings = {"Label": SERVICE, "ProgramArguments": server_arguments(binary, model, env),
                "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 30,
                "StandardOutPath": str(logs / "local-model.log"), "StandardErrorPath": str(logs / "local-model.log")}
    payload = plistlib.dumps(settings)
    changed = not agent.exists() or agent.read_bytes() != payload
    domain = f"gui/{os.getuid()}"
    loaded = subprocess.run(["launchctl", "print", f"{domain}/{SERVICE}"], capture_output=True).returncode == 0
    if changed and loaded:
        subprocess.run(["launchctl", "bootout", f"{domain}/{SERVICE}"], check=True)
        loaded = False
    agent.write_bytes(payload)
    if not loaded:
        subprocess.run(["launchctl", "bootstrap", domain, str(agent)], check=True)
    origin = env.get("SWARM_LOCAL_MODEL_URL", "http://127.0.0.1:11435").rstrip("/")
    deadline = time.monotonic() + 180
    print(f"Waiting for the persistent local model at {origin}; logs: {logs / 'local-model.log'}", flush=True)
    while time.monotonic() < deadline:
        try:
            with urlopen(origin + "/health", timeout=2) as response:
                if response.status == 200:
                    print("Nornic local Ornith model is ready; native context, MTP enabled, idle sleep disabled.")
                    return
        except (URLError, TimeoutError):
            pass
        time.sleep(1)
    raise RuntimeError(f"Local model did not become ready. Inspect {logs / 'local-model.log'}; no healthy model is claimed.")


if __name__ == "__main__":
    try:
        setup()
    except (RuntimeError, OSError, subprocess.CalledProcessError) as error:
        print(f"swarm-model: {error}", file=sys.stderr)
        sys.exit(1)
