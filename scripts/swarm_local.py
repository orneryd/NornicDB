#!/usr/bin/env python3
"""Set up and run the pinned T3 harness on a Unix host.

The Windows harness remains in setup-swarm-local.ps1/start-swarm-local.ps1.
All generated files and credentials live outside the repository.
"""

import argparse
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import urllib.error
import urllib.request


REPO = Path(__file__).resolve().parent.parent
T3_PIN = "2c8bdf6177159d8fcd12a897d7c9311f6eb93237"
PI_PACKAGE = "@earendil-works/pi-coding-agent@1.4.0"


def paths():
    root = Path(os.environ.get("SWARM_TOOLS_ROOT", "~/.local/share/nornicdb-swarm")).expanduser().resolve()
    return root, root / "T3", root / "state"


def run(*args, cwd=None):
    print("+", " ".join(map(str, args)), flush=True)
    subprocess.run([str(arg) for arg in args], cwd=cwd, check=True)


def output(*args, cwd=None):
    return subprocess.check_output([str(arg) for arg in args], cwd=cwd, text=True).strip()


def require_tools(*names):
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise RuntimeError(f"Install {', '.join(missing)} first; see docs/development/swarm-local.md")


def private_env(root):
    """Read a private, literal KEY=value file; never evaluate shell commands."""
    env = os.environ.copy()
    path = root / "env"
    if path.exists():
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise RuntimeError(f"Malformed assignment in {path}")
            key, value = line.split("=", 1)
            key = key.strip()
            if not key.isidentifier() or not key.isupper():
                raise RuntimeError(f"Invalid environment name in {path}: {key}")
            env.setdefault(key, value.strip().strip('"').strip("'"))
    return env


def model_entry(model, name):
    return {
        "id": model, "name": name, "reasoning": False, "input": ["text"],
        "contextWindow": 32768, "maxTokens": 4096,
        "samplingParams": {"reasoning_effort": "none"},
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
    }


def provider_instance(name, provider, model, pi_home, pi_binary):
    return {
        "driver": "pi", "displayName": name, "enabled": True,
        "environment": [
            {"name": "PI_CODING_AGENT_DIR", "value": str(pi_home), "sensitive": False},
            {"name": "T3_NORNIC_LOCAL_WORKER", "value": "1", "sensitive": False},
        ],
        "config": {
            "enabled": True, "binaryPath": str(pi_binary),
            "launchArgs": f"--provider {provider} --model {model} --no-context-files --no-skills --no-builtin-tools",
            "customModels": [],
        },
    }


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def configure_providers(root, env):
    pi_home = root / "pi-state"
    local_url = env.get("SWARM_LOCAL_OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    remote_url = env.get("SWARM_REMOTE_OLLAMA_URL", "").rstrip("/")
    local_model = env.get("SWARM_LOCAL_MODEL", "qwen3.5:27b")
    remote_model = env.get("SWARM_REMOTE_MODEL", "qwen3.5:9b")
    providers = {
        "ollamaLocal": {
            "baseUrl": local_url + "/v1", "api": "openai-completions", "apiKey": "ollama",
            "models": [model_entry(local_model, "Qwen 3.5 27B Local (32K)")],
        },
    }
    if remote_url:
        providers["ollamaRemote"] = {
            "baseUrl": remote_url + "/v1", "api": "openai-completions", "apiKey": "ollama",
            "models": [model_entry(remote_model, "Qwen 3.5 9B Remote (32K)")],
        }
    models_path = pi_home / "models.json"
    models = json.loads(models_path.read_text()) if models_path.exists() else {}
    models.setdefault("providers", {}).update(providers)
    write_json(models_path, models)
    settings_path = root / "state" / "userdata" / "settings.json"
    settings = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    instances = settings.setdefault("providerInstances", {})
    pi_binary = root / "pi" / "node_modules" / ".bin" / "pi"
    instances.setdefault("ollama_local", provider_instance("Ollama Local 27B", "ollamaLocal", local_model, pi_home, pi_binary))
    if remote_url:
        instances.setdefault("ollama_remote", provider_instance("Ollama Remote 9B", "ollamaRemote", remote_model, pi_home, pi_binary))
    codex = env.get("SWARM_CODEX_BINARY") or shutil.which("codex")
    if codex:
        instances.setdefault("codex", {
            "driver": "codex", "displayName": "Codex Frontier", "enabled": True,
            "config": {"enabled": True, "binaryPath": codex},
        })
    write_json(settings_path, settings)


def setup():
    require_tools("git", "vp", "npm", "uv", "ollama")
    root, t3, state = paths()
    root.mkdir(parents=True, exist_ok=True)
    env = private_env(root)
    patch = REPO / "testing" / "swarm-local" / "t3-nornic.patch"
    if not t3.exists():
        run("git", "clone", "https://github.com/pingdotgg/t3code.git", t3)
    head = output("git", "rev-parse", "HEAD", cwd=t3)
    if head != T3_PIN:
        if output("git", "status", "--porcelain", cwd=t3):
            raise RuntimeError("T3 has local changes; preserve them or use a new SWARM_TOOLS_ROOT")
        run("git", "checkout", "--detach", T3_PIN, cwd=t3)
    reverse = subprocess.run(["git", "apply", "--reverse", "--check", str(patch)], cwd=t3, capture_output=True)
    if reverse.returncode:
        run("git", "apply", "--check", patch, cwd=t3)
        run("git", "apply", patch, cwd=t3)
    if not (t3 / "node_modules" / ".modules.yaml").exists():
        run("vp", "install", "--frozen-lockfile", cwd=t3)
    pi_binary = root / "pi" / "node_modules" / ".bin" / "pi"
    if not pi_binary.exists():
        run("npm", "install", "--prefix", root / "pi", PI_PACKAGE)
    python = root / "code-intel" / "bin" / "python"
    if not python.exists():
        run("uv", "venv", "--python", "3.13", root / "code-intel")
    run("uv", "pip", "install", "--python", python, "-r", REPO / "scripts" / "requirements-code-intelligence.txt")
    # Model download is explicit: setup remains usable when only the remote worker
    # is available or a large local model is still being fetched.
    configure_providers(root, env)
    state.mkdir(parents=True, exist_ok=True)
    print(f"Setup complete in {root}. Run 'make swarm-doctor', then 'make swarm'.")


def auth_headers(env):
    headers = {"Content-Type": "application/json"}
    if env.get("NORNICDB_TOKEN"):
        headers["Authorization"] = "Bearer " + env["NORNICDB_TOKEN"]
    elif env.get("NORNICDB_USER") and env.get("NORNICDB_PASSWORD"):
        token = base64.b64encode((env["NORNICDB_USER"] + ":" + env["NORNICDB_PASSWORD"]).encode()).decode()
        headers["Authorization"] = "Basic " + token
    return headers


def request(url, env, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=auth_headers(env)), timeout=5) as response:
        return json.load(response)


def doctor():
    root, t3, _ = paths()
    env = private_env(root)
    for label, value in [
        ("T3 checkout", t3.exists()),
        ("Pi binary", (root / "pi" / "node_modules" / ".bin" / "pi").exists()),
        ("Code intelligence", (root / "code-intel" / "bin" / "python").exists()),
        ("Codex CLI", bool(env.get("SWARM_CODEX_BINARY") or shutil.which("codex"))),
    ]:
        print(f"{label}: {'ready' if value else 'missing'}")
    for label, url in [
        ("NornicDB", (env.get("SWARM_NORNICDB_URL") or env.get("NORNICDB_URI", "")).rstrip("/") + "/health" if (env.get("SWARM_NORNICDB_URL") or env.get("NORNICDB_URI")) else ""),
        ("Remote Ollama", env.get("SWARM_REMOTE_OLLAMA_URL", "").rstrip("/") + "/api/tags" if env.get("SWARM_REMOTE_OLLAMA_URL") else ""),
        ("Local Ollama", env.get("SWARM_LOCAL_OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/") + "/api/tags"),
    ]:
        if not url:
            print(f"{label}: configure its URL in {root / 'env'}")
            continue
        try:
            result = request(url, env)
            models = [model.get("name") for model in result.get("models", [])] if isinstance(result, dict) else []
            print(f"{label}: ready" + (f" ({', '.join(models)})" if models else ""))
        except (OSError, ValueError) as exc:
            print(f"{label}: unavailable ({exc})")


def start():
    require_tools("vp")
    root, t3, state = paths()
    env = private_env(root)
    uri = (env.get("SWARM_NORNICDB_URL") or env.get("NORNICDB_URI", "")).rstrip("/")
    if not uri:
        raise RuntimeError(f"Set NORNICDB_URI in your shell or {root / 'env'}; see docs/development/swarm-local.md")
    try:
        request(uri + "/health", env)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"NornicDB unavailable at {uri}: {exc}") from exc
    env.setdefault("NORNICDB_WORKFLOW_URL", uri)
    env["NORNICDB_URI"] = uri
    env.setdefault("NORNICDB_WORKFLOW_DATABASE", "t3workflow")
    env.setdefault("NORNIC_CODE_DATABASE", "nornicdbcode")
    env.setdefault("NORNIC_OLLAMA_URL", env.get("SWARM_REMOTE_OLLAMA_URL", env.get("SWARM_LOCAL_OLLAMA_URL", "http://127.0.0.1:11434")))
    env.setdefault("NORNIC_CLASSIFIER_MODEL", env.get("SWARM_REMOTE_ALIAS", "nornic-coder"))
    env.setdefault("NORNIC_FRONTIER_INSTANCES", "codex,ollama_local,ollama_remote")
    env["NORNIC_DUPLICATE_PYTHON"] = str(root / "code-intel" / "bin" / "python")
    env["NORNIC_GRAPHIFY"] = str(root / "code-intel" / "bin" / "graphify")
    env["NORNIC_DUPLICATE_RUNNER"] = str(REPO / "scripts" / "code_intelligence.py")
    env["PYTHONUTF8"] = "1"
    result = request(uri + "/db/system/tx/commit", env, {
        "statements": [{"statement": "CREATE DATABASE t3workflow IF NOT EXISTS"}],
    })
    if result.get("errors"):
        raise RuntimeError(f"Cannot create workflow database: {result['errors']}")
    configure_providers(root, env)
    print("Starting T3. Open its pairing URL in a browser; press Ctrl-C to stop.", flush=True)
    subprocess.run(["vp", "run", "dev", "--home-dir", str(state), "--no-seed"], cwd=t3, env=env, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "doctor", "start"))
    command = parser.parse_args().command
    try:
        {"setup": setup, "doctor": doctor, "start": start}[command]()
    except (RuntimeError, subprocess.CalledProcessError, urllib.error.HTTPError) as exc:
        print(f"swarm: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
