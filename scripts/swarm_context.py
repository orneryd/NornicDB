"""Shared model context admission configuration for Unix and Windows launchers."""
import argparse
import json
import os
from pathlib import Path

def local_context_window(env):
    window = int(env.get("SWARM_LOCAL_CONTEXT_WINDOW", "0"))
    if window < 0:
        raise RuntimeError("SWARM_LOCAL_CONTEXT_WINDOW must be zero (native) or a positive token count.")
    return window or 262144


def configure_context(env):
    """Configure admission capacities without overriding providers' native windows."""
    env.setdefault("NORNIC_ROUTER_CONTEXT_WINDOW", "32768")
    env.setdefault("NORNIC_READ_MODEL_URL", env.get("SWARM_LOCAL_MODEL_URL", "http://127.0.0.1:11435").rstrip("/"))
    env.setdefault("NORNIC_READ_MODEL", env.get("SWARM_LOCAL_MODEL", "nornic-ornith"))
    env.setdefault("NORNIC_READ_CONTEXT_WINDOW", str(local_context_window(env)))
    if "NORNIC_FRONTIER_CONTEXT_WINDOW" not in env:
        catalog = Path(env.get("CODEX_HOME", str(Path.home() / ".codex"))) / "models_cache.json"
        if catalog.exists():
            try:
                models = json.loads(catalog.read_text()).get("models", [])
                model = next((item for item in models if item.get("slug") == "gpt-6.1-sol"), None)
                if model:
                    window = model.get("context_window")
                    percent = model.get("effective_context_window_percent", 100)
                    if type(window) is int and window > 0 and type(percent) is int and 0 < percent <= 100:
                        env["NORNIC_FRONTIER_CONTEXT_WINDOW"] = str(window * percent // 100)
            except (OSError, ValueError, TypeError):
                # Unknown capacity stays unknown; never invent a frontier limit.
                pass
    return env


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models", type=Path)
    parser.add_argument("--provider")
    parser.add_argument("--model")
    args = parser.parse_args()
    env = dict(os.environ)
    if args.models:
        provider = json.loads(args.models.read_text())["providers"][args.provider]
        model = next(item for item in provider["models"] if item["id"] == args.model)
        env.setdefault("SWARM_LOCAL_MODEL_URL", provider["baseUrl"].removesuffix("/v1"))
        env.setdefault("SWARM_LOCAL_MODEL", model["id"])
        env.setdefault("SWARM_LOCAL_CONTEXT_WINDOW", str(model["contextWindow"]))
    configured = configure_context(env)
    names = ("NORNIC_ROUTER_CONTEXT_WINDOW", "NORNIC_READ_MODEL_URL", "NORNIC_READ_MODEL", "NORNIC_READ_CONTEXT_WINDOW", "NORNIC_FRONTIER_CONTEXT_WINDOW")
    print(json.dumps({name: configured[name] for name in names if name in configured}))
