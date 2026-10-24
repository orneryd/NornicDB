"""Tests for the local T3 launcher without network or model downloads."""

import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest import mock

import swarm_local


class SwarmLocalTest(unittest.TestCase):
    def test_context_capacity_uses_sol_catalog_and_preserves_explicit_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "models_cache.json").write_text(json.dumps({"models": [
                {"slug": "gpt-6-astra", "context_window": 1000000},
                {"slug": "gpt-6.1-sol", "context_window": 500000, "effective_context_window_percent": 95},
            ]}))
            env = swarm_local.configure_context({"CODEX_HOME": str(root), "SWARM_LOCAL_CONTEXT_WINDOW": "0"})
            self.assertEqual(env["NORNIC_FRONTIER_CONTEXT_WINDOW"], "475000")
            self.assertEqual(env["NORNIC_READ_CONTEXT_WINDOW"], "262144")
            self.assertEqual(env["NORNIC_ROUTER_CONTEXT_WINDOW"], "32768")
            explicit = swarm_local.configure_context({"CODEX_HOME": str(root), "NORNIC_FRONTIER_CONTEXT_WINDOW": "123456"})
            self.assertEqual(explicit["NORNIC_FRONTIER_CONTEXT_WINDOW"], "123456")

    def test_shared_context_cli_uses_windows_manifest_without_printing_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            models = root / "models.json"
            models.write_text(json.dumps({"providers": {"ollama": {"baseUrl": "http://windows:11434/v1", "models": [{"id": "nornic-agent-remote", "contextWindow": 262144}]}}}))
            env = dict(os.environ)
            for name in list(env):
                if name.startswith(("NORNIC_READ_", "SWARM_LOCAL_", "NORNIC_FRONTIER_")):
                    env.pop(name)
            env.update({"CODEX_HOME": directory, "LINEAR_API_KEY": "must-not-print"})
            result = subprocess.run([sys.executable, str(Path(swarm_local.__file__).with_name("swarm_context.py")), "--models", str(models), "--provider", "ollama", "--model", "nornic-agent-remote"], env=env, text=True, capture_output=True, check=True)
            configured = json.loads(result.stdout)
            self.assertEqual(configured["NORNIC_READ_MODEL"], "nornic-agent-remote")
            self.assertEqual(configured["NORNIC_READ_MODEL_URL"], "http://windows:11434")
            self.assertEqual(configured["NORNIC_READ_CONTEXT_WINDOW"], "262144")
            self.assertNotIn("must-not-print", result.stdout + result.stderr)

    def test_unknown_frontier_capacity_is_not_guessed(self):
        with tempfile.TemporaryDirectory() as directory:
            env = swarm_local.configure_context({"CODEX_HOME": directory})
            self.assertNotIn("NORNIC_FRONTIER_CONTEXT_WINDOW", env)

    def test_provider_setup_preserves_existing_instances(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = root / "state" / "userdata" / "settings.json"
            swarm_local.write_json(settings, {"providerInstances": {"existing": {"driver": "codex"}}})
            swarm_local.write_json(root / "pi-state" / "models.json", {"providers": {"custom": {"baseUrl": "http://custom"}}})
            with mock.patch.object(swarm_local.shutil, "which", return_value="/bin/codex"):
                swarm_local.configure_providers(root, {
                    "SWARM_REMOTE_OLLAMA_URL": "http://remote:11434",
                })
            saved = json.loads(settings.read_text())
            self.assertEqual(saved["providerInstances"]["existing"], {"driver": "codex"})
            self.assertIn("ollama_local", saved["providerInstances"])
            self.assertIn("ollama_remote", saved["providerInstances"])
            self.assertIn("codex", saved["providerInstances"])
            self.assertEqual(saved["providerInstances"]["ollama_local"]["displayName"], "Nornic Local · Ornith")
            self.assertEqual(saved["providerInstances"]["ollama_remote"]["displayName"], "Nornic Remote 9B")
            self.assertEqual(saved["defaultModelSelection"], {"instanceId": "ollama_local", "model": "default"})
            for worker_id in ("nornic_local_worker", "nornic_remote_worker"):
                worker = saved["providerInstances"][worker_id]
                self.assertNotIn("--no-builtin-tools", worker["config"]["launchArgs"])
                self.assertNotIn("--no-context-files", worker["config"]["launchArgs"])
                self.assertIn({"name": "T3_NORNIC_DELEGATED_WORKER", "value": "1", "sensitive": False}, worker["environment"])
                self.assertFalse(any(item["name"] == "T3_NORNIC_LOCAL_WORKER" for item in worker["environment"]))
            self.assertIn("--no-builtin-tools", saved["providerInstances"]["ollama_local"]["config"]["launchArgs"])

            models = json.loads((root / "pi-state" / "models.json").read_text())
            self.assertEqual(models["providers"]["custom"], {"baseUrl": "http://custom"})
            self.assertEqual(models["providers"]["ollamaRemote"]["baseUrl"], "http://remote:11434/v1")
            self.assertEqual(models["providers"]["nornicLocal"]["models"][0]["id"], "nornic-ornith")
            self.assertEqual(models["providers"]["nornicLocal"]["models"][0]["contextWindow"], 262144)
            self.assertEqual(models["providers"]["nornicLocal"]["baseUrl"], "http://127.0.0.1:11435/v1")

    def test_token_takes_precedence_over_basic_auth(self):
        self.assertEqual(
            swarm_local.auth_headers({"NORNICDB_TOKEN": "secret", "NORNICDB_USER": "u", "NORNICDB_PASSWORD": "p"})["Authorization"],
            "Bearer secret",
        )
        self.assertEqual(
            swarm_local.auth_headers({"NORNICDB_USER": "u", "NORNICDB_PASSWORD": "p"})["Authorization"],
            "Basic dTpw",
        )

    def test_explicit_user_default_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = root / "state" / "userdata" / "settings.json"
            swarm_local.write_json(settings, {
                "defaultModelSelection": {"instanceId": "codex", "model": "gpt-6.1-sol"},
            })
            with mock.patch.object(swarm_local.shutil, "which", return_value="/bin/codex"):
                swarm_local.configure_providers(root, {})
            self.assertEqual(json.loads(settings.read_text())["defaultModelSelection"]["model"], "gpt-6.1-sol")

    def test_lan_configuration_preserves_other_vite_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env.local").write_text("CUSTOM_SETTING=keep\nHOST=localhost\n")
            env = {"SWARM_T3_ADVERTISE_HOST": "192.168.1.231"}
            args = swarm_local.configure_network(root, env)
            self.assertEqual(args, ["--host", "0.0.0.0", "--port", "13773", "--dev-url", "http://192.168.1.231:5733"])
            self.assertEqual((root / ".env.local").read_text(), "CUSTOM_SETTING=keep\nHOST=0.0.0.0\n")
            self.assertEqual(env["T3CODE_PORT_OFFSET"], "0")
            with self.assertRaises(RuntimeError):
                swarm_local.configure_network(root, {"SWARM_T3_ADVERTISE_HOST": "127.0.0.1"})

    def test_private_env_does_not_execute_or_override_process_values(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "env").write_text("SWARM_REMOTE_OLLAMA_URL=http://remote:11434\nNORNICDB_TOKEN=from-file\n")
            with mock.patch.dict(os.environ, {"NORNICDB_TOKEN": "from-process"}):
                env = swarm_local.private_env(root)
            self.assertEqual(env["NORNICDB_TOKEN"], "from-process")
            self.assertEqual(env["SWARM_REMOTE_OLLAMA_URL"], "http://remote:11434")

    def test_start_routes_local_intake_to_frontier_codex(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.joinpath("T3").mkdir()
            root.joinpath("env").write_text("SWARM_NORNICDB_URL=http://remote:7474\nSWARM_REMOTE_OLLAMA_URL=http://remote:11434\n")
            with mock.patch.object(swarm_local, "paths", return_value=(root, root / "T3", root / "state")), \
                 mock.patch.object(swarm_local, "require_tools"), \
                 mock.patch.object(swarm_local, "configure_network", return_value=["--host", "0.0.0.0", "--port", "13773", "--dev-url", "http://192.168.1.231:5733"]), \
                 mock.patch.object(swarm_local, "request", return_value={"errors": []}), \
                 mock.patch.object(swarm_local, "configure_providers"), \
                 mock.patch.object(swarm_local.subprocess, "run") as process:
                swarm_local.start()
            launch_env = process.call_args.kwargs["env"]
            self.assertEqual(launch_env["NORNICDB_URI"], "http://remote:7474")
            self.assertEqual(launch_env["NORNIC_LOCAL_PROVIDER_INSTANCE"], "ollama_local")
            self.assertEqual(launch_env["NORNIC_FRONTIER_INSTANCES"], "codex")


if __name__ == "__main__":
    unittest.main()
