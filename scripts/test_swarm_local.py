"""Tests for the local T3 launcher without network or model downloads."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import swarm_local


class SwarmLocalTest(unittest.TestCase):
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
            self.assertEqual(saved["providerInstances"]["ollama_local"]["displayName"], "Nornic Local 27B")
            self.assertEqual(saved["providerInstances"]["ollama_remote"]["displayName"], "Nornic Remote 9B")
            self.assertEqual(saved["defaultModelSelection"]["model"], "gpt-6.1-sol")
            self.assertEqual(saved["defaultModelSelection"]["options"], [{"id": "reasoningEffort", "value": "low"}])
            models = json.loads((root / "pi-state" / "models.json").read_text())
            self.assertEqual(models["providers"]["custom"], {"baseUrl": "http://custom"})
            self.assertEqual(models["providers"]["ollamaRemote"]["baseUrl"], "http://remote:11434/v1")
            self.assertEqual(models["providers"]["ollamaLocal"]["models"][0]["id"], "qwen3.5:27b")

    def test_token_takes_precedence_over_basic_auth(self):
        self.assertEqual(
            swarm_local.auth_headers({"NORNICDB_TOKEN": "secret", "NORNICDB_USER": "u", "NORNICDB_PASSWORD": "p"})["Authorization"],
            "Bearer secret",
        )
        self.assertEqual(
            swarm_local.auth_headers({"NORNICDB_USER": "u", "NORNICDB_PASSWORD": "p"})["Authorization"],
            "Basic dTpw",
        )

    def test_astra_cannot_remain_the_implicit_default(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = root / "state" / "userdata" / "settings.json"
            swarm_local.write_json(settings, {
                "defaultModelSelection": {"instanceId": "codex", "model": "gpt-6-astra"},
            })
            with mock.patch.object(swarm_local.shutil, "which", return_value="/bin/codex"):
                swarm_local.configure_providers(root, {})
            self.assertEqual(json.loads(settings.read_text())["defaultModelSelection"]["model"], "gpt-6.1-sol")

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
            root.joinpath("env").write_text("SWARM_NORNICDB_URL=http://remote:7474\n")
            with mock.patch.object(swarm_local, "paths", return_value=(root, root / "T3", root / "state")), \
                 mock.patch.object(swarm_local, "require_tools"), \
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
