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

    def test_private_env_does_not_execute_or_override_process_values(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "env").write_text("SWARM_REMOTE_OLLAMA_URL=http://remote:11434\nNORNICDB_TOKEN=from-file\n")
            with mock.patch.dict(os.environ, {"NORNICDB_TOKEN": "from-process"}):
                env = swarm_local.private_env(root)
            self.assertEqual(env["NORNICDB_TOKEN"], "from-process")
            self.assertEqual(env["SWARM_REMOTE_OLLAMA_URL"], "http://remote:11434")


if __name__ == "__main__":
    unittest.main()
