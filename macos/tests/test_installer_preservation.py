"""Exercise the postinstall script in an isolated filesystem with mocked launchd."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/postinstall"


class InstallerPreservationTests(unittest.TestCase):
    def exercise(self, existing):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            config = home / ".nornicdb"
            agents = home / "Library/LaunchAgents"
            config.mkdir(parents=True)
            agents.mkdir(parents=True)
            original = {config / "config.yaml": b"embedding:\n  provider: openai\n  model: text-embedding-3-small\n",
                        config / "environment": b"export CUSTOM_SETTING='keep this'\n",
                        agents / "com.nornicdb.server.plist": b"existing runtime settings"}
            if existing:
                for path, content in original.items():
                    path.write_bytes(content)
            application = root / "Applications/NornicDB.app/Contents/MacOS/NornicDB"
            application.parent.mkdir(parents=True)
            application.write_text("#!/bin/sh\nexit 0\n")
            application.chmod(0o755)
            default = root / "usr/local/share/nornicdb/default-config.yaml"
            default.parent.mkdir(parents=True)
            default.write_text("embedding:\n  enabled: false\n")
            commands = root / "commands"
            commands.mkdir()
            for name in ["chown", "launchctl", "open", "pkill"]:
                (commands / name).write_text("#!/bin/sh\nexit 0\n")
            (commands / "sudo").write_text('#!/bin/sh\nif [ "$1" = "-u" ]; then shift 2; fi\nexec "$@"\n')
            (commands / "launchctl").write_text('#!/bin/sh\nif [ "$1" = "bootstrap" ] && [ ! -f "$REVIEW_MARKER" ]; then exit 99; fi\nprintf "%s\\n" "$*" >> "$COMMAND_LOG"\nexit 0\n')
            (commands / "id").write_text("#!/bin/sh\necho 501\n")
            for command in commands.iterdir():
                command.chmod(0o755)
            text = SCRIPT.read_text()
            text = text.replace('USER_HOME=$(eval echo ~$ACTUAL_USER)', f'USER_HOME="{home}"')
            text = text.replace('/usr/local/', str(root / 'usr/local') + '/')
            text = text.replace('/Applications/', str(root / 'Applications') + '/')
            script = root / "postinstall"
            script.write_text(text)
            result = subprocess.run(["bash", str(script)], env={**os.environ, "USER": "fixture", "COMMAND_LOG": str(root / "launchctl.log"), "REVIEW_MARKER": str(config / ".setup_review"), "PATH": str(commands) + ":" + os.environ["PATH"]}, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            launch_calls = (root / "launchctl.log").read_text()
            self.assertTrue((config / ".setup_review").exists())
            self.assertNotIn("com.nornicdb.server", launch_calls)
            self.assertIn("bootstrap gui/501 " + str(agents / "com.nornicdb.menubar.plist"), launch_calls)
            import plistlib
            menu_agent = plistlib.loads((agents / "com.nornicdb.menubar.plist").read_bytes())
            self.assertEqual(menu_agent["ProgramArguments"], [str(application)])
            if existing:
                for path, content in original.items():
                    self.assertEqual(path.read_bytes(), content)
                self.assertFalse((config / ".first_run").exists())
            else:
                self.assertNotIn("com.nornicdb.server", launch_calls)
                self.assertEqual((config / "config.yaml").read_bytes(), default.read_bytes())
                self.assertTrue((config / ".first_run").exists())
                self.assertFalse((agents / "com.nornicdb.server.plist").exists())

    def test_upgrade_preserves_configuration_environment_and_arguments(self):
        self.exercise(existing=True)

    def test_new_install_waits_for_setup(self):
        self.exercise(existing=False)


if __name__ == "__main__":
    unittest.main()
