"""Execute preinstall only against disposable payload/config paths."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/preinstall"


class PreinstallCleanupTests(unittest.TestCase):
    def exercise(self, busy=False, legacy_id="com.nornicdb.menubar", symlink=False, busy_server=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            application = root / "Applications/NornicDB.app"
            application.mkdir(parents=True)
            (application / "old-executable").write_text("old payload")
            if symlink:
                target = root / "other-application"
                application.rename(target)
                application.symlink_to(target, target_is_directory=True)
            binary = root / "usr/local/bin/nornicdb"
            binary.parent.mkdir(parents=True)
            binary.write_text("old server")
            legacy = home / "Applications/NornicDB.app/Contents"
            legacy.mkdir(parents=True)
            (legacy / "Info.plist").write_text("fixture")
            protected = [home / ".nornicdb/config.yaml", home / ".nornicdb/environment",
                         root / "usr/local/var/nornicdb/data/node", root / "usr/local/var/nornicdb/models/model.gguf",
                         home / "Library/LaunchAgents/com.nornicdb.server.plist"]
            for path in protected:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("keep exactly")
            commands = root / "commands"
            commands.mkdir()
            for name, script in {
                "sudo": 'if [ "$1" = "-u" ]; then shift 2; fi\nexec "$@"',
                "id": "echo 501", "sleep": "exit 0", "osascript": "exit 0", "killall": "exit 0",
                "pgrep": ('case "$*" in *nornicdb) exit 0;; *) exit 1;; esac' if busy_server else "exit " + ("0" if busy else "1")),
                "pkill": 'echo "pkill $*" >> "$COMMAND_LOG"',
                "launchctl": 'echo "launchctl $*" >> "$COMMAND_LOG"',
                "PlistBuddy": "echo " + legacy_id,
            }.items():
                path = commands / name
                path.write_text("#!/bin/sh\n" + script + "\n")
                path.chmod(0o755)
            text = SCRIPT.read_text().replace('USER_HOME=$(eval echo ~$ACTUAL_USER)', f'USER_HOME="{home}"')
            text = text.replace('/usr/local/', str(root / 'usr/local') + '/')
            text = text.replace(' /Applications/', ' ' + str(root / 'Applications') + '/')
            text = text.replace('/usr/libexec/PlistBuddy', str(commands / 'PlistBuddy'))
            script = root / "preinstall"
            script.write_text(text)
            result = subprocess.run(["bash", str(script)], capture_output=True, text=True,
                env={**os.environ, "USER": "fixture", "COMMAND_LOG": str(root / "commands.log"), "PATH": str(commands) + ":" + os.environ["PATH"]})
            calls = (root / "commands.log").read_text()
            self.assertIn("bootout gui/501/com.nornicdb.menubar", calls)
            self.assertIn("bootout gui/501/com.nornicdb.server", calls)
            for path in protected:
                self.assertEqual(path.read_text(), "keep exactly")
            if busy_server:
                self.assertNotIn("pkill -KILL -u 501 -x nornicdb", calls)
            if busy or symlink or busy_server:
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(application.exists())
                self.assertTrue(binary.exists())
            else:
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse(application.exists())
                self.assertFalse(binary.exists())
                self.assertEqual(legacy.parent.exists(), legacy_id != "com.nornicdb.menubar")

    def test_replaces_only_executable_payloads_after_stopping_both_agents(self):
        self.exercise()

    def test_retains_an_unrelated_app_in_the_legacy_location(self):
        self.exercise(legacy_id="com.example.Unrelated")

    def test_refuses_symlinked_installed_bundle(self):
        self.exercise(symlink=True)

    def test_refuses_replacement_instead_of_force_killing_a_busy_server(self):
        self.exercise(busy_server=True)

    def test_refuses_to_replace_payloads_if_an_old_process_wont_stop(self):
        self.exercise(busy=True)


if __name__ == "__main__":
    unittest.main()
