"""HTTP transport authentication: bearer token vs basic, and where a token may be sent (stdlib unittest)."""
import base64
import os
import subprocess
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
import graphify_local as gi  # noqa: E402


class HttpAuthTests(unittest.TestCase):
    def test_basic_by_default(self):
        driver = gi.connect("https://db.example.com", "admin", "pw")
        self.assertEqual(driver.auth_header, "Basic " + base64.b64encode(b"admin:pw").decode())

    def test_bearer_token_replaces_user_and_password(self):
        driver = gi.connect("https://db.example.com/", "admin", "pw", "abc.def.ghi")
        self.assertEqual(driver.auth_header, "Bearer abc.def.ghi")
        self.assertEqual(driver.base_url, "https://db.example.com")

    def test_token_is_never_sent_over_plain_http_to_a_remote_host(self):
        with self.assertRaises(SystemExit) as raised:
            gi.connect("http://db.example.com", "admin", "pw", "tok")
        self.assertIn("plain http", str(raised.exception))
        # loopback is fine (local development)
        self.assertEqual(gi.connect("http://localhost:7474", "a", "b", "tok").auth_header, "Bearer tok")

    def test_token_is_http_only(self):
        with self.assertRaises(SystemExit):
            gi.connect("bolt://db.example.com:7687", "admin", "pw", "tok")

    def test_exported_token_is_not_sent_to_an_overridden_local_server(self):
        """A NORNICDB_TOKEN exported for a remote server must not leak into http://localhost runs."""
        code = (
            "import sys; sys.path.insert(0, %r); import graphify_local as gi\n"
            "seen = []\n"
            "gi.last_ingested_state = lambda uri, u, p, d, r, token=None: seen.append(token)\n"
            "sys.argv = ['x', '--check', '--repo', 'o/r', '--uri', %r]\n"
            "try:\n    gi.main()\nexcept SystemExit:\n    pass\n"
            "print('TOKEN', seen)\n"
        )
        env = {**os.environ, "NORNICDB_URI": "https://db.example.com", "NORNICDB_TOKEN": "remote-only", "NORNICDB_USER": "test", "NORNICDB_PASSWORD": "fixture"}
        for uri, expected in (("http://localhost:7474", "[None]"), ("https://db.example.com", "['remote-only']")):
            out = subprocess.run([sys.executable, "-c", code % (str(SCRIPTS), uri)], env=env,
                                 capture_output=True, text=True).stdout
            self.assertIn("TOKEN " + expected, out, uri)


if __name__ == "__main__":
    unittest.main()
