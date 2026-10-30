"""Failures of the check step surface as one readable line (real HTTP)."""
from scripts import graphify_local as gi
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer


class FakeNornicDB(BaseHTTPRequestHandler):
    """Answers /db/<name>/tx/commit like NornicDB: 401 for bad credentials, 404 DatabaseNotFound, or rows."""
    mode = "ok"

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.mode == "unauthorized":
            status, body = 401, {"results": [], "errors": [{"code": "Neo.ClientError.Security.Unauthorized",
                                                              "message": "invalid credentials"}]}
        elif self.mode == "no_database":
            status, body = 404, {"results": [], "errors": [{"code": "Neo.ClientError.Database.DatabaseNotFound",
                                                              "message": "Database 'x' not found"}]}
        elif self.mode == "server_error":
            status, body = 500, {"results": [], "errors": [{"code": "Neo.DatabaseError.General.UnknownError",
                                                              "message": "boom"}]}
        else:
            status, body = 200, {"results": [{"columns": ["commit", "main_spec"], "data": [{"row": ["abc", ""]}]}],
                                 "errors": []}
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class CheckStepErrors(unittest.TestCase):
    def serve(self, mode):
        FakeNornicDB.mode = mode
        server = HTTPServer(("127.0.0.1", 0), FakeNornicDB)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}"

    def state(self, uri):
        return gi.last_ingested_state(uri, "u", "p", "db", "o/r")

    def test_reads_the_recorded_commit(self):
        self.assertEqual(self.state(self.serve("ok")), {"commit": "abc", "main_spec": ""})

    def test_a_missing_database_is_just_the_first_run(self):
        self.assertIsNone(self.state(self.serve("no_database")))

    def test_bad_credentials_fail_the_check_with_one_clear_line(self):
        with self.assertRaises(SystemExit) as raised:
            self.state(self.serve("unauthorized"))
        message = str(raised.exception)
        self.assertIn("rejected the credentials", message)
        self.assertNotIn("Traceback", message)
        self.assertEqual(len(message.splitlines()), 1)

    def test_a_server_error_fails_the_check_instead_of_assuming_a_first_run(self):
        with self.assertRaises(SystemExit) as raised:
            self.state(self.serve("server_error"))
        self.assertIn("returned an error", str(raised.exception))

    def test_an_unreachable_server_fails_the_check(self):
        server = HTTPServer(("127.0.0.1", 0), FakeNornicDB)
        port = server.server_port
        server.server_close()  # nothing listens on this port any more
        with self.assertRaises(SystemExit) as raised:
            self.state(f"http://127.0.0.1:{port}")
        self.assertIn("cannot reach NornicDB", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
