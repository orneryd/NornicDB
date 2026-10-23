"""Local model service contract tests; no model loads, downloads or launchd changes."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import swarm_model


class ModelServiceTests(unittest.TestCase):
    def test_native_context_and_persistent_mtp_server(self):
        args = swarm_model.server_arguments('/bin/llama-server', '/models/ornith.gguf', {})
        for flag, value in [('--ctx-size', '0'), ('--parallel', '1'), ('--spec-type', 'draft-mtp'), ('--sleep-idle-seconds', '-1')]:
            self.assertEqual(args[args.index(flag) + 1], value)
        self.assertEqual(args[args.index('--host') + 1], '127.0.0.1')
        self.assertNotIn('32768', args)

    def test_rejects_nonlocal_endpoints_and_context_beyond_the_model(self):
        for env in [{'SWARM_LOCAL_MODEL_URL': 'http://0.0.0.0:11435'}, {'SWARM_LOCAL_MODEL_URL': 'http://user:password@localhost:11435'}, {'SWARM_LOCAL_CONTEXT_WINDOW': '262145'}]:
            with self.assertRaises(RuntimeError):
                swarm_model.server_arguments('/binary', '/model', env)

    def test_artifact_size_and_digest_are_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            model = Path(directory) / 'model.gguf'
            model.write_bytes(b'fixture')
            with mock.patch.object(swarm_model, 'MODEL_BYTES', 7), mock.patch.object(swarm_model, 'MODEL_SHA256', hashlib.sha256(b'fixture').hexdigest()):
                swarm_model.verify_model(model)
                model.write_bytes(b'changed')
                with self.assertRaisesRegex(RuntimeError, 'checksum'):
                    swarm_model.verify_model(model)
                model.write_bytes(b'wrong-size')
                with self.assertRaisesRegex(RuntimeError, 'size'):
                    swarm_model.verify_model(model)


if __name__ == '__main__':
    unittest.main()
