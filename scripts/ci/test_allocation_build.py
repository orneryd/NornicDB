"""Exercise the allocation runner's local-LLM test configuration."""
import importlib.util
import sys
import tempfile
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('allocation_ratchet', ROOT / 'scripts/cypher-tck/allocation_ratchet.py')
ratchet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ratchet)


class AllocationBuildTests(unittest.TestCase):
    def test_benchmarks_do_not_disable_local_llm(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            completed = mock.Mock(returncode=0, stdout='', stderr='')
            with mock.patch.object(ratchet.subprocess, 'run', return_value=completed) as run, \
                 mock.patch.object(ratchet, 'parse_samples', return_value={}):
                ratchet.run_benchmarks(root, 'head', root, 'nornic', 1, 1)
            self.assertGreater(run.call_count, 0)
            for call in run.call_args_list:
                command = call.args[0]
                self.assertIn('-tags=noui', command)
                self.assertNotIn('nolocalllm', ' '.join(command))

    def test_linux_hydrates_each_checkout_before_running(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            base, head, artifacts = (root / name for name in ('base', 'head', 'artifacts'))
            base.mkdir(); head.mkdir()
            argv = ['allocation_ratchet.py', '--base', str(base), '--head', str(head),
                    '--artifacts', str(artifacts), '--operations', '1', '--samples', '1']
            with mock.patch.object(sys, 'argv', argv), mock.patch.object(ratchet.platform, 'system', return_value='Linux'), \
                 mock.patch.object(ratchet.platform, 'machine', return_value='x86_64'), \
                 mock.patch.object(ratchet.subprocess, 'run') as run, \
                 mock.patch.object(ratchet, 'install_public_execute_fixture'), \
                 mock.patch.object(ratchet, 'run_benchmarks', return_value={}), \
                 mock.patch.object(ratchet, 'compare_allocations', return_value=[]):
                self.assertEqual(ratchet.main(), 0)
            self.assertEqual([call.kwargs['cwd'] for call in run.call_args_list], [base.resolve(), head.resolve()])
            self.assertTrue(all(call.args[0] == ['bash', 'scripts/hydrate-llama-cpu-libs.sh'] for call in run.call_args_list))


if __name__ == '__main__':
    unittest.main()
