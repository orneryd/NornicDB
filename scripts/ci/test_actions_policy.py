import copy
import runpy
from pathlib import Path
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[2]
policy = runpy.run_path(str(ROOT / 'scripts/check-actions-policy.py'))


class PolicyTests(unittest.TestCase):
    def load(self, name):
        return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())

    def test_release_requires_all_guarded_jobs(self):
        name = 'release-verification.yml'
        workflow = self.load(name)
        self.assertFalse(policy['violations'](name, workflow))
        for mutate in ('guard', 'dependency', 'final'):
            broken = copy.deepcopy(workflow)
            if mutate == 'guard':
                broken['jobs']['prepare'].pop('if')
            elif mutate == 'dependency':
                broken['jobs']['checks'].pop('needs')
            else:
                broken['jobs']['verify']['needs'].remove('tests')
            self.assertTrue(policy['violations'](name, broken), mutate)

    def test_graphify_policy_requires_gate_and_secret_isolation(self):
        name = 'release-graphify-pr.yml'
        workflow = self.load(name)
        self.assertFalse(policy['violations'](name, workflow))
        broken = copy.deepcopy(workflow)
        broken['jobs']['extract']['steps'].append({'run': 'echo ignored', 'env': {'TOKEN': '${{ secrets.TOKEN }}'}})
        self.assertTrue(policy['violations'](name, broken))
        broken = copy.deepcopy(workflow)
        broken['jobs']['authorize']['steps'][-1]['run'] = 'echo bypass'
        self.assertTrue(policy['violations'](name, broken))

    def test_manual_ci_requires_one_approved_plan_for_every_job(self):
        name = 'ci.yml'
        workflow = self.load(name)
        self.assertFalse(policy['violations'](name, workflow))
        for job in ('test', 'coverage-groups', 'coverage', 'make-test-shards', 'make-test-lint', 'make-test'):
            broken = copy.deepcopy(workflow)
            broken['jobs'][job].pop('needs')
            self.assertTrue(policy['violations'](name, broken), job)
        broken = copy.deepcopy(workflow)
        broken['jobs']['coverage']['if'] = "${{ always() }}"
        self.assertTrue(policy['violations'](name, broken))

    def test_ci_test_jobs_enable_cgo_and_do_not_disable_local_llm(self):
        for name, jobs in (("ci.yml", ("coverage-groups", "make-test-shards")),
                           ("release-verification.yml", ("tests", "checks")),
                           ("cypher-conformance.yml", ("allocations", "conformance")),
                           ("cypher-differential-ratchet.yml", ("ratchet",))):
            workflow = self.load(name)
            for job in jobs:
                self.assertEqual(str(workflow["jobs"][job]["env"]["CGO_ENABLED"]), "1", (name, job))
        for path in ("scripts/cypher-tck/run-differential.sh", "scripts/cypher-tck/allocation_ratchet.py"):
            self.assertNotIn("nolocalllm", (ROOT / path).read_text())
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        self.assertIn("CYPHER_TCK_TAGS := noui\n", makefile)
        self.assertIn("./pkg/heimdall/...", (ROOT / "scripts/ci/test-groups.sh").read_text())

    def test_ingestion_policy_requires_authorization(self):
        name = 'release-graphify-ingest.yml'
        workflow = self.load(name)
        self.assertFalse(policy['violations'](name, workflow))
        workflow['jobs']['ingest'].pop('needs')
        self.assertTrue(policy['violations'](name, workflow))


if __name__ == '__main__':
    unittest.main()
