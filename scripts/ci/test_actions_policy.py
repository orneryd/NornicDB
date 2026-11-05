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
                broken['jobs']['verify']['needs'].remove('coverage')
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

    def test_ingestion_policy_requires_authorization(self):
        name = 'release-graphify-ingest.yml'
        workflow = self.load(name)
        self.assertFalse(policy['violations'](name, workflow))
        workflow['jobs']['ingest'].pop('needs')
        self.assertTrue(policy['violations'](name, workflow))


if __name__ == '__main__':
    unittest.main()
