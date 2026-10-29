"""Population-derived completion controls; no production subjects run here."""
import copy
import hashlib
import pathlib
import tempfile
import unittest

from completion import SITE_FIELDS, obligations, site_id


class CompletionPolicy(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name)
        self.proof = self.root / 'proof_test.go'
        self.proof.write_text('func TestRetainedSemantic() { assertEqual(expected, actual) }\n')
        (self.root / 'writer.go').write_text('func Emit() { return QuoteName(name) }\n')
        self.row = dict(Runtime='go', File='writer.go', Function='Emit', Callee='QuoteName',
                        Source='QuoteName(name)', Line=7, End=7, exercised=True)
        self.record = dict(File='writer.go', Line=7, Subject='proof_test.go#TestRetainedSemantic')
        self.program = dict(subject=self.record['Subject'], outcome='fixture#TestRetainedSemantic',
            observation='Exact decoded name and stored result equality.',
            sources={'proof_test.go': hashlib.sha256(self.proof.read_bytes()).hexdigest()},
            assertions=[dict(file='proof_test.go', source='assertEqual(expected, actual)')])
        self.identity = site_id(self.row)
        self.catalog = dict(programs={'proof': self.program}, rows={self.identity: dict(
            site={key: self.row[key] for key in SITE_FIELDS}, programs=['proof'],
            source_sha256=hashlib.sha256((self.root / 'writer.go').read_bytes()).hexdigest())}, exclusions={})

    def check(self, rows=None, records=None, passed=None, catalog=None):
        return obligations(rows if rows is not None else [self.row],
            records if records is not None else [self.record],
            passed if passed is not None else [self.program['outcome']],
            catalog if catalog is not None else self.catalog, self.root)

    def test_complete_requires_all_independent_evidence(self):
        result, errors = self.check()
        self.assertFalse(errors)
        self.assertEqual('complete', result[0]['status'])

    def test_new_executable_row_in_known_file_fails_even_with_reach(self):
        row = dict(self.row, Line=8, End=8)
        record = dict(self.record, Line=8)
        result, errors = self.check(rows=[self.row, row], records=[self.record, record])
        self.assertEqual(2, len(result))
        self.assertTrue(any('undispositioned executable row' in error for error in errors))

    def test_retained_test_symbol_without_emission_fails(self):
        _, errors = self.check(rows=[dict(self.row, exercised=False)], records=[])
        self.assertTrue(any('lacks recorded reach' in error for error in errors))

    def test_reach_without_passing_semantic_program_fails(self):
        _, errors = self.check(passed=[])
        self.assertTrue(any('semantic observation' in error for error in errors))

    def test_unrelated_passing_program_cannot_supply_semantics(self):
        _, errors = self.check(records=[dict(self.record, Subject='unrelated_test.go#TestUnrelated')])
        self.assertTrue(any('semantic observation' in error for error in errors))

    def test_retained_symbol_without_assertion_fails(self):
        self.proof.write_text('func TestRetainedSemantic() {}\n')
        _, errors = self.check()
        self.assertTrue(any('semantic observation' in error for error in errors))

    def test_observation_description_without_assertions_fails(self):
        catalog = copy.deepcopy(self.catalog)
        catalog['programs']['proof']['assertions'] = []
        _, errors = self.check(catalog=catalog)
        self.assertTrue(any('semantic observation' in error for error in errors))

    def test_unchanged_call_span_cannot_hide_a_changed_producer(self):
        (self.root / 'writer.go').write_text('func Emit() { name = trim(name); return QuoteName(name) }\n')
        _, errors = self.check()
        self.assertTrue(any('construction source changed' in error for error in errors))

    def test_source_disposition_does_not_create_an_exclusion(self):
        row = dict(self.row, exercised=False, source_disposition='presentation')
        catalog = dict(programs={}, rows={}, exclusions={})
        _, errors = self.check(rows=[row], records=[], catalog=catalog)
        self.assertTrue(any('undispositioned executable row' in error for error in errors))
        catalog['exclusions'][self.identity] = dict(site={key: row[key] for key in SITE_FIELDS}, reason='presentation')
        _, errors = self.check(rows=[row], records=[], catalog=catalog)
        self.assertTrue(any('reviewed narrow reason' in error for error in errors))
        catalog['exclusions'][self.identity]['review'] = dict(reference='review#accepted-presentation', head='reviewed-head')
        result, errors = self.check(rows=[row], records=[], catalog=catalog)
        self.assertFalse(errors)
        self.assertEqual('reviewed_exclusion', result[0]['status'])

    def test_exclusion_never_inherits_to_a_new_call(self):
        row = dict(self.row, Line=8, End=8, exercised=False)
        catalog = dict(programs={}, rows={}, exclusions={self.identity: dict(
            site={key: self.row[key] for key in SITE_FIELDS}, reason='presentation',
            review=dict(reference='review#accepted-presentation', head='reviewed-head'))})
        _, errors = self.check(rows=[row], records=[], catalog=catalog)
        self.assertTrue(any('undispositioned executable row' in error for error in errors))

    def test_empty_population_fails(self):
        _, errors = self.check(rows=[])
        self.assertIn('empty discovered obligation population', errors)


if __name__ == '__main__':
    unittest.main()
