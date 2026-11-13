"""Regression controls for reproducible issue-corpus qualification."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import extract_issue_corpus as extractor

PLAIN_AVERAGE = "MATCH (i:I) RETURN avg(i.price) AS av"
ORDERED_AVERAGE = "MATCH (i:I) WITH i ORDER BY i.sku RETURN avg(i.price) AS av"
ROUNDED_AVERAGE = "MATCH (i:I) RETURN round(avg(i.price) * 100) / 100 AS av"
CORPUS = Path(__file__).resolve().parents[3] / "testing/cypher/tck/testdata/differential/issues.json"


class IssueCorpusQualificationTests(unittest.TestCase):
    def render(self, issues):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "issues.json"
            with (
                mock.patch.object(extractor, "issues", return_value=issues),
                mock.patch.object(sys, "argv", ["extract_issue_corpus.py", str(output)]),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                extractor.main()
            return output.read_bytes()

    def original_issue(self):
        issue = next(row for row in json.loads(CORPUS.read_text()) if row["issue"] == 463)
        queries = [issue["statements"][0]["query"], PLAIN_AVERAGE, ROUNDED_AVERAGE]
        return {"number": 463, "title": issue["title"],
                "body": "```cypher\n" + ";\n".join(queries) + ";\n```"}

    def test_average_replay_has_repeatable_explicit_input_order(self):
        first = self.render([self.original_issue()])
        self.assertEqual(first, self.render([self.original_issue()]))
        statements = json.loads(first)[0]["statements"]
        self.assertEqual(3, len(statements), "retain setup and both aggregate controls")
        self.assertEqual("5f4686ca28f2", statements[0]["id"], "retain setup identity")
        self.assertEqual(ORDERED_AVERAGE, statements[1]["query"])
        expected_id = hashlib.sha1(f"463:1:{ORDERED_AVERAGE}".encode()).hexdigest()[:12]
        self.assertEqual(expected_id, statements[1]["id"], "hash the actual replay text")
        self.assertEqual(ROUNDED_AVERAGE, statements[2]["query"])
        self.assertEqual("72df9219afac", statements[2]["id"], "retain the original wrapped-aggregate regression")

    def test_ordering_is_scoped_to_the_reviewed_statement(self):
        for number, query in [
            (464, PLAIN_AVERAGE),
            (463, "MATCH (i:I) RETURN avg(i.qty) AS av"),
            (463, "RETURN 'MATCH (i:I) RETURN avg(i.price) AS av' AS text"),
        ]:
            with self.subTest(issue=number, query=query):
                issue = {"number": number, "title": "unrelated",
                         "body": "```cypher\n" + query + "\n```"}
                output = json.loads(self.render([issue]))
                self.assertEqual(query, output[0]["statements"][0]["query"])
                expected_id = hashlib.sha1(f"{number}:0:{query}".encode()).hexdigest()[:12]
                self.assertEqual(expected_id, output[0]["statements"][0]["id"])

    def test_checked_in_fixture_matches_reextraction(self):
        original = self.original_issue()
        actual = json.loads(self.render([original]))[0]
        checked_in = next(row for row in json.loads(CORPUS.read_text()) if row["issue"] == 463)
        self.assertEqual(checked_in, actual)


if __name__ == "__main__":
    unittest.main()
