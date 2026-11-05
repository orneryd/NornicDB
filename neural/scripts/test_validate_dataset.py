"""Tests for neural/scripts/validate_dataset.py structural delimiter counting.

The unbalanced-delimiter check must ignore delimiter characters that sit in
Cypher lexical data (quoted strings, backtick identifiers, `//` and `/* */`
comments), using the same extent rules as internal/cyphertext (ScanQuotedText
and CommentEndStatus) and its browser peer ui/src/utils/cypherText.mjs, whose
shared statement_spans.json fixtures exercise the same language boundary.
The validator stays a standalone offline Python tool: it needs no Go, Node,
database or non-standard dependency to run these tests.

Run from the repository root:

    python3 -m unittest neural.scripts.test_validate_dataset
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from neural.scripts.validate_dataset import (
    count_structural_delimiters,
    lexical_span_end,
    validate_cypher_syntax,
)

SCRIPT = Path(__file__).with_name("validate_dataset.py")

DELIMITERS = ("(", ")", "[", "]", "{", "}")

# Each template puts one delimiter character ({d}) inside lexical data and is
# otherwise structurally balanced, so counting that delimiter is a false
# positive.
LEXICAL_CONTEXTS = {
    "single-quoted string": "MATCH (n:Person) RETURN n.name AS x, '{d}' AS s",
    "double-quoted string": 'MATCH (n:Person) RETURN n.name AS x, "{d}" AS s',
    "backtick identifier": "MATCH (n:Person) RETURN 1 AS `a{d}b`",
    "line comment": "MATCH (n:Person) RETURN n // {d}",
    "block comment": "MATCH (n:Person) /* {d} */ RETURN n",
}


def example(output):
    return {"instruction": "Write a Cypher query", "output": output}


def structural_issues(output):
    """The unbalanced-delimiter issues reported for one example output."""
    return [issue for issue in validate_cypher_syntax([example(output)])
            if "Unbalanced" in issue]


class StructuralDelimiterCountTest(unittest.TestCase):
    """Delimiter counts skip lexical data and still catch real imbalance."""

    def test_every_delimiter_in_every_lexical_context_is_ignored(self):
        for context, template in LEXICAL_CONTEXTS.items():
            baseline = count_structural_delimiters(template.format(d="x"))
            for delimiter in DELIMITERS:
                with self.subTest(context=context, delimiter=delimiter):
                    self.assertEqual(
                        count_structural_delimiters(template.format(d=delimiter)),
                        baseline,
                    )
                    self.assertEqual(structural_issues(template.format(d=delimiter)), [])

    def test_one_scan_counts_all_three_pairs(self):
        self.assertEqual(
            count_structural_delimiters("(a) [b] {c}"),
            {"(": 1, ")": 1, "[": 1, "]": 1, "{": 1, "}": 1},
        )
        self.assertEqual(
            count_structural_delimiters("([{ }])"),
            {"(": 1, ")": 1, "[": 1, "]": 1, "{": 1, "}": 1},
        )

    def test_real_imbalance_still_reports_each_delimiter_pair(self):
        cases = [
            ("MATCH (n:Person RETURN n", "Line 1: Unbalanced parentheses"),
            ("MATCH (n) RETURN n[0", "Line 1: Unbalanced brackets"),
            ("MATCH (n) RETURN {a: 1", "Line 1: Unbalanced braces"),
            ("MATCH (n)) RETURN n", "Line 1: Unbalanced parentheses"),
            ("MATCH (n) RETURN n]", "Line 1: Unbalanced brackets"),
            ("MATCH (n) RETURN n}", "Line 1: Unbalanced braces"),
        ]
        for output, expected in cases:
            with self.subTest(output=output):
                self.assertEqual(structural_issues(output), [expected])


class EscapeAndExtentTest(unittest.TestCase):
    """Quoted-token and comment extents follow the canonical lexical readers."""

    def test_backslash_escaped_quote_keeps_delimiter_inside_string(self):
        cases = [
            "MATCH (n:Person) RETURN 'a\\'(b' AS x",
            'MATCH (n:Person) RETURN "a\\"(b" AS x',
        ]
        for output in cases:
            with self.subTest(output=output):
                self.assertEqual(structural_issues(output), [])

    def test_escaped_quote_does_not_end_the_string(self):
        self.assertEqual(lexical_span_end("'a\\'b'( )", 0), 6)
        self.assertEqual(lexical_span_end('"a\\"b"( )', 0), 6)

    def test_doubled_backtick_escapes_identifier_delimiter(self):
        self.assertEqual(lexical_span_end("`a``b` )", 0), 6)
        self.assertEqual(lexical_span_end("`a``(b` )", 0), 7)
        self.assertEqual(
            count_structural_delimiters("`a``(b` )"),
            {"(": 0, ")": 1, "[": 0, "]": 0, "{": 0, "}": 0},
        )

    def test_backslash_does_not_escape_a_backtick_identifier(self):
        self.assertEqual(lexical_span_end(r"`a\` )", 0), 4)
        self.assertEqual(
            structural_issues(r"RETURN 1 AS `a\` )"),
            ["Line 1: Unbalanced parentheses"],
        )

    def test_doubled_quote_starts_a_second_string_literal(self):
        # statement_spans.json: RETURN 'a'';b';RETURN 2 keeps two adjacent
        # single-quoted strings; the doubled quote is not an escape.
        self.assertEqual(lexical_span_end("'a''b'", 0), 3)
        self.assertEqual(lexical_span_end("'a''b'", 3), 6)

    def test_comment_marker_inside_string_is_data(self):
        # A // or /* inside a string does not start a comment, so the trailing
        # delimiter is still structural and still unbalanced.
        self.assertEqual(
            structural_issues("MATCH (n:Person) RETURN '//' AS x )"),
            ["Line 1: Unbalanced parentheses"],
        )
        self.assertEqual(
            structural_issues("MATCH (n:Person) RETURN '/*' AS x )"),
            ["Line 1: Unbalanced parentheses"],
        )

    def test_quote_inside_comment_is_data(self):
        self.assertEqual(structural_issues("MATCH (n:Person) RETURN n // it's )"), [])
        self.assertEqual(structural_issues("MATCH (n:Person) /* it's */ RETURN n"), [])

    def test_line_comment_ends_at_line_break(self):
        for newline in ("\n", "\r", "\r\n"):
            with self.subTest(newline=repr(newline)):
                self.assertEqual(
                    structural_issues("MATCH (n:Person) // note" + newline + "RETURN n)"),
                    ["Line 1: Unbalanced parentheses"],
                )

    def test_block_comment_ends_at_first_close(self):
        self.assertEqual(
            structural_issues("MATCH (n:Person) /* note */ RETURN n)"),
            ["Line 1: Unbalanced parentheses"],
        )

    def test_plain_text_is_not_a_lexical_span(self):
        self.assertEqual(lexical_span_end("MATCH (n) RETURN n", 0), -1)


class ValidatorCliTest(unittest.TestCase):
    """The public CLI keeps its line attribution, exit codes and flag behavior."""

    def run_validator(self, outputs, *args):
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "dataset.jsonl"
            dataset.write_text(
                "".join(json.dumps(example(output)) + "\n" for output in outputs),
                encoding="utf-8",
            )
            completed = subprocess.run(
                [sys.executable, str(SCRIPT), str(dataset), *args],
                capture_output=True, text=True, check=False,
            )
        # The fixture file and its directory are removed with the context.
        self.assertFalse(dataset.exists())
        return completed

    def test_cli_attributes_imbalance_to_its_line_and_accepts_siblings(self):
        completed = self.run_validator([
            "MATCH (n:Person {name: 'Ada'}) RETURN n",
            "MATCH (n:Person RETURN n",
            "MATCH (n:Person) // keep {1}\nRETURN n AS `odd name`",
        ], "--check-cypher")
        self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
        reported = [line for line in completed.stdout.splitlines()
                    if "Unbalanced" in line]
        self.assertEqual([issue.strip() for issue in reported],
                         ["Line 2: Unbalanced parentheses"])
        self.assertNotIn("Line 1:", completed.stdout)
        self.assertNotIn("Line 3:", completed.stdout)

    def test_cli_exits_zero_for_quoted_and_commented_delimiters(self):
        completed = self.run_validator([
            "MATCH (n:Person {name: 'a{b'}) RETURN n",
            "MATCH (n:Person) RETURN n // ( unbalanced in a comment",
            "MATCH (n:Person) /* ] unbalanced in a comment */ RETURN n",
            "MATCH (n) RETURN n[`x]y`]",
            "MATCH (n:Person) RETURN 'it\\'s ( fine' AS x",
            "MATCH (n:Person) RETURN 1 AS `a``b`",
        ], "--check-cypher")
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertNotIn("Unbalanced", completed.stdout)

    def test_cli_without_check_cypher_skips_structural_checks(self):
        completed = self.run_validator([
            "MATCH (n:Person {name: 'a{b'}) RETURN n",
            "MATCH (n:Person RETURN n",
        ])
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertNotIn("Unbalanced", completed.stdout)
        self.assertNotIn("Cypher Syntax", completed.stdout)


if __name__ == "__main__":
    unittest.main()
