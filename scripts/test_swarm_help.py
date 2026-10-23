"""Deterministic guidance traversal and citation tests; no live services."""
from pathlib import Path
import subprocess
import tempfile
import unittest

import swarm_help as help_corpus


class HelpCorpusTests(unittest.TestCase):
    def test_transitive_guidance_handles_cycles_fragments_spaces_and_external_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)
            git("init", "-q")
            git("config", "user.name", "Fixture")
            git("config", "user.email", "fixture@example.invalid")
            (root / "docs").mkdir()
            (root / "AGENTS.md").write_text("[Guide](<docs/a guide.md#rules>) [external](https://example.invalid/x.md) [escape](../escape.md) [missing](docs/missing.md)")
            (root / "docs" / "a guide.md").write_text("[nested](nested.md) [home](../AGENTS.md)\n[ref]: nested.md#again\n")
            (root / "docs" / "nested.md").write_text("# Nested\n" + "x" * 2500)
            git("add", ".")
            git("commit", "-qm", "fixture")
            revision, paths = help_corpus.snapshot(root)
            selected, missing, external = help_corpus.guidance_paths(root, revision, paths)
            self.assertEqual(selected, ["AGENTS.md", "docs/a guide.md", "docs/nested.md"])
            self.assertIn("docs/missing.md", missing)
            self.assertTrue(any("outside repository" in entry for entry in missing))
            self.assertEqual(external, ["https://example.invalid/x.md"])
            # A dirty working tree must not silently change committed guidance.
            (root / "docs" / "nested.md").write_text("uncommitted")
            sources = [(root, "fixture/repo", revision, selected)]
            rows, manifest, digest = help_corpus.corpus_rows(sources)
            self.assertEqual((rows, manifest, digest), help_corpus.corpus_rows(sources))
            self.assertTrue(all(row["props"]["source_revision"] == revision for row in rows))
            self.assertTrue(all(row["props"]["source_repository"] == "fixture/repo" for row in rows))
            self.assertTrue(all(len(row["props"]["content"]) <= 2048 for row in rows))
            self.assertTrue(any("# Nested" in row["props"]["content"] for row in rows))
            self.assertEqual(len({row["id"] for row in rows}), len(rows))

    def test_missing_root_fails_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "no AGENTS.md"):
                help_corpus.guidance_paths(Path(directory), "HEAD", set())


if __name__ == "__main__":
    unittest.main()
