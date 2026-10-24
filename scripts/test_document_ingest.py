import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import document_ingest as docs
import graphify_local as gi


class DocumentIngestionTests(unittest.TestCase):
    def test_chunk_bounds_preserve_unicode_whitespace_and_line_citations(self):
        text = "\n# API\n" + "界" * 4100 + "\nfinal\n"
        chunks = list(docs.document_chunks(text))
        self.assertEqual("".join(chunk[2] for chunk in chunks), text)
        self.assertTrue(all(0 < len(chunk[2]) <= 2048 for chunk in chunks))
        self.assertEqual(chunks[0][0], 1)
        self.assertEqual(chunks[-1][1], 4)

    def test_nested_documents_are_ingested_from_git_with_stable_ids(self):
        class Result:
            def __init__(self, row=None):
                self.row = row
            def single(self):
                return self.row
            def consume(self):
                return None

        class Driver(gi.HttpDriver):
            def __init__(self):
                self.rows = []
            def session(self, database=None):
                return self
            def run(self, query, **params):
                if "RETURN r.owner" in query:
                    return Result({"owner": params["owner"]})
                if "UNWIND $rows" in query:
                    self.rows.extend(params["rows"])
                    return Result({"count": len(params["rows"])})
                if "RETURN r.last_commit" in query:
                    return Result({"commit": params["commit"]})
                return Result()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                subprocess.run(["git", "-C", str(root), *args], check=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            git("init", "-q")
            git("config", "user.name", "Fixture")
            git("config", "user.email", "fixture@example.invalid")
            (root / "docs").mkdir()
            (root / "README.md").write_text("\n# Root\n", encoding="utf-8")
            (root / "docs" / "nested.md").write_text("# Nested\n" + "界" * 4100 + "\n", encoding="utf-8")
            (root / "source.py").write_text("answer = 42\n", encoding="utf-8")
            git("add", ".")
            git("commit", "-qm", "fixture")
            driver = Driver()
            with patch.object(docs.ci, "connection", return_value=("http://localhost", "u", "p", "")), \
                    patch.object(docs.ci, "identity", return_value=("fixture/docs", "code")), \
                    patch.object(docs.gi, "connect", return_value=driver), contextlib.redirect_stdout(io.StringIO()):
                docs.ingest_documents(root, "HEAD")
                first = list(driver.rows)
                driver.rows = []
                # A new revision with unchanged docs must reuse their chunk identities.
                (root / "source.py").write_text("answer = 43\n", encoding="utf-8")
                git("add", ".")
                git("commit", "-qm", "code only")
                docs.ingest_documents(root, "HEAD")
            self.assertEqual({row["props"]["source_file"] for row in first}, {"README.md", "docs/nested.md"})
            self.assertEqual(first, driver.rows)
            self.assertEqual(len({row["id"] for row in first}), len(first))
            self.assertEqual(first[0]["props"]["content"], "\n# Root\n")


if __name__ == "__main__":
    unittest.main()
