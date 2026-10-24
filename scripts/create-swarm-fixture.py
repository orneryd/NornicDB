"""Create a new, isolated Git fixture for local workflow acceptance (no hooks)."""
import argparse
from pathlib import Path
import subprocess

from test_code_intelligence_hooks import BODY


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    target = parser.parse_args().destination.resolve()
    if target.exists():
        parser.error("Destination already exists; choose a new directory. Nothing was changed.")
    target.mkdir(parents=True)
    files = {
        "calc.py": "def square(value):\n    return value + value\n",
        "canonical.py": BODY,
        "candidate.py": "# Reserved for the duplicate-gate acceptance fixture.\n",
        "test_calc.py": "import unittest\nfrom calc import square\n\nclass SquareTests(unittest.TestCase):\n    def test_square(self):\n        self.assertEqual(square(3), 9)\n        self.assertEqual(square(-4), 16)\n",
        ".gitignore": "__pycache__/\n.no-hooks/\n",
    }
    for name, content in files.items():
        (target / name).write_text(content, encoding="utf-8", newline="\n")
    def git(*args):
        subprocess.run(["git", "-C", str(target), *args], check=True)
    git("init", "-b", "main")
    git("config", "user.name", "Workflow acceptance fixture")
    git("config", "user.email", "fixture@example.invalid")
    git("config", "core.hooksPath", str(target / ".no-hooks"))
    git("add", ".")
    git("commit", "-m", "Create bounded workflow acceptance fixture")
    print(f"Created {target}. The square unittest intentionally fails until the worker fixes calc.py.")


if __name__ == "__main__":
    main()
