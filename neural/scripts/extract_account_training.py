#!/usr/bin/env python3
"""
Extracts text from a PDF and converts it into JSONL training data for
an account management representative model.

Output format:
{"instruction": "...", "input": "...", "output": "..."}
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import List, Tuple

try:
    from pypdf import PdfReader
except ModuleNotFoundError as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: pypdf\n\n"
        "Install just the PDF extractor deps:\n"
        "  python3 -m pip install -r neural/requirements-extract.txt\n\n"
        "Or install only pypdf:\n"
        "  python3 -m pip install pypdf\n\n"
        "If you see 'externally-managed-environment' (Homebrew/PEP 668), use a venv:\n"
        "  python3 -m venv neural/.venv-extract\n"
        "  source neural/.venv-extract/bin/activate\n"
        "  python -m pip install -r neural/requirements-extract.txt\n"
        "  python neural/scripts/extract_account_training.py --pdf \"Account Management New Hire Training Manual 1.pdf\" --output neural/data/account_management_train.jsonl\n"
    ) from e


DEFAULT_INSTRUCTION = (
    "You are an account management representative. Answer using the training manual."
)


def extract_pages(pdf_path: Path) -> List[str]:
    reader = PdfReader(str(pdf_path))
    pages = []
    for page in reader.pages:
        text = page.extract_text() or ""
        pages.append(text)
    return pages


def split_lines(text: str) -> List[str]:
    lines = []
    for raw in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw.strip()
        if line:
            lines.append(line)
    return lines


def remove_repeated_lines(pages: List[str]) -> List[List[str]]:
    page_lines = [set(split_lines(p)) for p in pages]
    counts = Counter()
    for lines in page_lines:
        counts.update(lines)

    total = len(pages)
    threshold = max(3, int(total * 0.6))
    repeated = {
        line
        for line, count in counts.items()
        if count >= threshold and len(line) <= 120
    }
    cleaned = []
    for lines in page_lines:
        cleaned.append([line for line in lines if line not in repeated])
    return cleaned


def merge_hyphenated(lines: List[str]) -> List[str]:
    merged = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.endswith("-") and i + 1 < len(lines):
            merged.append(line[:-1] + lines[i + 1])
            i += 2
            continue
        merged.append(line)
        i += 1
    return merged


def is_heading(line: str) -> bool:
    if len(line) < 4 or len(line) > 90:
        return False
    if line.endswith("."):
        return False
    if line.isupper():
        return True
    if line.endswith(":"):
        return True
    if re.match(r"^\d+(\.\d+)*\s+[A-Za-z].+$", line):
        return True
    words = line.split()
    if len(words) <= 6 and all(w[:1].isupper() for w in words if w.isalpha()):
        return True
    return False


def extract_sections(lines: List[str]) -> List[Tuple[str, List[str]]]:
    sections = []
    title = "Overview"
    buffer: List[str] = []
    for line in lines:
        if is_heading(line):
            if buffer:
                sections.append((title, buffer))
            title = line.rstrip(":")
            buffer = []
        else:
            buffer.append(line)
    if buffer:
        sections.append((title, buffer))
    return sections


def split_bullets(lines: List[str]) -> Tuple[List[str], List[str]]:
    bullets = []
    text_lines = []
    for line in lines:
        if re.match(r"^[-•*]\s+", line) or re.match(r"^\d+\.\s+", line):
            bullets.append(re.sub(r"^[-•*]\s+|^\d+\.\s+", "", line).strip())
        else:
            text_lines.append(line)
    return bullets, text_lines


def normalize_text(lines: List[str]) -> str:
    joined = " ".join(lines)
    joined = re.sub(r"\s+", " ", joined).strip()
    return joined


def first_sentences(text: str, count: int = 2) -> str:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return " ".join(parts[:count]).strip()


def format_response(title: str, text: str, bullets: List[str]) -> str:
    if bullets:
        bullet_lines = "\n".join(f"- {b}" for b in bullets)
        return f"Here’s the guidance for {title}:\n{bullet_lines}".strip()
    if text:
        return f"Here’s the guidance for {title}: {text}".strip()
    return f"Here’s the guidance for {title}: (no additional details provided)."


def build_examples(sections: List[Tuple[str, List[str]]], max_chars: int) -> List[dict]:
    examples = []
    for title, lines in sections:
        bullets, text_lines = split_bullets(lines)
        text = normalize_text(text_lines)
        if len(text) < 30 and len(bullets) < 2:
            continue

        response = format_response(title, text[:max_chars], bullets)
        examples.append(
            {
                "instruction": DEFAULT_INSTRUCTION,
                "input": f"Give guidance on {title}.",
                "output": response,
            }
        )

        if bullets:
            examples.append(
                {
                    "instruction": DEFAULT_INSTRUCTION,
                    "input": f"List the key steps or rules for {title}.",
                    "output": "\n".join(f"- {b}" for b in bullets[:40]),
                }
            )

        if text:
            summary = first_sentences(text, 2)
            if summary:
                examples.append(
                    {
                        "instruction": DEFAULT_INSTRUCTION,
                        "input": f"Summarize {title} for a new hire.",
                        "output": summary,
                    }
                )

        do_lines = [b for b in bullets if b.lower().startswith(("do ", "always", "ensure"))]
        dont_lines = [b for b in bullets if b.lower().startswith(("don't", "do not", "never"))]
        if do_lines or dont_lines:
            output_parts = []
            if do_lines:
                output_parts.append("Do:\n" + "\n".join(f"- {b}" for b in do_lines))
            if dont_lines:
                output_parts.append("Don’t:\n" + "\n".join(f"- {b}" for b in dont_lines))
            examples.append(
                {
                    "instruction": DEFAULT_INSTRUCTION,
                    "input": f"What should I do or avoid for {title}?",
                    "output": "\n".join(output_parts),
                }
            )

    return examples


def write_jsonl(path: Path, rows: List[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract PDF into training JSONL")
    parser.add_argument("--pdf", default="Account Management New Hire Training Manual 1.pdf")
    parser.add_argument("--output", default="neural/data/account_management_train.jsonl")
    parser.add_argument("--max-chars", type=int, default=1200)
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        raise SystemExit(f"PDF not found: {pdf_path}")

    pages = extract_pages(pdf_path)
    cleaned_pages = remove_repeated_lines(pages)
    merged_lines: List[str] = []
    for page_lines in cleaned_pages:
        merged_lines.extend(merge_hyphenated(page_lines))

    sections = extract_sections(merged_lines)
    examples = build_examples(sections, args.max_chars)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_path, examples)

    print(f"Wrote {len(examples)} examples to {output_path}")


if __name__ == "__main__":
    main()
