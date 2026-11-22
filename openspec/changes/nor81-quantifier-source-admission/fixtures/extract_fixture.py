import hashlib
import html
import json
import pathlib
import re

TICKET = "/tmp/n152-148-ticket.json"
HERE = pathlib.Path(__file__).resolve().parent

with open(TICKET, "r", encoding="utf-8") as handle:
    ticket = json.load(handle)

blocks = [html.unescape(block) for block in re.findall(r"<pre><code>(.*?)</code></pre>", ticket["description_html"], re.S)]
assert len(blocks) == 7, len(blocks)
harness, script7982, script8081, results7982, results8081 = blocks[0], blocks[1], blocks[2], blocks[3], blocks[4]

(HERE / "nor79_to_82_static_claims_test.go.txt").write_text(harness, encoding="utf-8")


def between(text, start, end):
    first = text.index(start)
    last = len(text) if end is None else text.index(end, first)
    return text[first:last]


accept_and_comprehension_script = between(script7982, "printf \"%s\\n\" '81-accept RUN'", "printf \"%s\\n\" '82-date() RUN'")
reduce_script = between(script8081, "printf \"%s\\n\" '3 RUN'", None)
accept_and_comprehension_results = between(results7982, "81-accept RUN", "82-date() RUN")
reduce_results = between(results8081, "3 RUN", None)

assert "81-list-reject PROFILE" in accept_and_comprehension_script
assert "81-list-reject PROFILE" in accept_and_comprehension_results
assert "exit=1" in reduce_results

parts = [
    "# NOR-81 reference evidence (verbatim excerpts)\n",
    "\n",
    "Provenance: extracted programmatically from the inline code blocks of the\n",
    "Plane N152-148 card (`/tmp/n152-148-ticket.json`). The source script blocks are\n",
    "`/tmp/nor79-82-reference.sh` (ticket block 2 of 7) and `/tmp/nor80-81-reference.sh`\n",
    "(block 3 of 7); the result blocks are `/tmp/nor79-82-reference-results.log`\n",
    "(block 4 of 7) and `/tmp/nor80-81-reference-results.log` (block 5 of 7). Added text is\n",
    "limited to these headers. Result logs begin with an SSH post-quantum warning at\n",
    "the top of the connection; it is omitted here as transport noise, and each\n",
    "excerpt begins at the first statement label it quotes. Headers are not part\n",
    "of the original evidence.\n",
    "\n",
    "## Exact NOR-81 script sections (Neo4j 5.26.30 reference run)\n",
    "\n",
    "```bash\n",
    accept_and_comprehension_script,
    "```\n",
    "\n",
    "```bash\n",
    reduce_script,
    "```\n",
    "\n",
    "## Exact result sections\n",
    "\n",
    "```text\n",
    accept_and_comprehension_results,
    "```\n",
    "\n",
    "```text\n",
    reduce_results,
    "```\n",
]
out = "".join(parts)
(HERE / "nor81-reference-evidence.txt").write_text(out, encoding="utf-8")

for name in ("nor79_to_82_static_claims_test.go.txt", "nor81-reference-evidence.txt"):
    data = (HERE / name).read_bytes()
    print(name, len(data), hashlib.sha256(data).hexdigest())
