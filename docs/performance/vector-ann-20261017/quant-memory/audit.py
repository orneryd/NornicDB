"""Validate and summarize the five completed memory diagnostic cases."""
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
SOURCE = "92e1c44138aecac592bd421e94de67fcd5a90b9f"
MODES = ["sq-int8", "bq-asym4", "bq-asym8", "pq-x4", "pq-x64"]
rows = []
corpus = None
for case, mode in enumerate(MODES, 1):
    folder = "results-pq64" if mode == "pq-x64" else "results"
    path = ROOT / folder / f"nornicdb-{mode}.json"
    data = json.loads(path.read_text())
    assert data["vectors"] == data["loaded_points"] == 88155
    assert data["dimensions"] == 1024 and data["k"] == 10
    assert len(data["runs"]) == 1
    run = data["runs"][0]
    assert run["queries"] == 1000 and run["concurrency"] == 8
    settings = data["provenance"]["nornic_build_settings"]
    assert settings["vcs.revision"] == SOURCE and settings["vcs.modified"] == "false"
    hashes = data["provenance"]["corpus_sha256"]
    if corpus is None:
        corpus = hashes
    assert hashes == corpus
    assert run["search_params"]["hnsw_ef"] == "50"
    quant = run["search_params"]["quantization"]
    assert quant["rescore"] is True
    assert quant["oversampling"] == (2.0 if mode.startswith("bq") else 1.0)
    old_folder = "mapped-scalar" if mode == "sq-int8" else "mapped-compressed"
    old = json.loads((ROOT.parent / "hnsw-query-cost" / old_folder / path.name).read_text())
    assert old["provenance"]["corpus_sha256"] == hashes
    assert old["runs"][0]["recall@10"] == run["recall@10"]
    snapshot_dir = ROOT / f"case-{case}"
    stats = {key: int(value) for key, value in re.findall(
        r"^# (HeapAlloc|HeapIdle|HeapReleased|HeapInuse|HeapSys) = (\d+)",
        (snapshot_dir / "heap-after-stats.txt").read_text(), re.M)}
    assert len(stats) == 5
    snapshot = json.loads((snapshot_dir / "snapshot.json").read_text())
    assert snapshot["instrumented"] is True
    assert snapshot["vmmap-summary.txt_exit"] == snapshot["vmmap-resident.txt_exit"] == 0
    footprint = re.search(r"^Physical footprint:\s+([^\n]+)",
                          (snapshot_dir / "vmmap-summary.txt").read_text(), re.M).group(1)
    encoded = int(re.search(r"\((\d+) encoded bytes\)", data["strategy"]).group(1))
    rows.append(dict(mode=mode, case=case, encoded_bytes=encoded, heap=stats,
                     rss_after_gc=snapshot["rss_after_gc"], physical_footprint_vmmap=footprint,
                     recall_at_10=run["recall@10"], source=SOURCE,
                     json_sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
assert rows[1]["encoded_bytes"] == rows[2]["encoded_bytes"] == 11283840
print(json.dumps(dict(result="pass", scope="five completed selected full-corpus diagnostic cases; no runtime edits",
                      cases=rows), indent=2))
