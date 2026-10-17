"""Audit the archived full-corpus IVF-HNSW before/after artifacts."""
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parent
expected = {
    "serial": ("ivf-hnsw+serial-control", "7c201376d1203a651fc625869fb98df7be746ac4", 193),
    "current": ("ivf-hnsw+no-fallback", "c5ef90e3825d2871f7df772a293b09237c00c2a8", 209),
}
reference = json.loads((root.parent / "cpu/nornicdb-ivf-hnsw.json").read_text())
cases = []
for phase, (mode, revision, graphs) in expected.items():
    result = json.loads((root / phase / f"nornicdb-{mode}.json").read_text())
    assert result["engine"] == "nornicdb" and result["mode"] == mode
    assert result["vectors"] == result["loaded_points"] == 88155
    assert result["dimensions"] == 1024 and result["k"] == 10
    assert result["gpu_requested"] is False
    assert result["strategy"] == "IVF-HNSW (per-cluster HNSW)"
    assert result["settings"]["NORNICDB_VECTOR_HNSW_QUANTIZATION"] == "none"
    assert result["settings"]["NORNICDB_GPU_BACKEND"] == "cpu"
    provenance = result["provenance"]
    assert provenance["nornic_build_settings"]["vcs.revision"] == revision
    assert provenance["nornic_build_settings"]["vcs.modified"] == "false"
    assert provenance["corpus_sha256"] == reference["provenance"]["corpus_sha256"]
    assert provenance["checkout_dirty"] is False
    assert len(result["runs"]) == 1
    run = result["runs"][0]
    assert run["queries"] == 1000 and run["concurrency"] == 8
    assert run["hnsw_ef"] is None
    assert 0 <= run["recall@10"] <= 1
    assert 0 < run["p50_ms"] <= run["p95_ms"] <= run["p99_ms"]
    assert run["qps"] > 0
    log = (root / phase / f"nornicdb-{mode}.restart.log").read_text()
    assert int(re.findall(r"Built (\d+) cluster HNSW indexes", log)[-1]) == graphs
    cases.append({"phase": phase, "revision": revision, "vectors": 88155,
                  "queries": 1000, "cluster_graphs": graphs,
                  "recall@10": run["recall@10"], "p50_ms": run["p50_ms"],
                  "p95_ms": run["p95_ms"], "qps": run["qps"]})
print(json.dumps({"passed": True, "cases": cases}, indent=2))
