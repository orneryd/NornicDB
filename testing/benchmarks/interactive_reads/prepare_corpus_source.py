#!/usr/bin/env python3
# Author: Codex NOR-285 implementation agent, 2026-10-26.
"""Append maintenance/export and maintained-read tests to a pinned lab source copy.

No production reader, writer, maintenance logic or original full-comparison test
is changed. The appended test calls the canonical U16 fixture and maintenance,
then exports its database only after graceful server termination.
The separately named read test consumes that export without maintenance or
catch-up mutations. The original full test still requires unmaintained input.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

AUTHOR = "Codex NOR-285 implementation agent, 2026-10-26"
REVISION = "67e180c85f726db12c0e51ee4ec76da990a386ed"

PREPARATION = r'''

// Author: Codex NOR-285 implementation agent, 2026-10-26.
#[test]
#[ignore = "NOR-285 lab-only canonical maintenance and stopped-store export"]
fn test_nor285_prepare_maintained_corpus() {
    let destination = PathBuf::from(std::env::var_os("NOR285_MAINTAINED_STORE_OUT")
        .expect("explicit fresh maintained-store destination"));
    assert!(destination.is_absolute() && !destination.exists(), "fresh absolute output required");
    let mut f = fixture();
    let _resources = resources::Sampler::start();
    let started = Instant::now();
    let result = m::Maintenance::default().maintain(
        f.graph.as_ref(), &m::Changes { full: true, ..m::Changes::default() }
    ).expect("canonical full holdings maintenance");
    let seconds = started.elapsed().as_secs_f64();
    assert_eq!(result.documents, 5280, "complete observed document population");
    let writes: i64 = result.batches.iter().map(|b| b.writes).sum();
    let counted: i64 = result.batches.iter().map(|b| b.counted).sum();
    assert_eq!(writes, counted, "canonical counted maintenance writes");
    let marker = m::built(f.graph.as_ref()).expect("holdings marker");
    assert_eq!(marker.built, Some(true));
    assert_eq!(marker.updating_since, None);
    emit("holdings_build", seconds, json!({"author":"Codex NOR-285 implementation agent",
        "documents":result.documents,"components":result.components,
        "batches":result.batches.len(),"writes":writes,"counted":counted,
        "outcome":"completed","read_workloads_executed":false}));
    save("holdings_elements", &holdings_counts(&f));
    save("holdings_state", &holdings_state(&f));
    f.nor285_export_stopped_store(&destination);
    emit("nor285_stopped_store_export", 0.0, json!({"author":"Codex NOR-285 implementation agent",
        "destination":destination,"gracefully_stopped":true,
        "read_workloads_executed":false,"outcome":"completed"}));
}

// Author: Codex NOR-285 implementation agent, 2026-10-26.
#[test]
#[ignore = "NOR-285 lab-only reads on the one admitted maintained store"]
fn test_nor285_maintained_read_comparison() {
    assert_eq!(std::env::var("PD_PERF_DOCUMENTS").expect("population"), "5280");
    let store = std::env::var_os("PD_NATIVE_STORE_IN").expect("maintained database TAR");
    let expected_path = std::env::var_os("NOR285_EXPECTED_HOLDINGS_COUNTS")
        .expect("preparation results/holdings_elements.json");
    let expected: Value = serde_json::from_slice(
        &std::fs::read(expected_path).expect("preparation holdings count bytes")
    ).expect("preparation holdings count JSON");
    // Do not call fixture(): its unmaintained guard remains the original
    // end-to-end test's contract. Native still copies the TAR into a private dir.
    let f = common::Fixture::new().expect("job-selected guarded native fixture");
    let _resources = resources::Sampler::start();
    let documents = f.run("MATCH (d:PDDocument) RETURN count(d) AS n", json!({}));
    assert_eq!(documents[0]["n"], json!(5280));
    let marker = m::built(f.graph.as_ref()).expect("maintained holdings marker");
    assert_eq!(marker.built, Some(true), "maintained export required");
    assert_eq!(marker.updating_since, None, "maintenance must have finished");
    let counts = holdings_counts(&f);
    assert_eq!(counts, expected, "same preparation holdings population");
    assert!(counts["held"].as_u64().expect("held count") > 0, "populated holdings required");
    emit("nor285_maintained_input", 0.0, json!({"author":"Codex NOR-285 implementation agent",
        "documents":5280,"store_sha256":bytes_hash(&std::fs::read(store).expect("maintained TAR bytes")),
        "holdings":counts,"built":marker.built,"updating_since":marker.updating_since,
        "maintenance_executed":false}));
    save("holdings_elements", &counts);
    let before = holdings_state(&f);
    save("holdings_state_before", &before);
    let graph_before = nor285_graph_counts(&f);
    assert_eq!(graph_before["topic_nodes"], json!(0));
    assert_eq!(graph_before["topic_links"], json!(0));
    // Reuse the current application's canonical constants/parameters through
    // the existing plan, retaining only the five allocated complete families.
    let mut complete = true;
    let mut queries = 0;
    for (step, query, parameters) in plan(&f) {
        match step {
            "same_party" | "related_doc" | "related_case" | "suggest_doc" |
            "suggest_identity" | "topic_groups" => {
                complete &= timed(&f, step, &query, parameters);
                queries += 1;
            }
            "walk" => {
                for depth in [4, 3, 2, 1] {
                    let mut args = parameters.clone();
                    args["depth"] = json!(depth);
                    complete &= timed(&f, &format!("walk_doc_d{depth}"), &query, args);
                    queries += 1;
                }
            }
            _ => {}
        }
    }
    assert_eq!(queries, 10, "complete five-family variant population");
    let graph_after = nor285_graph_counts(&f);
    save("graph_counts_before", &graph_before);
    save("graph_counts_after", &graph_after);
    assert_eq!(graph_after, graph_before, "topic projection cleanup and net graph counts");
    let after = holdings_state(&f);
    save("holdings_state_after", &after);
    assert_eq!(after, before, "maintained graph state preserved");
    assert!(complete, "every allocated complete read must succeed");
    emit("nor285_maintained_comparison_complete", 0.0,
        json!({"author":"Codex NOR-285 implementation agent","documents":5280,
            "queries":queries,"maintenance_executed":false,"outcome":"completed"}));
}

// Author: Codex NOR-285 implementation agent, 2026-10-26.
fn nor285_graph_counts(f: &common::Fixture) -> Value {
    let count = |query: &str| f.run(query, json!({}))[0]["n"].clone();
    json!({
        "nodes":count("MATCH (n) RETURN count(n) AS n"),
        "links":count("MATCH ()-[r]->() RETURN count(r) AS n"),
        "topic_nodes":count("MATCH (n:PDTopicLink) RETURN count(n) AS n"),
        "topic_links":count("MATCH ()-[r:PD_TOPIC_LINK]->() RETURN count(r) AS n"),
    })
}
'''

FIXTURE_EXPORT = r'''

// Author: Codex NOR-285 implementation agent, 2026-10-26.
impl Fixture {
    pub fn nor285_export_stopped_store(&mut self, destination: &std::path::Path) {
        self._native.nor285_export_stopped_store(destination);
    }
}
'''

NATIVE_EXPORT = r'''

// Author: Codex NOR-285 implementation agent, 2026-10-26.
impl Native {
    pub fn nor285_export_stopped_store(&mut self, destination: &Path) {
        assert!(destination.is_absolute() && !destination.exists(), "fresh absolute output required");
        self.agent = ureq::Agent::new_with_defaults();
        let pid = i32::try_from(self.process.id()).ok().and_then(Pid::from_raw)
            .expect("native engine pid");
        kill_process(pid, Signal::TERM).expect("graceful native engine termination");
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(status) = self.process.try_wait().expect("native exit state") {
                assert!(status.success(), "native engine failed during graceful shutdown: {status}");
                break;
            }
            assert!(Instant::now() < deadline, "no stopped store exported: graceful shutdown incomplete");
            std::thread::sleep(Duration::from_millis(50));
        }
        std::fs::create_dir_all(destination.parent().expect("destination parent"))
            .expect("create private export parent");
        let status = Command::new("cp").arg("-a").arg(self.dir.path().join("database"))
            .arg(destination).status().expect("copy stopped fixture store");
        assert!(status.success(), "stopped fixture store copy failed: {status}");
    }
}
'''

FILES = [
    ("rust/crates/pd-graph/tests/u16_perf.rs", "e62b434926c6b2158d513ff83d7eca7e9285d169", PREPARATION),
    ("rust/crates/pd-graph/tests/knowledge/common.rs", "f744e80247b9a49dc93e7608e77bc83e37f1bd84", FIXTURE_EXPORT),
    ("rust/crates/pd-test-support/src/native.rs", "cef8d3f4d8228a595eeda3b4d439a88247e32e73", NATIVE_EXPORT),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("private_source_copy", type=Path)
    args = parser.parse_args()
    if sys.platform != "linux":
        raise SystemExit("Source preparation runs on the Linux lab only")
    root = args.private_source_copy.resolve()
    if not str(root).startswith("/work/tnrun/"):
        raise SystemExit("Use a fresh private tnrun source extraction, never the canonical checkout/cache")
    receipt_path = root / "nor285-preparation-source.json"
    if receipt_path.exists():
        raise SystemExit("Source was already prepared; inspect its receipt")
    changes = []
    # Check every preimage before writing any file.
    for relative, expected_blob, addition in FILES:
        path = root / relative
        before = path.read_bytes()
        blob = hashlib.sha1(b"blob " + str(len(before)).encode() + b"\0" + before).hexdigest()
        if blob != expected_blob:
            raise SystemExit(f"Source revision/preimage differs: {relative}: {blob}")
        changes.append((path, before, before + addition.encode(), expected_blob))
    receipt = {"author": AUTHOR, "source_revision": REVISION, "changed_files": [],
               "scope": "Appended preparation/export and separately named maintained-read test only; canonical product sources and original full-comparison test unchanged."}
    for path, before, after, blob in changes:
        path.write_bytes(after)
        receipt["changed_files"].append({"path": str(path.relative_to(root)),
            "before_git_blob": blob, "before_sha256": hashlib.sha256(before).hexdigest(),
            "after_sha256": hashlib.sha256(after).hexdigest(),
            "original_bytes_preserved_as_prefix": after.startswith(before)})
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
