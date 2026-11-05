// Author: Codex NOR-285 implementation agent, 2026-10-26.
package interactive_reads_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

// This is a census of observed input, not a generator or repair. In particular,
// no assumed seven-entity distribution, case or correction is filled in.
func TestNOR285CorpusCensus(t *testing.T) {
	if os.Getenv("NOR285_CENSUS") != "1" {
		t.Skip("explicit lab-only input census")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("census must run on Linux lab")
	}
	corpus := os.Getenv("NOR285_CORPUS")
	archive := filepath.Join(t.TempDir(), "unmaintained.tar")
	name := filepath.Base(corpus)
	if name != "5280-database" {
		t.Fatal("census tar scope must be the explicitly allocated historical snapshot")
	}
	args := []string{"--format=gnu", "-cf", archive, "-C", filepath.Dir(corpus), "--transform=s,^5280-database,database,", name}
	output, err := exec.Command("tar", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("archive input: %v: %s", err, output)
	}
	file, err := os.Open(archive)
	must(t, err)
	hash := sha256.New()
	archiveBytes, err := io.Copy(hash, file)
	must(t, err)
	must(t, file.Close())
	actual := fmt.Sprintf("%x", hash.Sum(nil))
	expected := "1c74fa9c95b5ad3b0e6c85316d230c4cdfeae284bd77be2d67611694a2ad49be"
	emit(t, "input-archive", map[string]interface{}{"corpus": corpus, "tar_args": args, "bytes": archiveBytes, "sha256": actual, "reported_sha256": expected, "matches_reported_tar": actual == expected, "workload_measured": false})
	executor, _ := openSavedCorpus(t, "")
	ctx := context.Background()
	results := map[string]*cypher.ExecuteResult{}
	for _, entry := range []struct{ name, statement string }{
		{"total_nodes", "MATCH (n) RETURN count(n) AS nodes"},
		{"total_relationships", "MATCH ()-[r]->() RETURN count(r) AS relationships"},
		{"labels", "MATCH (n) UNWIND labels(n) AS label RETURN label, count(n) AS nodes ORDER BY label"},
		{"relationship_types", "MATCH ()-[r]->() RETURN type(r) AS relationship_type, count(r) AS relationships ORDER BY relationship_type"},
		{"relationship_kinds", "MATCH ()-[r:PD_RELATION]->() RETURN r.kind AS kind, count(r) AS relationships ORDER BY kind"},
		{"documents", "MATCH (d:PDDocument) RETURN d.id AS id ORDER BY id"},
		{"document_owned_nodes", "MATCH (n) WHERE n.document_id IS NOT NULL RETURN n.document_id AS document_id, labels(n) AS labels, count(n) AS nodes"},
		{"corrections", "MATCH (c:PDCorrection) RETURN c.operation AS operation, c.status AS status, count(c) AS corrections"},
		{"required_starts", "MATCH (n:PDGraphNode) WHERE n.id IN ['doc10', 'case3'] RETURN n.id AS id, labels(n) AS labels, keys(n) AS property_keys ORDER BY id"},
		{"named_identities", "MATCH (i:PDIdentity) RETURN count(i) AS identities, count(i.name) AS named_identities"},
	} {
		result, err := executor.Execute(ctx, entry.statement, nil)
		must(t, err)
		results[entry.name] = result
		emit(t, "census-query", map[string]interface{}{"name": entry.name, "statement": entry.statement, "result": result, "workload_measured": false})
	}
	byDocument := map[string]map[string]int64{}
	for _, row := range results["documents"].Rows {
		byDocument[fmt.Sprint(row[0])] = map[string]int64{}
	}
	orphans := []interface{}{}
	for _, row := range results["document_owned_nodes"].Rows {
		id := fmt.Sprint(row[0])
		labels := []string{}
		switch items := row[1].(type) {
		case []interface{}:
			for _, item := range items {
				labels = append(labels, fmt.Sprint(item))
			}
		case []string:
			labels = append(labels, items...)
		default:
			t.Fatalf("unexpected label value: %T", row[1])
		}
		sort.Strings(labels)
		key := strings.Join(labels, ":")
		count, ok := row[2].(int64)
		if !ok {
			t.Fatalf("unexpected count: %T", row[2])
		}
		if byDocument[id] == nil {
			orphans = append(orphans, row)
			continue
		}
		byDocument[id][key] += count
	}
	// Explicit zeroes here describe absent observations; no graph data is added.
	histograms := map[string]map[int64]int{}
	labelSets := map[string]bool{}
	for _, counts := range byDocument {
		for label := range counts {
			labelSets[label] = true
		}
	}
	for label := range labelSets {
		histograms[label] = map[int64]int{}
		for _, counts := range byDocument {
			histograms[label][counts[label]]++
		}
	}
	emit(t, "corpus-census", map[string]interface{}{"corpus": corpus, "parser": os.Getenv("NORNICDB_PARSER"), "raw_public_results": results, "per_document_owned_label_counts": byDocument, "per_label_count_histograms": histograms, "unknown_document_owners": orphans, "workload_measured": false, "resources": resourceState()})
	if len(byDocument) != 5280 {
		t.Fatalf("observed %d distinct documents", len(byDocument))
	}
}
