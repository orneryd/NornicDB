// Author: Codex NOR-285 implementation agent, 2026-10-26.
package interactive_reads_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// Checks the supplied report's immutable query identity and required workload
// population independently of the availability of its saved physical store.
func TestNOR285FrozenWorkload(t *testing.T) {
	for name, data := range map[string][]byte{"historical": workloadJSON, "current": currentWorkloadJSON} {
		t.Run(name, func(t *testing.T) { checkFrozenWorkload(t, name, data) })
	}
	t.Run("corpus_identity", checkCorpusIdentity)
}

func checkCorpusIdentity(t *testing.T) {
	root := t.TempDir()
	must(t, os.Mkdir(filepath.Join(root, "wal"), 0700))
	path := filepath.Join(root, "wal", "wal.log")
	must(t, os.WriteFile(path, []byte("original"), 0600))
	// A fixed vector checks the digest protocol independently of its traversal.
	want := digest([]byte(`{"wal/wal.log":"0682c5f2076f099c34cfdd15a9e063849ed437a49677e6fcc5b4198c76575be5"}`))
	actual, err := savedCorpusDigest(root)
	must(t, err)
	must(t, requireCorpusIdentity(actual, want))
	if requireCorpusIdentity(actual, "") == nil {
		t.Fatal("missing frozen identity admitted")
	}
	for _, change := range []struct {
		name  string
		apply func(string) error
	}{
		{"changed_bytes", func(root string) error {
			return os.WriteFile(filepath.Join(root, "wal", "wal.log"), []byte("modified"), 0600)
		}},
		{"added_file", func(root string) error { return os.WriteFile(filepath.Join(root, "extra"), []byte("extra"), 0600) }},
		{"removed_file", func(root string) error { return os.Remove(filepath.Join(root, "wal", "wal.log")) }},
		{"renamed_file", func(root string) error {
			return os.Rename(filepath.Join(root, "wal", "wal.log"), filepath.Join(root, "wal", "renamed.log"))
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			copyRoot := t.TempDir()
			must(t, os.CopyFS(copyRoot, os.DirFS(root)))
			must(t, change.apply(copyRoot))
			changed, err := savedCorpusDigest(copyRoot)
			must(t, err)
			if requireCorpusIdentity(changed, want) == nil {
				t.Fatal("different export bytes admitted")
			}
		})
	}
}

func checkFrozenWorkload(t *testing.T, name string, data []byte) {
	t.Helper()
	var packet workload
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	must(t, decoder.Decode(&packet))
	if packet.Author == "" || packet.Documents != 5280 || len(packet.Cases) != 10 {
		t.Fatal("incomplete attributed full-scale input packet")
	}
	corpusHash, err := hex.DecodeString(packet.CorpusSHA256)
	must(t, err)
	if len(corpusHash) != 32 || packet.CorpusDigestAlgorithm != corpusDigestAlgorithm {
		t.Fatal("missing frozen maintained-export content identity")
	}
	families := map[string]int{}
	seen := map[string]bool{}
	for _, item := range packet.Cases {
		if seen[item.Name] || item.Name == "" {
			t.Fatalf("duplicate/unnamed case: %q", item.Name)
		}
		seen[item.Name] = true
		families[item.Family]++
		if digest([]byte(item.Statement)) != item.StatementSHA256 {
			t.Fatalf("changed statement: %s", item.Name)
		}
		if name == "historical" && item.Name == "same_party" && (len(item.Statement) != 63177 || item.StatementSHA256 != "0caa2b3923cdb2f064197ddcfc7763143303ec67170cb7e68bc609eaa32ae21f") {
			t.Fatal("SAME_PARTY no longer matches the supplied complete bound-holder statement")
		}
		if name == "current" && item.Name == "same_party" && (len(item.Statement) != 70555 || item.StatementSHA256 != "9c0318d7a01a3e3f393b1d11f6da62f23934d7e14b6d5ff280a3cfe8b20d146c") {
			t.Fatal("SAME_PARTY no longer matches the current 0.4.3 frozen contract")
		}
	}
	want := map[string]int{"SAME_PARTY": 1, "WALK": 4, "TOPIC_GROUPS": 1, "RELATED": 2, "SUGGEST": 2}
	if !reflect.DeepEqual(families, want) {
		t.Fatalf("missing read family or variants: %v", families)
	}
}

// A labelled plumbing check of the same frozen maintained-export admission used
// by profiles, followed by small public reads without a measured full workload.
func TestNOR285CorpusPlumbing(t *testing.T) {
	if os.Getenv("NOR285_PLUMBING") != "1" {
		t.Skip("explicit lab-only plumbing check")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("plumbing must run on the Linux lab")
	}
	var packet workload
	must(t, json.Unmarshal(currentWorkloadJSON, &packet))
	if packet.CorpusDigestAlgorithm != corpusDigestAlgorithm || packet.CorpusSHA256 == "" {
		t.Fatal("frozen packet must identify the maintained export bytes")
	}
	executor, corpusSHA256 := openSavedCorpus(t, packet.CorpusSHA256)
	ctx := context.Background()
	counts := map[string]interface{}{}
	for _, entry := range []struct{ name, statement string }{
		{"documents", "MATCH (d:PDDocument) RETURN count(d) AS n"},
		{"nodes", "MATCH (n) RETURN count(n) AS n"},
		{"relationships", "MATCH ()-[r]->() RETURN count(r) AS n"},
		{"holdings", "MATCH (h:PDHeld) RETURN count(h) AS n"},
	} {
		result, err := executor.Execute(ctx, entry.statement, nil)
		must(t, err)
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			t.Fatalf("invalid count: %+v", result)
		}
		counts[entry.name] = result.Rows[0][0]
	}
	if fmt.Sprint(counts["documents"]) != "5280" {
		t.Fatalf("wrong plumbing corpus: %v", counts)
	}
	control, err := executor.Execute(ctx, "RETURN $items AS items, $count AS count", map[string]interface{}{"items": []interface{}{"évidence", "party"}, "count": int64(5280)})
	must(t, err)
	if !reflect.DeepEqual(control.Columns, []string{"items", "count"}) || len(control.Rows) != 1 || fmt.Sprint(control.Rows[0][1]) != "5280" {
		t.Fatalf("public result plumbing differs: %+v", control)
	}
	emit(t, "plumbing-only", map[string]interface{}{"corpus": os.Getenv("NOR285_CORPUS"), "corpus_sha256": corpusSHA256, "corpus_digest_algorithm": corpusDigestAlgorithm, "packet_sha256": digest(currentWorkloadJSON), "counts": counts, "parser": os.Getenv("NORNICDB_PARSER"), "control": control.Rows, "workload_measured": false, "resources": resourceState()})
}
