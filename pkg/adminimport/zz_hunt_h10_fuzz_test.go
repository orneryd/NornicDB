package adminimport

// Hunt ground H10 (decoders and parsers), target 7 of 8:
//
//	FuzzHuntH10CSVImport  a node CSV file (Neo4j admin-import header) imported
//	                      with ImportFull into an empty in-memory database
//
// Oracle: no panic; no hang (30 s); round-trip equality: when the import
// succeeds, ExportNeo4jCSV of the imported database imported again into a
// second empty database gives the same nodes (labels and properties).
//
// Run: go test ./pkg/adminimport -run '^$' -fuzz '^FuzzHuntH10CSVImport$' -fuzztime 30m -parallel 1

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func huntH10NodeSet(t *testing.T, engine storage.Engine) []string {
	nodes, err := storage.CollectStoredNodes(context.Background(), engine)
	if err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		labels := append([]string(nil), node.Labels...)
		sort.Strings(labels)
		data, _ := json.Marshal(map[string]any{"labels": labels, "properties": node.Properties})
		out = append(out, string(data))
	}
	sort.Strings(out)
	return out
}

func FuzzHuntH10CSVImport(f *testing.F) {
	// Left out (DECISIONS 15, already lead L-H10-001): the former seed#0
	// "id:ID,name,age:int,:LABEL\n1,Ann,30,Person\n2,Bob,,Person;Admin\n".
	f.Add("id:ID,score:float,tags:string[],:LABEL\n1,1.5,a;b;c,T\n2,-0.0,,T\n")
	f.Add("id:ID,flag:boolean,when:datetime,:LABEL\n1,true,2026-01-01T00:00:00Z,E\n")
	f.Add("id:ID,\"quoted, name\",:LABEL\n1,\"a \"\"b\"\" c\",Q\n")
	f.Add("id:ID,v:int[],:LABEL\n1,1;2;3,L\n2,9223372036854775807,L\n")
	f.Fuzz(func(t *testing.T, csv string) {
		huntH10Remember("FuzzHuntH10CSVImport", []byte(csv))
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v (input %q)", r, csv)
			}
		}()
		dir := t.TempDir()
		path := filepath.Join(dir, "nodes.csv")
		if err := os.WriteFile(path, []byte(csv), 0o600); err != nil {
			t.Fatal(err)
		}
		base := storage.NewMemoryEngine()
		defer base.Close()
		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err = ImportFull(context.Background(), base, Options{DatabaseName: "fz", NodeSources: []string{path}, Now: time.Unix(1700000000, 0).UTC()})
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("hang importing %q", csv)
		}
		if err != nil {
			return
		}
		first := huntH10NodeSet(t, storage.NewNamespacedEngine(base, "fz"))
		out := filepath.Join(dir, "export")
		if err := ExportNeo4jCSV(storage.NewNamespacedEngine(base, "fz"), Neo4jCSVExportOptions{OutputDir: out}); err != nil {
			t.Fatalf("export of an imported database failed: %v (input %q)", err, csv)
		}
		nodeSources, relSources, err := DiscoverNeo4jCSVSources(out, Options{})
		if err != nil {
			t.Fatalf("discover exported files: %v (input %q)", err, csv)
		}
		second := storage.NewMemoryEngine()
		defer second.Close()
		if _, err := ImportFull(context.Background(), second, Options{DatabaseName: "fz2", NodeSources: nodeSources, RelSources: relSources, Now: time.Unix(1700000000, 0).UTC()}); err != nil {
			t.Fatalf("re-import of the export failed: %v (input %q)", err, csv)
		}
		again := huntH10NodeSet(t, storage.NewNamespacedEngine(second, "fz2"))
		if len(first) != len(again) {
			t.Fatalf("round trip changed the node count %d -> %d (input %q)", len(first), len(again), csv)
		}
		for i := range first {
			if first[i] != again[i] {
				t.Fatalf("round trip changed a node:\n%s\n%s\n(input %q)", first[i], again[i], csv)
			}
		}
	})
}

// huntH10Remember keeps the input being run in $HUNT_H10_LAST_DIR/<target>.last
// (the lab's shared /work/tnrun dataset), so that an input that crashes the
// process outside the fuzz goroutine survives the job.
func huntH10Remember(target string, data []byte) {
	dir := os.Getenv("HUNT_H10_LAST_DIR")
	if dir == "" {
		return
	}
	_ = os.MkdirAll(dir, 0o755)
	_ = os.WriteFile(filepath.Join(dir, target+".last"), data, 0o644)
}
