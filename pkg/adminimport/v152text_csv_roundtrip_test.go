package adminimport_test

// v1.5.2 verification, rebuilt from report entry 30: the admin tool's CSV
// export followed by its CSV import must return the exported data unchanged.
// The calls are the ones cmd/nornicdb-admin makes (runExportNeo4jCSV and
// runImportFull with --from-path). Shapes from the report: a node lacking a
// property that its neighbours carry (small-06 without name) and 1,536-value
// float lists from rand.New(rand.NewSource(42)).

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func v152Props(t *testing.T, engine storage.Engine) map[string]map[string]any {
	t.Helper()
	nodes, err := engine.AllNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]any{}
	for _, n := range nodes {
		uid, _ := n.Properties["uid"].(string)
		out[uid] = n.Properties
	}
	return out
}

func TestV152TextCSVExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	srcDir, csvDir, dstDir := t.TempDir(), t.TempDir(), t.TempDir()
	src, err := storage.NewBadgerEngine(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	ns := storage.NewNamespacedEngine(src, "nornic")
	for i := 0; i < 20; i++ {
		props := map[string]any{"uid": fmt.Sprintf("small-%02d", i), "name": fmt.Sprintf("small-%d", i), "n": int64(i)}
		if i == 6 {
			delete(props, "name")
		}
		if _, err := ns.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("s%02d", i)), Labels: []string{"Small"}, Properties: props}); err != nil {
			t.Fatal(err)
		}
	}
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 50; i++ {
		vec := make([]float64, 1536)
		for j := range vec {
			vec[j] = r.Float64()*2 - 1
		}
		if _, err := ns.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("e%04d", i)), Labels: []string{"Emb"}, Properties: map[string]any{"uid": fmt.Sprintf("emb-%04d", i), "title": fmt.Sprintf("document %d", i), "vec": vec}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 19; i++ {
		if err := ns.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("next-%02d", i)), StartNode: storage.NodeID(fmt.Sprintf("s%02d", i)), EndNode: storage.NodeID(fmt.Sprintf("s%02d", i+1)), Type: "NEXT", Properties: map[string]any{"uid": fmt.Sprintf("next-%02d", i), "w": int64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	want := v152Props(t, ns)
	if err := adminimport.ExportNeo4jCSV(ns, adminimport.Neo4jCSVExportOptions{OutputDir: csvDir, Delimiter: ',', ArrayDelimiter: ';', VectorDelimiter: ';', Quote: '"'}); err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}

	discoverOpts := adminimport.Options{Delimiter: ',', ArrayDelimiter: ';', VectorDelimiter: ';', Quote: '"'}
	nodes, rels, err := adminimport.DiscoverNeo4jCSVSources(csvDir, discoverOpts)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dstDir, HighPerformance: true})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if _, err := adminimport.ImportFull(ctx, dst, adminimport.Options{DatabaseName: "nornic", NodeSources: nodes, RelSources: rels, DataDir: dstDir,
		Delimiter: ',', ArrayDelimiter: ';', VectorDelimiter: ';', Quote: '"', SchemaFile: adminimport.DefaultNeo4jCSVNornicSchemaPath(csvDir)}); err != nil {
		t.Fatal(err)
	}
	got := v152Props(t, storage.NewNamespacedEngine(dst, "nornic"))

	differences := 0
	if len(got) != len(want) {
		t.Errorf("node count after round trip = %d, want %d", len(got), len(want))
	}
	for uid, w := range want {
		g := got[uid]
		for key, wv := range w {
			gv, ok := g[key]
			if !ok || !reflect.DeepEqual(fmt.Sprint(gv), fmt.Sprint(wv)) {
				differences++
				if differences <= 8 {
					t.Logf("V152TEXT E30 %s.%s: exported %T, imported %T present=%v", uid, key, wv, gv, ok)
				}
			}
		}
		for key, gv := range g {
			if _, ok := w[key]; !ok {
				differences++
				if differences <= 8 {
					t.Logf("V152TEXT E30 %s.%s: absent before export, imported as %T %q", uid, key, gv, fmt.Sprint(gv))
				}
			}
		}
	}
	t.Logf("V152TEXT E30 nodes=%d differences=%d (node CSV files %v, relationship CSV files %v)", len(want), differences, nodes, rels)
	if differences > 0 {
		t.Errorf("CSV export followed by CSV import changed %d property values", differences)
	}
}
