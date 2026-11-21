package adminimport

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-admin-csv-roundtrip).
// Test-side only. Shows the decisive step: the export writes the label list
// joined by the array delimiter with no escape (neo4j_csv.go:180), so the
// label set ["A;B"] and the label set ["A","B"] produce byte-identical label
// cells; the importer's splitList (importer.go:1230) cannot tell them apart.
// Also logs the same step for a string-list property (neo4j_csv.go:395-429).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRCAAdminCSVLabelCellIsAmbiguous(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	source := storage.NewNamespacedEngine(base, "source")
	for _, n := range []*storage.Node{
		{ID: "n1-one-label", Labels: []string{"A;B"}},
		{ID: "n2-two-labels", Labels: []string{"A", "B"}},
		{ID: "n3-list-prop", Labels: []string{"P"}, Properties: map[string]any{"tags": []string{"x;y"}}},
	} {
		_, err := source.CreateNode(t.Context(), n)
		require.NoError(t, err)
	}
	out := filepath.Join(t.TempDir(), "csv")
	require.NoError(t, ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: out}))
	raw, err := os.ReadFile(filepath.Join(out, "nodes.csv"))
	require.NoError(t, err)
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		t.Logf("RCA_CSV_LINE %d %s", i, line)
	}

	nodes, edges, err := DiscoverNeo4jCSVSources(out, Options{})
	require.NoError(t, err)
	dest := importNeo4jCSVPackage(t, Options{}, nodes, edges)
	for _, id := range []storage.NodeID{"n1-one-label", "n2-two-labels", "n3-list-prop"} {
		got, err := dest.GetNode(t.Context(), id)
		require.NoError(t, err)
		t.Logf("RCA_CSV_REIMPORT id=%s labels=%#v tags=%#v", id, got.Labels, got.Properties["tags"])
	}
	// Decisive value: the label cell is the same text for both label sets.
	t.Logf("RCA_CSV_SPLIT splitList(%q)=%#v", "A;B", splitList("A;B", ';'))
}
