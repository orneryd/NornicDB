package adminimport_test

// Verifier of hunt lead L-H10-001 (v1.5.2 verification campaign, test-side
// only). A Neo4j admin-import node file is imported with ImportFull, the store
// is exported with ExportNeo4jCSV (default options), the export is found with
// DiscoverNeo4jCSVSources and imported with ImportFull into a second store, as
// `nornicdb-admin database import full` and `database export neo4j-csv` do.
// Both stores are read back through Cypher.
//
// A "fail" case must disagree on a build that has the defect (DISAGREE); a
// "control" case must round-trip on every build.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func verifyH10Base(t *testing.T) storage.Engine {
	t.Helper()
	return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
}

// verifyH10ReadNodes: every node's labels and properties as a user reads them
// through Cypher, plus the Cypher lookup by the id property the lead names.
func verifyH10ReadNodes(t *testing.T, engine storage.Engine, lookup string) (string, int) {
	t.Helper()
	exec := cypher.NewStorageExecutor(engine)
	ctx := context.Background()
	res, err := exec.Execute(ctx, "MATCH (n) RETURN labels(n) AS labels, properties(n) AS props", nil)
	require.NoError(t, err)
	var rows []string
	for _, row := range res.Rows {
		rows = append(rows, fmt.Sprintf("%#v %#v", row[0], row[1]))
	}
	sort.Strings(rows)
	n := -1
	if lookup != "" {
		res, err := exec.Execute(ctx, lookup, nil)
		require.NoError(t, err)
		n = len(res.Rows)
	}
	return fmt.Sprintf("%v", rows), n
}

func verifyH10EmitCSV(t *testing.T, fields map[string]any) {
	t.Helper()
	data, _ := json.Marshal(fields)
	t.Logf("VERIFY_H10 %s", data)
}

func TestVerifyH10CSVRoundTrip(t *testing.T) {
	cases := []struct {
		name, role, nodesCSV, lookup string
	}{
		// The lead's smallest input, unchanged.
		{"id-ID-column", "fail", "id:ID,name,age:int,:LABEL\n1,Ann,30,Person\n2,Bob,,Person;Admin\n", "MATCH (n {id: '2'}) RETURN n"},
		// Same file with the ID column named uid instead of id.
		{"control-uid-ID-column", "control", "uid:ID,name,age:int,:LABEL\n1,Ann,30,Person\n2,Bob,,Person;Admin\n", "MATCH (n {uid: '2'}) RETURN n"},
		// id as an ordinary string property next to an unnamed :ID column.
		{"control-id-plain-property", "control", ":ID,id,name,:LABEL\nk1,1,Ann,Person\nk2,2,Bob,Person;Admin\n", "MATCH (n {id: '2'}) RETURN n"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("L-H10-001/"+tc.name, func(t *testing.T) {
			ctx := context.Background()
			fields := map[string]any{"lead": "L-H10-001", "case": tc.name, "role": tc.role, "input_nodes_csv": tc.nodesCSV}

			inDir := filepath.Join(t.TempDir(), "in")
			require.NoError(t, os.MkdirAll(inDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(inDir, "nodes.csv"), []byte(tc.nodesCSV), 0o644))
			nodeSources, relSources, err := adminimport.DiscoverNeo4jCSVSources(inDir, adminimport.Options{})
			require.NoError(t, err)
			base1 := verifyH10Base(t)
			_, err = adminimport.ImportFull(ctx, base1, adminimport.Options{DatabaseName: "first", NodeSources: nodeSources, RelSources: relSources})
			require.NoError(t, err)
			first := storage.NewNamespacedEngine(base1, "first")
			firstText, firstHits := verifyH10ReadNodes(t, first, tc.lookup)
			fields["first_import"], fields["first_lookup_rows"] = firstText, firstHits

			outDir := filepath.Join(t.TempDir(), "export")
			require.NoError(t, adminimport.ExportNeo4jCSV(first, adminimport.Neo4jCSVExportOptions{OutputDir: outDir}))
			if data, err := os.ReadFile(filepath.Join(outDir, "nodes.csv")); err == nil {
				fields["export_nodes_csv"] = string(data)
			}
			nodeSources, relSources, err = adminimport.DiscoverNeo4jCSVSources(outDir, adminimport.Options{})
			require.NoError(t, err)
			base2 := verifyH10Base(t)
			_, err = adminimport.ImportFull(ctx, base2, adminimport.Options{DatabaseName: "second", NodeSources: nodeSources, RelSources: relSources})
			if err != nil {
				fields["reimport_error"] = err.Error()
				verifyH10EmitCSV(t, fields)
				t.Fatalf("DISAGREE re-import of the export refused: %v", err)
			}
			secondText, secondHits := verifyH10ReadNodes(t, storage.NewNamespacedEngine(base2, "second"), tc.lookup)
			fields["reimport"], fields["reimport_lookup_rows"] = secondText, secondHits
			same := firstText == secondText && firstHits == secondHits
			fields["same"] = same
			verifyH10EmitCSV(t, fields)
			if !same {
				t.Errorf("DISAGREE round trip changed the nodes:\n first    %s (lookup rows %d)\n reimport %s (lookup rows %d)", firstText, firstHits, secondText, secondHits)
			}
		})
	}
}
