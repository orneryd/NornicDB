package adminimport_test

// Verifier of hunt leads L-H2-001..005, 008, 009 (v1.5.2 verification campaign,
// test-side only). Every source graph is written through public Cypher
// (StorageExecutor.Execute), which also checks the leads' premise that Cypher
// can store the shape; the graph is then exported with ExportNeo4jCSV and
// imported with ImportFull from DiscoverNeo4jCSVSources, as the admin export and
// import commands do, and both stores are read back through Cypher.
//
// Each case is a subtest "<lead>/<case>". A "fail" case must disagree on a
// build that has the defect (it reports DISAGREE); a "control" case must
// round-trip on every build.

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

type verifyH2Case struct {
	lead  string
	name  string
	role  string // "fail" or "control"
	setup []string
}

func verifyH2Store(t *testing.T, namespace string) (storage.Engine, storage.Engine) {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	return base, storage.NewNamespacedEngine(base, namespace)
}

// verifyH2Read is the graph as a user reads it through Cypher: every node's
// uid, labels and properties, and every relationship's uid, type and
// properties, ordered by uid. Go types are kept (%#v).
func verifyH2Read(t *testing.T, engine storage.Engine) (string, []map[string]any) {
	t.Helper()
	exec := cypher.NewStorageExecutor(engine)
	ctx := context.Background()
	var rows []map[string]any
	for _, q := range []string{
		"MATCH (n) RETURN 'node' AS kind, n.uid AS uid, labels(n) AS labels, properties(n) AS props",
		"MATCH ()-[r]->() RETURN 'rel' AS kind, r.uid AS uid, type(r) AS labels, properties(r) AS props",
	} {
		res, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
		for _, row := range res.Rows {
			m := map[string]any{}
			for i, c := range res.Columns {
				m[c] = row[i]
			}
			rows = append(rows, m)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i]["kind"], rows[i]["uid"]) < fmt.Sprint(rows[j]["kind"], rows[j]["uid"])
	})
	return fmt.Sprintf("%#v", rows), rows
}

func verifyH2Emit(t *testing.T, fields map[string]any) {
	t.Helper()
	data, _ := json.Marshal(fields)
	t.Logf("VERIFY_H2 %s", data)
}

func TestVerifyH2CSVRoundTrip(t *testing.T) {
	cases := []verifyH2Case{
		// L-H2-001: ';' inside a string-list element.
		{"L-H2-001", "node-list-element-with-semicolon", "fail", []string{"CREATE (:Item {uid: 'n1', tags: ['a;b', 'c']})"}},
		{"L-H2-001", "rel-list-element-with-semicolon", "fail", []string{"CREATE (:Item {uid: 'n1'})-[:R {uid: 'e1', w: ['a;b']}]->(:Item {uid: 'n2'})"}},
		{"L-H2-001", "control-list-without-semicolon", "control", []string{"CREATE (:Item {uid: 'n1', tags: ['a', 'c']})-[:R {uid: 'e1', w: ['ab']}]->(:Item {uid: 'n2'})"}},
		// L-H2-002: ';' inside a label.
		{"L-H2-002", "label-with-semicolon", "fail", []string{"CREATE (:`A;B` {uid: 'n1'})"}},
		{"L-H2-002", "control-label-with-comma", "control", []string{"CREATE (:`A,B` {uid: 'n1'})"}},
		// L-H2-003: one key, different types on different nodes.
		{"L-H2-003", "same-key-int-and-string", "fail", []string{"CREATE (:Item {uid: 'n1', code: 7}), (:Item {uid: 'n2', code: '007'})"}},
		{"L-H2-003", "same-key-bool-and-string", "fail", []string{"CREATE (:Item {uid: 'n1', flag: true}), (:Item {uid: 'n2', flag: 'yes'})"}},
		{"L-H2-003", "same-key-string-and-list", "fail", []string{"CREATE (:Item {uid: 'n1', v: 'x'}), (:Item {uid: 'n2', v: ['x', 'y']})"}},
		{"L-H2-003", "control-same-key-same-type", "control", []string{"CREATE (:Item {uid: 'n1', code: 7}), (:Item {uid: 'n2', code: 8})"}},
		// L-H2-004: empty lists and empty list elements.
		{"L-H2-004", "list-with-empty-element", "fail", []string{"CREATE (:Item {uid: 'n1', tags: ['', 'x']})"}},
		{"L-H2-004", "list-single-empty-element", "fail", []string{"CREATE (:Item {uid: 'n1', tags: ['']})"}},
		{"L-H2-004", "empty-list", "fail", []string{"CREATE (:Item {uid: 'n1', tags: []})"}},
		{"L-H2-004", "control-non-empty-list", "control", []string{"CREATE (:Item {uid: 'n1', tags: ['x']})"}},
		// L-H2-005: datetime values.
		{"L-H2-005", "datetime-utc", "fail", []string{"CREATE (:Item {uid: 'n1', at: datetime('2026-11-20T09:30:00.123456789Z')})"}},
		{"L-H2-005", "datetime-offset", "fail", []string{"CREATE (:Item {uid: 'n1', at: datetime('2026-11-20T10:30:00.123456789+01:00')})"}},
		{"L-H2-005", "control-string-property", "control", []string{"CREATE (:Item {uid: 'n1', at: '2026-11-20T09:30:00Z'})"}},
		// L-H2-008: mixed-type lists.
		{"L-H2-008", "list-int-and-string", "fail", []string{"CREATE (:Item {uid: 'n1', xs: [1, 'a']})"}},
		{"L-H2-008", "list-int-and-float", "fail", []string{"CREATE (:Item {uid: 'n1', xs: [1, 2.5]})"}},
		{"L-H2-008", "control-int-list", "control", []string{"CREATE (:Item {uid: 'n1', xs: [1, 2]})"}},
		// L-H2-009: property key with ':'.
		{"L-H2-009", "key-with-colon", "fail", []string{"CREATE (:Item {uid: 'n1', `a:b`: 'v'})"}},
		{"L-H2-009", "control-key-with-underscore", "control", []string{"CREATE (:Item {uid: 'n1', a_b: 'v'})"}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.lead+"/"+tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, source := verifyH2Store(t, "source")
			exec := cypher.NewStorageExecutor(source)
			fields := map[string]any{"lead": tc.lead, "case": tc.name, "role": tc.role, "setup": tc.setup}

			// Premise: Cypher stores the shape.
			for _, stmt := range tc.setup {
				if _, err := exec.Execute(ctx, stmt, nil); err != nil {
					fields["premise"] = "cypher-refused"
					fields["premise_error"] = err.Error()
					verifyH2Emit(t, fields)
					if tc.role == "control" {
						t.Fatalf("control setup refused: %v", err)
					}
					return // the lead's shape cannot be written through Cypher: no disagreement to test.
				}
			}
			fields["premise"] = "cypher-stored"
			stored, err := storage.CollectStoredNodes(ctx, source)
			require.NoError(t, err)
			storedTypes := map[string]string{}
			for _, n := range stored {
				for k, v := range n.Properties {
					storedTypes[fmt.Sprint(n.Properties["uid"], ".", k)] = fmt.Sprintf("%T=%#v", v, v)
				}
			}
			fields["stored_go_types"] = storedTypes
			sourceText, _ := verifyH2Read(t, source)
			fields["source"] = sourceText

			outDir := filepath.Join(t.TempDir(), "neo4j-csv")
			if err := adminimport.ExportNeo4jCSV(source, adminimport.Neo4jCSVExportOptions{OutputDir: outDir}); err != nil {
				fields["export_error"] = err.Error()
				verifyH2Emit(t, fields)
				t.Fatalf("DISAGREE export refused: %v", err)
			}
			if data, err := os.ReadFile(filepath.Join(outDir, "nodes.csv")); err == nil {
				fields["nodes_csv"] = string(data)
			}
			if data, err := os.ReadFile(filepath.Join(outDir, "relationships.csv")); err == nil {
				fields["relationships_csv"] = string(data)
			}
			nodeSources, relSources, err := adminimport.DiscoverNeo4jCSVSources(outDir, adminimport.Options{})
			require.NoError(t, err)

			base, _ := verifyH2Store(t, "unused")
			_, err = adminimport.ImportFull(ctx, base, adminimport.Options{
				DatabaseName: "roundtrip",
				NodeSources:  nodeSources,
				RelSources:   relSources,
				BuildIndexes: false,
			})
			if err != nil {
				fields["import_error"] = err.Error()
				fields["same"] = false
				verifyH2Emit(t, fields)
				t.Fatalf("DISAGREE import of the export refused: %v", err)
			}
			importedText, _ := verifyH2Read(t, storage.NewNamespacedEngine(base, "roundtrip"))
			fields["imported"] = importedText
			same := sourceText == importedText
			fields["same"] = same
			verifyH2Emit(t, fields)
			if !same {
				t.Errorf("DISAGREE round trip changed the graph:\n source   %s\n imported %s", sourceText, importedText)
			}
		})
	}
}
