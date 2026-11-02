package cypher

// PR1615 CSV field quoting.
//
// Every CSV field the export procedures publish goes through one field writer:
// the identifiers, labels, relationship types and endpoints of a whole-graph
// export, and the column names and values of a query export. These regressions
// pin that writer's rules, the published bytes of an ordinary export, and the
// encoding/csv roundtrip of fields whose own bytes are CSV syntax (a comma, a
// quote, a CR or a LF).
//
// A field whose bytes contain the two-byte CR LF sequence roundtrips as LF only,
// because encoding/csv's reader normalizes CR LF to LF on every input line; that
// is the reader's rule, not the encoder's, so the roundtrip cases below keep CR
// and LF separate.

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// csvFieldsSpecialValues are the field bytes a reader takes as CSV syntax, and so
// the bytes that must stay inside their own field.
var csvFieldsSpecialValues = []string{",", `"`, "\r", "\n"}

// csvFieldsExecutor is one executor whose local export file access is allowed, so
// its exports reach the public CALL path, with a temporary directory for its
// files.
func csvFieldsExecutor(t *testing.T) (*StorageExecutor, string) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "csv-fields"))
	exec.SetAllowLocalAPOCFileAccess(true)
	return exec, t.TempDir()
}

// csvFieldsRead parses one exported file with the public CSV reader, which is the
// oracle for the export's byte output.
func csvFieldsRead(t *testing.T, path string) [][]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	reader := csv.NewReader(strings.NewReader(string(raw)))
	reader.Comment = '#'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	require.NoError(t, err, "exported CSV must parse:\n%s", raw)
	return records
}

// TestWriteCSVField pins the one CSV field writer: an unquoted field is quoted
// only when its own bytes would move the field boundary, the export's always
// quoted fields (labels, properties, query values) are quoted even when they need
// no quoting, and a quoted field doubles its quotes.
func TestWriteCSVField(t *testing.T) {
	cases := []struct {
		name         string
		field        string
		alwaysQuoted bool
		want         string
	}{
		{name: "ordinary", field: "n1", want: "n1"},
		{name: "empty", field: "", want: ""},
		{name: "comma", field: "a,b", want: `"a,b"`},
		{name: "quote", field: `a"b`, want: `"a""b"`},
		{name: "carriage return", field: "a\rb", want: "\"a\rb\""},
		{name: "line feed", field: "a\nb", want: "\"a\nb\""},
		{name: "quotes only", field: `""`, want: `""""""`},
		{name: "forced ordinary", field: "Person", alwaysQuoted: true, want: `"Person"`},
		{name: "forced empty", field: "", alwaysQuoted: true, want: `""`},
		{name: "forced comma", field: "a,b", alwaysQuoted: true, want: `"a,b"`},
		{name: "forced quote", field: `"a"`, alwaysQuoted: true, want: `"""a"""`},
		{name: "forced line feed", field: "a\nb", alwaysQuoted: true, want: "\"a\nb\""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var builder strings.Builder
			writeCSVField(&builder, tc.field, tc.alwaysQuoted)
			require.Equal(t, tc.want, builder.String())

			if tc.field == "" {
				// A blank line is not a record for encoding/csv, so the empty
				// field's own bytes are asserted above only.
				return
			}
			records, err := csv.NewReader(strings.NewReader(builder.String() + "\n")).ReadAll()
			require.NoError(t, err, builder.String())
			require.Equal(t, [][]string{{tc.field}}, records)
		})
	}
}

// TestAPOCExportCSVOrdinaryBytes pins the published byte output of an ordinary
// export: the plain identifiers, labels, type and endpoints of a whole-graph
// export, and the header, the always quoted non-null values and the empty cell of
// a null value of a query export.
func TestAPOCExportCSVOrdinaryBytes(t *testing.T) {
	exec, dir := csvFieldsExecutor(t)

	_, err := exec.storage.CreateNode(t.Context(), &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person", "Employee"},
		Properties: map[string]interface{}{"name": "Alice"},
	})
	require.NoError(t, err)
	require.NoError(t, exec.storage.CreateEdge(t.Context(), &storage.Edge{
		ID:         "e1",
		StartNode:  "n1",
		EndNode:    "n1",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"since": int64(2020)},
	}))

	graphPath := filepath.Join(dir, "graph.csv")
	_, err = exec.Execute(context.Background(), "CALL apoc.export.csv.all($file,{})", map[string]interface{}{"file": graphPath})
	require.NoError(t, err)
	graphRaw, err := os.ReadFile(graphPath)
	require.NoError(t, err)
	require.Equal(t,
		"# Nodes\n"+
			"_id,_labels,properties\n"+
			"n1,\"Person;Employee\",\"{\"\"name\"\":\"\"Alice\"\"}\"\n"+
			"\n"+
			"# Relationships\n"+
			"_id,_type,_start,_end,properties\n"+
			"e1,KNOWS,n1,n1,\"{\"\"since\"\":2020}\"\n",
		string(graphRaw))

	queryPath := filepath.Join(dir, "query.csv")
	_, err = exec.Execute(context.Background(), "CALL apoc.export.csv.query($query,$file,{})", map[string]interface{}{
		"file":  queryPath,
		"query": `RETURN 42 AS number, null AS missing, "a" AS text`,
	})
	require.NoError(t, err)
	queryRaw, err := os.ReadFile(queryPath)
	require.NoError(t, err)
	require.Equal(t, "number,missing,text\n\"42\",,\"a\"\n", string(queryRaw))
}

// TestAPOCExportCSVSpecialFieldsRoundtrip proves fields whose own bytes are CSV
// syntax keep their field boundaries: identifiers, labels, relationship types and
// endpoints of a whole-graph export, and the column names, string values and JSON
// values of a query export.
func TestAPOCExportCSVSpecialFieldsRoundtrip(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("graph identifiers", func(t *testing.T) {
			exec, dir := csvFieldsExecutor(t)
			ids := make([]storage.NodeID, 0, len(csvFieldsSpecialValues)+1)
			for _, special := range csvFieldsSpecialValues {
				ids = append(ids, storage.NodeID("node"+special+"id"))
			}
			ids = append(ids, `node,"all"_together`)
			for i, id := range ids {
				_, err := exec.storage.CreateNode(t.Context(), &storage.Node{
					ID:         id,
					Labels:     []string{"la,bel" + csvFieldsSpecialValues[i%len(csvFieldsSpecialValues)] + "x"},
					Properties: map[string]interface{}{"index": int64(i)},
				})
				require.NoError(t, err)
			}
			edgeType := "LI,N\"K\"Z\rX"
			for i, id := range ids {
				require.NoError(t, exec.storage.CreateEdge(t.Context(), &storage.Edge{
					ID:         storage.EdgeID("edge," + string(id)),
					StartNode:  id,
					EndNode:    ids[(i+1)%len(ids)],
					Type:       edgeType,
					Properties: map[string]interface{}{"index": int64(i)},
				}))
			}

			path := filepath.Join(dir, "graph.csv")
			_, err := exec.Execute(context.Background(), "CALL apoc.export.csv.all($file,{})", map[string]interface{}{"file": path})
			require.NoError(t, err)

			records := csvFieldsRead(t, path)
			require.Len(t, records, 2+2*len(ids))
			require.Equal(t, []string{"_id", "_labels", "properties"}, records[0])
			require.Equal(t, []string{"_id", "_type", "_start", "_end", "properties"}, records[1+len(ids)])

			nodes := records[1 : 1+len(ids)]
			edges := records[2+len(ids):]
			nodeByID := make(map[string][]string, len(nodes))
			for _, record := range nodes {
				require.Len(t, record, 3)
				nodeByID[record[0]] = record
			}
			require.Len(t, nodeByID, len(ids), "each node record carries its own identifier")
			edgeByID := make(map[string][]string, len(edges))
			for _, record := range edges {
				require.Len(t, record, 5)
				edgeByID[record[0]] = record
			}
			require.Len(t, edgeByID, len(ids), "each relationship record carries its own identifier")

			for i, id := range ids {
				wantNode := []string{string(id), "la,bel" + csvFieldsSpecialValues[i%len(csvFieldsSpecialValues)] + "x", fmt.Sprintf(`{"index":%d}`, i)}
				require.Equal(t, wantNode, nodeByID[string(id)])

				edgeID := "edge," + string(id)
				require.Equal(t,
					[]string{edgeID, edgeType, string(id), string(ids[(i+1)%len(ids)]), fmt.Sprintf(`{"index":%d}`, i)},
					edgeByID[edgeID])
			}
		})

		t.Run("query columns and values", func(t *testing.T) {
			exec, dir := csvFieldsExecutor(t)

			path := filepath.Join(dir, "query.csv")
			_, err := exec.Execute(context.Background(), "CALL apoc.export.csv.query($query,$file,{})", map[string]interface{}{
				"file":  path,
				"query": "RETURN 1 AS `a,b`, 2 AS `q\"uote`, 3 AS plain",
			})
			require.NoError(t, err)
			require.Equal(t, [][]string{{"a,b", `q"uote`, "plain"}, {"1", "2", "3"}}, csvFieldsRead(t, path))

			valuesPath := filepath.Join(dir, "values.csv")
			_, err = exec.Execute(context.Background(), "CALL apoc.export.csv.query($query,$file,{})", map[string]interface{}{
				"file":  valuesPath,
				"query": `RETURN 'x,y' AS comma, 'x"y' AS quote, 'x\rCR' AS cr, 'x\nLF' AS lf, null AS missing, 3 AS number, [1,2] AS list`,
			})
			require.NoError(t, err)
			require.Equal(t,
				[][]string{
					{"comma", "quote", "cr", "lf", "missing", "number", "list"},
					{"x,y", `x"y`, "x\rCR", "x\nLF", "", "3", "[1,2]"},
				},
				csvFieldsRead(t, valuesPath))
		})

		t.Run("large graph", func(t *testing.T) {
			exec, dir := csvFieldsExecutor(t)
			const population = 2500
			ids := make([]storage.NodeID, population)
			for i := 0; i < population; i++ {
				ids[i] = storage.NodeID(fmt.Sprintf("node,%d \"x\"\nid", i))
			}
			for i, id := range ids {
				_, err := exec.storage.CreateNode(t.Context(), &storage.Node{
					ID:         id,
					Labels:     []string{"Ex,port"},
					Properties: map[string]interface{}{"index": int64(i)},
				})
				require.NoError(t, err)
			}
			for i, id := range ids {
				require.NoError(t, exec.storage.CreateEdge(t.Context(), &storage.Edge{
					ID:         storage.EdgeID(fmt.Sprintf("edge,%d\r\"id", i)),
					StartNode:  id,
					EndNode:    ids[(i+1)%len(ids)],
					Type:       "LI,N\"K\nZ",
					Properties: map[string]interface{}{"index": int64(i)},
				}))
			}

			path := filepath.Join(dir, "large.csv")
			_, err := exec.Execute(context.Background(), "CALL apoc.export.csv.all($file,{})", map[string]interface{}{"file": path})
			require.NoError(t, err)

			records := csvFieldsRead(t, path)
			require.Len(t, records, 2+2*population)
			nodes := make(map[string][]string, population)
			for _, record := range records[1 : 1+population] {
				require.Len(t, record, 3)
				nodes[record[0]] = record
			}
			require.Len(t, nodes, population, "every node keeps its own record")
			edges := make(map[string][]string, population)
			for _, record := range records[2+population:] {
				require.Len(t, record, 5)
				edges[record[0]] = record
			}
			require.Len(t, edges, population, "every relationship keeps its own record")
			for i, id := range ids {
				require.Equal(t, []string{string(id), "Ex,port", fmt.Sprintf(`{"index":%d}`, i)}, nodes[string(id)])
				require.Equal(t,
					[]string{fmt.Sprintf("edge,%d\r\"id", i), "LI,N\"K\nZ", string(id), string(ids[(i+1)%len(ids)]), fmt.Sprintf(`{"index":%d}`, i)},
					edges[fmt.Sprintf("edge,%d\r\"id", i)])
			}
		})
	})
}
