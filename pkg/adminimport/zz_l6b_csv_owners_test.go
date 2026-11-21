package adminimport

// Lane L6b (closeout): runs at the CSV owners read in source.
//   M01 L-H2-004  splitList drops empty elements; empty list cell skipped (preserveEmptyStringProperty)
//   M02 L-H2-003  mergeColumn collapses mixed shapes to string; L-H2-005 inferValueShape default branch
//   M04 L-H10-001 inferNodeColumns leaves out id == node ID
//   M03 L-H2-009  splitNameType splits a header key at its first ':'

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL6bCSVOwners(t *testing.T) {
	opts := Options{ArrayDelimiter: ';', Delimiter: ','}

	// M01 L-H2-004: export cells for ['', 'x'], [''] and [], and the import step.
	for _, cell := range []string{";x", "", "x"} {
		parts := splitList(cell, ';')
		parsed, err := parsePropertyValue(cell, columnSpec{Name: "v", Kind: kindProperty, Type: "string[]"}, opts)
		t.Logf("L6B_M01 splitList(importer.go:1230-1240, empty part dropped :1235) cell=%q -> %q | parsePropertyValue(string[])=%#v err=%v", cell, parts, parsed, err)
	}
	listCol := columnSpec{Name: "v", Kind: kindProperty, Type: "string[]"}
	strCol := columnSpec{Name: "v", Kind: kindProperty, Type: "string"}
	t.Logf("L6B_M01 preserveEmptyStringProperty(importer.go:1242) string[] -> %v | string -> %v", preserveEmptyStringProperty(listCol), preserveEmptyStringProperty(strCol))
	exportEmptyList := strings.Join([]string{}, ";")
	exportOneEmpty := strings.Join([]string{""}, ";")
	exportEmptyFirst := strings.Join([]string{"", "x"}, ";")
	t.Logf("L6B_M01 export join (neo4j_csv.go:395-397): []=%q [''] =%q ['', 'x']=%q", exportEmptyList, exportOneEmpty, exportEmptyFirst)
	if got := splitList(";x", ';'); len(got) == 1 && exportEmptyList == exportOneEmpty {
		t.Errorf("CAUSE M01: splitList drops the empty element (%q -> %q); [] and [''] export to the same cell %q, and the list column's empty cell is skipped (preserveEmptyStringProperty=false)", ";x", got, exportOneEmpty)
	}

	// M02 L-H2-003: mergeColumn over two shapes of one key.
	for _, pair := range [][2]any{{int64(7), "seven"}, {true, "x"}, {"s", []any{"x", "y"}}, {int64(7), int64(8)}} {
		col := mergeColumn(inferredColumn{}, "k", pair[0])
		col = mergeColumn(col, "k", pair[1])
		t.Logf("L6B_M02 mergeColumn(neo4j_csv.go:247, collapse :256-259) values=%#v,%#v -> header=%q kind=%q scalar=%q | formatScalar(first)=%q formatScalar(second)=%q",
			pair[0], pair[1], col.Header, col.Kind, col.ScalarType, formatScalar(pair[0], col.ScalarType), formatScalar(pair[1], col.ScalarType))
	}
	mixed := mergeColumn(mergeColumn(inferredColumn{}, "code", int64(7)), "code", "seven")
	if mixed.ScalarType == "string" {
		t.Errorf("CAUSE M02 L-H2-003: a key holding an integer and a string becomes one string column %q, so the integer comes back as a string", mixed.Header)
	}
	// M02 L-H2-005: a Cypher datetime value.
	dt, err := cypher.NewStorageExecutor(storage.NewNamespacedEngine(newL6bMemoryEngine(), "l6bcsv")).Execute(t.Context(), "RETURN datetime('2024-01-02T03:04:05Z') AS d", nil)
	if err != nil || dt == nil || len(dt.Rows) == 0 {
		t.Fatalf("datetime: %v", err)
	}
	value := dt.Rows[0][0]
	kind, scalar, _ := inferValueShape(value)
	t.Logf("L6B_M02 inferValueShape(neo4j_csv.go:274, default branch :325-327) value type=%T -> kind=%q scalar=%q formatScalar=%q", value, kind, scalar, formatScalar(value, scalar))
	parsedBack, _ := parseScalar("2024-01-02T03:04:05Z", "datetime", opts)
	t.Logf("L6B_M02 parseScalar(importer.go:721, datetime column) -> %T %#v", parsedBack, parsedBack)
	if scalar == "string" {
		t.Errorf("CAUSE M02 L-H2-005: inferValueShape has no case for %T, so it becomes a string column; and a datetime column is read back as %T", value, parsedBack)
	}

	// M04 L-H10-001: inferNodeColumns and a property id equal to the node ID.
	nodes := []*storage.Node{
		{ID: "2", Labels: []string{"Person"}, Properties: map[string]any{"id": "2", "name": "Bob"}},
	}
	control := []*storage.Node{
		{ID: "2", Labels: []string{"Person"}, Properties: map[string]any{"id": "other", "name": "Bob"}},
	}
	headers := func(cols []inferredColumn) []string {
		var out []string
		for _, c := range cols {
			out = append(out, c.Header)
		}
		return out
	}
	trigger := headers(inferNodeColumns(nodes))
	ctl := headers(inferNodeColumns(control))
	t.Logf("L6B_M04 inferNodeColumns(neo4j_csv.go:214, skip :218-220) id==nodeID -> %q | id!=nodeID -> %q", trigger, ctl)
	if !strings.Contains(fmt.Sprint(trigger), "id:") && strings.Contains(fmt.Sprint(ctl), "id:") {
		t.Errorf("CAUSE M04: the property id equal to the node ID is left out of the columns (%q), and the lead :ID header stores no property on import", trigger)
	}

	// M03 L-H2-009: header key containing ':'.
	for _, raw := range []string{"a:b:string", "a_b:string", "plain"} {
		name, typ := splitNameType(raw)
		t.Logf("L6B_M03 splitNameType(importer.go:666-671) %q -> name=%q type=%q", raw, name, typ)
	}
	header := mergeColumn(inferredColumn{}, "a:b", "v").Header
	name, typ := splitNameType(header)
	t.Logf("L6B_M03 export header for key %q = %q (neo4j_csv.go:269) -> import splitNameType name=%q type=%q", "a:b", header, name, typ)
	if name != "a:b" {
		t.Errorf("CAUSE M03: the exported header %q is split at the first ':' into name %q and type %q", header, name, typ)
	}
}

func newL6bMemoryEngine() storage.Engine { return storage.NewMemoryEngine() }
