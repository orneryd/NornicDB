package apocexport_test

// The apoc.export.csv writer emits one header row followed by one row per node.
// A CSV field is meaningful only together with the header above it, so these
// checks resolve every emitted field through its own header name instead of
// assuming a column order: the header row and a data row that describe the same
// node must agree field for field.

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/stretchr/testify/require"
)

// csvAlignmentKeys is the exported property schema. The writer's established
// schema is the first node's property set, so the fixture stays inside it.
var csvAlignmentKeys = []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}

// csvAlignmentFixture is a multi-node graph whose values are unique per node and
// property, so a value written under the wrong header cannot match it. One value
// needs CSV quoting, and one node omits a property so that column's field stays
// empty for that row.
func csvAlignmentFixture() []*apocexport.Node {
	nodes := []*apocexport.Node{
		{ID: 1, Labels: []string{"Person", "Employee"}},
		{ID: 2},
		{ID: 3, Labels: []string{"Company"}},
		{ID: 4, Labels: []string{"Person"}},
		{ID: 5},
	}
	for index := range nodes {
		properties := make(map[string]interface{}, len(csvAlignmentKeys))
		for _, key := range csvAlignmentKeys {
			properties[key] = fmt.Sprintf("%s-%d", key, nodes[index].ID)
		}
		nodes[index].Properties = properties
	}
	nodes[1].Properties["charlie"] = "comma,quote\"and\nnewline-2"
	delete(nodes[2].Properties, "bravo")
	return nodes
}

// TestCsvExportsEachFieldUnderItsOwnHeader drives the Go entry point and every
// bundled csv alias with an admitted caller, then parses the written document
// and checks that each emitted property field sits under the header naming it.
func TestCsvExportsEachFieldUnderItsOwnHeader(t *testing.T) {
	nodes := csvAlignmentFixture()
	for _, name := range []string{"GoCsv", "apoc.export.csv", "apoc.export.csvAll", "apoc.export.csvData"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nodes.csv")
			var err error
			if name == "GoCsv" {
				err = apocexport.Csv(exportCaller(t), nodes, path)
			} else {
				_, err = callRegisteredExport(t, name, exportCaller(t), nodes, path)
			}
			require.NoError(t, err)
			requireCSVHeaderAlignment(t, path, nodes)
		})
	}
}

// requireCSVHeaderAlignment parses an exported document and checks every emitted
// field against the value its own header names. It accepts any column order and
// rejects a value that landed under another property's header.
func requireCSVHeaderAlignment(t *testing.T, path string, nodes []*apocexport.Node) {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	rows, err := csv.NewReader(file).ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, len(nodes)+1, "one header row and one row per node")

	header := rows[0]
	require.GreaterOrEqual(t, len(header), 2)
	require.Equal(t, []string{"id", "labels"}, header[:2])
	columns := make(map[string]int, len(header)-2)
	for index, name := range header[2:] {
		_, duplicate := columns[name]
		require.False(t, duplicate, "property column %q appears once", name)
		columns[name] = index + 2
	}
	require.Len(t, columns, len(csvAlignmentKeys), "the first node's property set is the exported schema")

	for rowIndex, node := range nodes {
		row := rows[rowIndex+1]
		require.Len(t, row, len(header), "row %d carries one field per header", rowIndex)
		require.Equal(t, strconv.FormatInt(node.ID, 10), row[0], "row %d id field", rowIndex)
		require.Equal(t, strings.Join(node.Labels, ";"), row[1], "row %d labels field", rowIndex)
		for name, index := range columns {
			expected := ""
			if value, found := node.Properties[name]; found {
				expected = fmt.Sprint(value)
			}
			require.Equal(t, expected, row[index], "node %d column %q", node.ID, name)
		}
	}
}
