package cypher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// gdsLiteralNameFixture has one relationship of each type whose name looks
// like relationship-pattern syntax, the plain types A and B, and a node whose
// one label is `D5G|N`.
const gdsLiteralNameFixture = "CREATE (a:D5G)-[:`A|B`]->(b:D5G), (a)-[:A]->(b), (a)-[:B]->(b), (a)-[:`A:B`]->(b), (a)-[:`A``B`]->(b), (a)-[:`$(R)`]->(b), (:`D5G|N`)"

type gdsLiteralNameCase struct {
	name       string
	nodes      any
	types      any
	nodeCount  int64
	relCount   int64
	wantsError bool
}

// The GDS 2.13.12 results on gdsLiteralNameFixture: a projected string or
// list item is one label or type name, taken literally.
var gdsLiteralNameCases = []gdsLiteralNameCase{
	{name: "pipe-string", nodes: "D5G", types: "A|B", nodeCount: 2, relCount: 1},
	{name: "colon-string", nodes: "D5G", types: "A:B", nodeCount: 2, relCount: 1},
	{name: "backtick-string", nodes: "D5G", types: "A`B", nodeCount: 2, relCount: 1},
	{name: "dynamic-looking", nodes: "D5G", types: "$(R)", nodeCount: 2, relCount: 1},
	{name: "pipe-list", nodes: "D5G", types: []any{"A|B"}, nodeCount: 2, relCount: 1},
	{name: "colon-list", nodes: "D5G", types: []any{"A:B"}, nodeCount: 2, relCount: 1},
	{name: "names-list", nodes: "D5G", types: []any{"A", "B"}, nodeCount: 2, relCount: 2},
	{name: "pipe-map", nodes: "D5G", types: map[string]any{"literal": map[string]any{"type": "A|B"}}, nodeCount: 2, relCount: 1},
	{name: "all", nodes: "D5G", types: "*", nodeCount: 2, relCount: 6},
	{name: "pipe-label", nodes: "D5G|N", types: "*", nodeCount: 1, relCount: 0},
	{name: "pipe-label-list", nodes: []any{"D5G|N"}, types: "*", nodeCount: 1, relCount: 0},
	{name: "spaced-type", nodes: "D5G", types: " A ", wantsError: true},
	{name: "empty-type", nodes: "D5G", types: "", wantsError: true},
	{name: "missing-split-type", nodes: "D5G", types: "A|C", wantsError: true},
}

const gdsLiteralNameQuery = "CALL gds.graph.project($g, $n, $r) YIELD nodeCount, relationshipCount RETURN nodeCount, relationshipCount"

func newGDSLiteralNameExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	gdsFollowUpCatalog(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gds_literal_names"))
	_, err := exec.Execute(context.Background(), gdsLiteralNameFixture, nil)
	require.NoError(t, err)
	return exec
}

// A label or type name given to a GDS projection as a string or list item is
// data, read by the one literal-name reader: 'A|B' projects the type A|B,
// never A and B.
func TestGDSProjectionNamesAreLiteral(t *testing.T) {
	exec := newGDSLiteralNameExecutor(t)
	for index, test := range gdsLiteralNameCases {
		t.Run(test.name, func(t *testing.T) {
			params := map[string]any{"g": fmt.Sprintf("literal_%d", index), "n": test.nodes, "r": test.types}
			result, err := exec.Execute(context.Background(), gdsLiteralNameQuery, params)
			if test.wantsError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, [][]any{{test.nodeCount, test.relCount}}, result.Rows)
		})
	}
}

// The same projections against live Neo4j GDS (--ref gds) give the same
// counts, or fail as it does.
func TestGDSProjectionNamesMatchReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires --ref gds")
	}
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	if _, err := neo4j.ExecuteQuery(ctx, driver, "RETURN gds.version() AS version", nil, neo4j.EagerResultTransformer); err != nil {
		t.Skipf("requires --ref gds; the reference has no GDS: %v", err)
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	run := func(query string, params map[string]any) ([]*neo4j.Record, error) {
		result, err := session.Run(ctx, query, params)
		if err != nil {
			return nil, err
		}
		return result.Collect(ctx)
	}
	_, err = run("MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)
	_, err = run(gdsLiteralNameFixture, nil)
	require.NoError(t, err)
	exec := newGDSLiteralNameExecutor(t)
	for index, test := range gdsLiteralNameCases {
		t.Run(test.name, func(t *testing.T) {
			name := fmt.Sprintf("literal_reference_%d", index)
			drop := func() {
				_, err := run("CALL gds.graph.drop($g, false)", map[string]any{"g": name})
				require.NoError(t, err)
			}
			drop()
			t.Cleanup(drop)
			params := map[string]any{"g": name, "n": test.nodes, "r": test.types}
			records, referenceErr := run(gdsLiteralNameQuery, params)
			result, err := exec.Execute(ctx, gdsLiteralNameQuery, params)
			if referenceErr != nil {
				require.Error(t, err, "GDS fails: %v", referenceErr)
				require.True(t, test.wantsError, "GDS fails: %v", referenceErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, records, 1)
			want := [][]any{{records[0].Values[0], records[0].Values[1]}}
			require.Equal(t, want, result.Rows)
			require.Equal(t, [][]any{{test.nodeCount, test.relCount}}, want, "the fixed expectation records GDS's result")
		})
	}
}

// resolvedTokenNames reads a string as one literal name and several names
// only from a list.
func TestResolvedTokenNames(t *testing.T) {
	for _, test := range []struct {
		value any
		names []string
		bad   any
		ok    bool
	}{
		{value: "A|B", names: []string{"A|B"}, ok: true},
		{value: "A:B", names: []string{"A:B"}, ok: true},
		{value: "", names: []string{""}, ok: true},
		{value: []string{"A", "B|C"}, names: []string{"A", "B|C"}, ok: true},
		{value: []any{"A|B", "C"}, names: []string{"A|B", "C"}, ok: true},
		{value: []any{}, names: []string{}, ok: true},
		{value: []any{"A", int64(1)}, bad: int64(1)},
		{value: int64(1), bad: int64(1)},
		{value: nil},
		{value: map[string]any{"type": "A"}, bad: map[string]any{"type": "A"}},
	} {
		names, bad, ok := resolvedTokenNames(test.value)
		require.Equal(t, test.ok, ok, "%#v", test.value)
		require.Equal(t, test.bad, bad, "%#v", test.value)
		if test.ok {
			require.Equal(t, test.names, names, "%#v", test.value)
		}
	}
}
