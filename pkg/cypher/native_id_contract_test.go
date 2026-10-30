package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// v1.4.3 retains the documented native string ID API by explicit maintainer
// decision (#1165); Bolt entity structure IDs are a separate representation.
func TestAcceptedNativeIDContract(t *testing.T) {
	_, run := newGDSGraph(t)
	r, err := run("MATCH (n:P) RETURN id(n) AS native, elementId(n) AS element")
	require.NoError(t, err)
	require.NotEmpty(t, r.Rows)
	for _, row := range r.Rows {
		id, ok := row[0].(string)
		require.True(t, ok)
		require.NotEmpty(t, id)
		element, ok := row[1].(string)
		require.True(t, ok)
		require.True(t, strings.HasPrefix(element, "4:"))
		require.True(t, strings.HasSuffix(element, ":"+id))
	}
	r, err = run("MATCH ()-[r:K]->() RETURN id(r) AS native, elementId(r) AS element")
	require.NoError(t, err)
	require.NotEmpty(t, r.Rows)
	for _, row := range r.Rows {
		id, ok := row[0].(string)
		require.True(t, ok)
		require.NotEmpty(t, id)
		element, ok := row[1].(string)
		require.True(t, ok)
		require.True(t, strings.HasPrefix(element, "5:"))
		require.True(t, strings.HasSuffix(element, ":"+id))
	}
	_, err = run("CALL gds.graph.project('native_id_contract', 'P', 'K')")
	require.NoError(t, err)
	r, err = run("CALL gds.fastRP.stream('native_id_contract', {embeddingDimension:4, randomSeed:7}) YIELD nodeId RETURN nodeId")
	require.NoError(t, err)
	require.Len(t, r.Rows, 4)
	for _, row := range r.Rows {
		id, ok := row[0].(string)
		require.True(t, ok, "GDS nodeId retains the native string contract")
		require.NotEmpty(t, id)
	}
}
