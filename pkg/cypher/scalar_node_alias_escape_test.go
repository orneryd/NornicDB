package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestScalarNodeDemandPreservesEveryAliasEscape(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:EIBig) WITH n AS a, n AS b RETURN b",
		"MATCH (n:EIBig) WITH n AS a, n AS b RETURN a, b",
		"MATCH (n:EIBig) WITH n AS a, n AS n RETURN n",
	} {
		t.Run(query, func(t *testing.T) {
			graph := newScalarNodeDemandGraph(t, true)
			result := graph.run(t, query)
			require.Len(t, result.Rows, 1)
			for _, value := range result.Rows[0] {
				node, ok := value.(*storage.Node)
				require.True(t, ok, "an alias must retain its node value")
				require.Equal(t, scalarNodeWholeProperties(), node.Properties)
			}
		})
	}
}
