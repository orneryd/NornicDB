package fabric

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// Author: #1663 lane. This public compatibility proof runs unchanged as an
// overlay on release cd67390c0. Native-policy admission belongs
// to the eventual Cypher host; an unconfigured peer preserves these bytes.
func TestStandaloneNativePolicyReleaseContract(t *testing.T) {
	for _, query := range []string{
		"CREATE DECAY PROFILE d OPTIONS { halfLifeSeconds: 3600, function: 'none' }",
		"CREATE PROMOTION PROFILE p OPTIONS { multiplier: 1.0 }",
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { WHEN n.name = 'APPLY' APPLY PROFILE 'p' ON ACCESS { SET n.a = {x: 'SET; WITH'} SET n.b = coalesce(n.with, 'WITH') } }",
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { ON ACCESS { SET n.a = {x 1} } }",
		"CREATE DECAY PROFILE d OPTIONS {",
	} {
		t.Run(query, func(t *testing.T) {
			catalog := NewCatalog()
			// newTestLocalExecutor's local storage registers db1.
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			planner := NewFabricPlanner(catalog)
			fragment, err := planner.Plan(t.Context(), "  "+query+"  ", "db1", cyphertext.AcceptedSource)
			require.NoError(t, err)
			exec, ok := fragment.(*FragmentExec)
			require.True(t, ok)
			require.Equal(t, query, exec.Query)
			require.Equal(t, "db1", exec.GraphName)
			require.True(t, exec.IsWrite)
			require.Nil(t, exec.Graph)
			require.Empty(t, exec.OutputColumns())
			init, ok := exec.Input.(*FragmentInit)
			require.True(t, ok)
			require.Empty(t, init.OutputColumns())
			mock := &mockCypherExecutor{results: map[string]*ResultStream{
				query: {Columns: []string{"accepted"}, Rows: [][]interface{}{{query}}},
			}, calls: map[string]int{}}
			gateway := NewQueryGateway(planner, NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil))
			result, err := gateway.Execute(t.Context(), nil, "  "+query+"  ", "db1", nil, "")
			require.NoError(t, err)
			require.Equal(t, []string{"accepted"}, result.Columns)
			require.Equal(t, [][]interface{}{{query}}, result.Rows)
			require.Equal(t, 1, mock.calls[query])
		})
	}
}
