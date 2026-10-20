package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestAdmissionVersionPublicReroutes(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "use", "composite"} {
			for _, explicit := range []bool{false, true} {
				for _, sample := range []struct {
					name, query string
					invalid     bool
				}{
					{"conditional", "WITH 'outer' AS value CALL (value) { WHEN true THEN RETURN value AS tag ELSE RETURN value AS tag } RETURN tag", false},
					{"next", "RETURN 'outer' AS value NEXT WHEN true THEN RETURN value AS tag ELSE RETURN value AS tag", false},
					{"nested", "WITH 'outer' AS value CALL (value) { CALL (value) { WHEN true THEN RETURN value AS tag ELSE RETURN value AS tag } RETURN tag } RETURN tag", false},
					{"invalid_star", "WITH 'outer' AS value CALL (value) { WHEN true THEN RETURN *, 'tag' AS tag ELSE RETURN *, 'tag' AS tag } RETURN tag", true},
				} {
					for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
						t.Run(fmt.Sprintf("%s/%s/%s/norm_%v/tx_%v", sample.name, mode, route, normalize, explicit), func(t *testing.T) {
							exec := newFabricCallRowsExecutor(t)
							query := sample.query
							if route == "use" {
								exec = fabricCallRowsCompositeExecutor(t, exec)
								query = "USE comp.one " + query
							}
							if route == "composite" {
								exec = fabricCallRowsCompositeExecutor(t, exec)
								query = "USE comp CALL { USE comp.one " + query + " } RETURN tag"
							}
							if explicit {
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer exec.Execute(ctx, "ROLLBACK", nil)
							}
							result, err := exec.Execute(ctx, "CYPHER 25 "+mode+query, nil)
							if sample.invalid {
								require.Error(t, err)
								var semantic *SemanticError
								require.ErrorAs(t, err, &semantic)
								require.Equal(t, "UndefinedVariable", semantic.Detail)
								return
							}
							require.NoError(t, err)
							require.Equal(t, []string{"tag"}, result.Columns)
							if mode == "EXPLAIN " {
								require.Empty(t, result.Rows)
							} else {
								require.Equal(t, [][]interface{}{{"outer"}}, result.Rows)
							}
						})
					}
				}
			}
		}
	}
	// Prefix order is framing, and genuine Cypher 5 gates remain gates.
	exec := newFabricCallRowsExecutor(t)
	result, err := exec.Execute(ctx, "PROFILE CYPHER 25 LET value='outer' RETURN value AS tag", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"outer"}}, result.Rows)
	if config.IsANTLRParser() {
		_, err = exec.Execute(ctx, "CYPHER 5 LET value='outer' RETURN value AS tag", nil)
		require.Error(t, err)
	}
}
