package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The composite route must consume the same import facts as direct CALL:
// nested declarations stay local; explicit imports remain correlated.
func TestFabricCallImportScope(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalize=%t/explicit=%t", normalize, explicit), func(t *testing.T) {
				config.SetCypherQueryNormalizationEnabled(normalize)
				exec := newFabricCallRowsExecutor(t)
				ctx := context.Background()
				_, err := exec.Execute(ctx, "USE comp.one CREATE (:ImportA {id:1}), (:ImportA {id:2}), (:ImportB)-[:IMPORT_R]->(:ImportC {id:9})", nil)
				require.NoError(t, err)
				if explicit {
					exec = fabricCallRowsCompositeExecutor(t, exec)
				}
				outer := "USE comp CALL { USE comp.one MATCH (n:ImportA) RETURN n, n.id AS k } "
				tail := " WITH k, c ORDER BY k RETURN collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r"
				for _, tc := range []struct{ name, call, want string }{
					{"import_scalar", "CALL { WITH k USE comp.one MATCH (m:ImportA) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
					{"scope_scalar", "CALL (k) { USE comp.one MATCH (m:ImportA) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
					{"implicit_scalar_extension", "CALL { USE comp.one MATCH (m:ImportA) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
					{"nested_local", "CALL { USE comp.one MATCH (m:ImportB) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c }", `["1:1", "2:1"]`},
					{"nested_imported", "CALL { WITH n USE comp.one MATCH (m:ImportB) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c }", `["1:0", "2:0"]`},
					{"import_alias_rejected", "CALL { WITH k AS kk USE comp.one RETURN kk AS c }", "ERROR"},
					{"import_star", "CALL { WITH * USE comp.one RETURN k AS c }", `["1:1", "2:2"]`},
					{"import_where_rejected", "CALL { WITH k WHERE k > 1 USE comp.one RETURN k AS c }", "ERROR"},
					{"scope_star", "CALL (*) { USE comp.one RETURN k AS c }", `["1:1", "2:2"]`},
					{"scope_empty_local", "CALL () { USE comp.one MATCH (m:ImportB) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c }", `["1:1", "2:1"]`},
					{"import_two", "CALL { WITH k, n USE comp.one MATCH (m:ImportA) WHERE m.id = k RETURN count(m) AS c }", `["1:1", "2:1"]`},
					{"property_name_control", "CALL { USE comp.one MATCH (m:ImportB) WHERE m.k IS NULL RETURN count(m) AS c }", `["1:1", "2:1"]`},
				} {
					t.Run(tc.name, func(t *testing.T) {
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
						}
						query := outer + tc.call + tail
						result, err := exec.Execute(ctx, query, nil)
						if tc.want == "ERROR" {
							require.Error(t, err, query)
							return
						}
						require.NoError(t, err, query)
						encoded, err := json.Marshal(result.Rows)
						require.NoError(t, err)
						require.JSONEq(t, "[["+tc.want+"]]", string(encoded), query)
					})
				}
			})
		}
	}
}
