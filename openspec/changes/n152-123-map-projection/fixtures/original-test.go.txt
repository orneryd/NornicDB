package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

// The projection produces a map, unequal to these integer k values. A valid
// read is empty; its receiver must not be reinterpreted as "b.k = a".
// Each cell has a fresh fixture/transaction, so a first failure cannot turn
// later observations into failures of an already-poisoned transaction.
func TestNOR319MapProjectionInsidePatternProperty(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := func(t *testing.T, explicit bool) *StorageExecutor {
			t.Helper()
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(t.Context(), "CREATE (a:A {k:7}), (b:B {k:7,id:'b'}), (c:B {k:8,id:'c'}), (a)-[:R]->(b), (a)-[:R]->(c)", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
			}
			control, err := exec.Execute(t.Context(), "MATCH (a:A)-->(b:B {k:a.k}) RETURN b.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{"b"}}, control.Rows, "a real matching edge exercises correlated pattern values")
			return exec
		}
		for _, explicit := range []bool{false, true} {
			name := "autocommit"
			if explicit {
				name = "explicit"
			}
			t.Run(name, func(t *testing.T) {
				for _, query := range []string{
					"MATCH (a)-->(b {k: a{.k}}) RETURN b",
					"MATCH (a:A) MATCH (a)-->(b {k: a{.k}}) RETURN b",
				} {
					for _, mode := range []string{"", "PROFILE ", "EXPLAIN "} {
						t.Run(mode+query, func(t *testing.T) {
							exec := fixture(t, explicit)
							result, err := exec.Execute(t.Context(), mode+query, nil)
							require.NoError(t, err)
							require.NotNil(t, result)
							if mode != "EXPLAIN " {
								require.Equal(t, []string{"b"}, result.Columns)
								require.Empty(t, result.Rows, "integer k does not equal a map projection")
							}
						})
					}
				}
				t.Run("undefined receiver remains invalid", func(t *testing.T) {
					exec := fixture(t, explicit)
					_, err := exec.Execute(t.Context(), "MATCH (a:A) RETURN missing{.k}", nil)
					require.Error(t, err)
				})
			})
		}
	})
}
