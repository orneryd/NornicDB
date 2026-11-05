package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPR1612ReviewFraming(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, normalize := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalize=%v", normalize), func(t *testing.T) {
			config.SetCypherQueryNormalizationEnabled(normalize)
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
			_, err := e.Execute(context.Background(), "CREATE (:R1612)", nil)
			require.NoError(t, err)
			for _, q := range []string{"CYPHER runtime=slotted/* gap */ RETURN 1 AS x", "CYPHER runtime=slotted /* gap */ RETURN 1 AS x", "CYPHER/* header */5 runtime/* key */=/* value */slotted/* tail */ RETURN 1 AS x"} {
				t.Run(q, func(t *testing.T) {
					r, err := e.Execute(context.Background(), q, nil)
					require.NoError(t, err)
					require.Equal(t, []string{"x"}, r.Columns)
					require.Equal(t, [][]interface{}{{int64(1)}}, r.Rows)
				})
			}
			t.Run("unfinished comment after option", func(t *testing.T) {
				_, err := e.Execute(context.Background(), "CYPHER runtime=slotted /* incomplete", nil)
				requireStatusCode(t, err, "Neo.DatabaseError.Statement.ExecutionFailed")
			})

			for _, q := range []string{"WITH 1 AS x FINISH", "EXPLAIN WITH 1 AS x FINISH", "PROFILE WITH 1 AS x FINISH", "PROFILE CALL db.labels() YIELD label WHERE label = 'R1612' FINISH"} {
				t.Run(q, func(t *testing.T) {
					r, err := e.Execute(context.Background(), q, nil)
					require.NoError(t, err)
					require.Empty(t, r.Columns)
					require.Empty(t, r.Rows)
				})
			}
			t.Run("valid explained quoted column", func(t *testing.T) {
				r, err := e.Execute(context.Background(), "EXPLAIN /* gap */ RETURN 1 AS `a b`", nil)
				require.NoError(t, err)
				require.Equal(t, []string{"a b"}, r.Columns)
				require.Empty(t, r.Rows)
			})

			for _, q := range []string{"EXPLAIN /* gap */ RETURN missing", "EXPLAIN /* gap */ WITH 1 AS x RETURN missing"} {
				t.Run(q, func(t *testing.T) {
					_, err := e.Execute(context.Background(), q, nil)
					requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				})
			}
		})
	}
}
