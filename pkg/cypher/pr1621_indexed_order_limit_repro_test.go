package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reproduction for PR #1621 (NOR-19), confirmed on release/v1.4.4 @ 29ecd7909.
//
// HANDOFF: with an index on :P(id), a WHERE on the indexed property plus a
// relationship property filter and ORDER BY ... LIMIT 1 returns no rows
// (autocommit and explicit transaction); the unindexed variants pass, as does
// the UNWIND [null] relationship-map case. SUSPECTED: the indexed seed path
// applies the LIMIT (hint.earlyLimit / orderExpr handling in
// pkg/cypher/pipeline_executor.go and pipeline_traversal_source.go) before the
// relationship-property filter rejects candidates. Fix must apply the full
// pattern before ORDER/SKIP/LIMIT. Also see TestTraversalLimitPlacement.
func TestReproPR1621(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, idx := range []bool{false, true} {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "repro"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, `CREATE (:P {id:1})-[:R {value:1}]->(:Q), (:P {id:2})-[:R {value:2}]->(:Q), (:P {id:3})-[:R {value:3}]->(:Q)`, nil)
			require.NoError(t, err)
			if idx {
				_, err = exec.Execute(ctx, `CREATE INDEX p_id FOR (p:P) ON (p.id)`, nil)
				require.NoError(t, err)
			}
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			r, err := exec.Execute(ctx, `MATCH (p:P)-[:R {value:3}]->(q:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1`, nil)
			require.NoError(t, err)
			assert.Equal(t, [][]interface{}{{int64(3)}}, r.Rows, "explicit=%v idx=%v", explicit, idx)
			r, err = exec.Execute(ctx, `UNWIND [null] AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q)`, nil)
			require.NoError(t, err)
			assert.Equal(t, [][]interface{}{{int64(0)}}, r.Rows, "null explicit=%v idx=%v", explicit, idx)
		}
	}
}

// An indexed candidate rejected by the relationship predicate must not consume
// the only requested result slot before the next candidate is examined.
func TestIndexedLimitAfterRejectedRelationshipCandidate(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "limit_candidate"))
	ctx := t.Context()
	_, err := exec.Execute(ctx, `CREATE (:P {id:1})-[:R {value:0}]->(:Q), (:P {id:2})-[:R {value:1}]->(:Q)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE INDEX p_id FOR (p:P) ON (p.id)`, nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, `MATCH (p:P)-[:R {value:1}]->(:Q) WHERE p.id > 0 RETURN p.id ORDER BY p.id LIMIT 1`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}
