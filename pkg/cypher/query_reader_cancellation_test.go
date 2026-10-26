// Author of retained syntax cache key adaptation: Codex R17 implementation agent.
package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

func TestQueryReaderCancellationDoesNotPoisonCompilation(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		for _, version := range []string{"5", "25"} {
			t.Run(parser.name+"/"+version, func(t *testing.T) {
				t.Cleanup(parser.setup())
				query := fmt.Sprintf("MATCH (n:ReaderCanceled%sVersion%s) RETURN count(n) AS count", parser.name, version)
				key := semanticCacheKey(query, version == "25")
				pipelineClauseSplits.Lock()
				delete(pipelineClauseSplits.splits, pipelineClauseKey{source: key})
				pipelineClauseSplits.Unlock()
				checkpoint := newCheckpointContext(32, nil)
				defer checkpoint.cancel()
				ctx := withCypherVersion(checkpoint, "CYPHER "+version+" "+query)
				clauses, ok := splitPipelineClauses(ctx, query)
				require.ErrorIs(t, checkpoint.Context.Err(), context.Canceled)
				require.False(t, ok)
				require.Nil(t, clauses)
				pipelineClauseSplits.RLock()
				_, cached := pipelineClauseSplits.splits[pipelineClauseKey{source: key}]
				pipelineClauseSplits.RUnlock()
				require.False(t, cached, "canceled syntax must not publish a declined split")

				executor := NewStorageExecutor(newTestMemoryEngine(t))
				checkpoint = newCheckpointContext(32, nil)
				defer checkpoint.cancel()
				ctx = withCypherVersion(checkpoint, "CYPHER "+version+" "+query)
				err := executor.validateSemanticScopes(ctx, query)
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, executor.semanticValidationCache.contains(key))
				require.False(t, executor.matchSemanticValidationCache.contains(key))

				checkpoint = newCheckpointContext(32, nil)
				defer checkpoint.cancel()
				ctx = withCypherVersion(checkpoint, "CYPHER "+version+" "+query)
				names := resultmetadata.Known(nil)
				program := executor.projectedStatementScope(ctx, query, names, names, version == "25")
				require.ErrorIs(t, program.err, context.Canceled)
				_, cached = projectionFactsCache.get(projectionKey(query, names, names, version == "25"))
				require.False(t, cached, "a canceled bound program must not replace later live compilation")

				live := withCypherVersion(t.Context(), "CYPHER "+version+" "+query)
				clauses, ok = splitPipelineClauses(live, query)
				require.True(t, ok, "a live attempt must compile the same query after cancellation")
				require.Len(t, clauses, 2)
				require.NoError(t, executor.validateSemanticScopes(live, query))
				require.True(t, executor.semanticValidationCache.contains(key))
				result, err := executor.Execute(live, query, nil)
				require.NoError(t, err)
				require.Equal(t, []string{"count"}, result.Columns)
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			})
		}
	}
}

func TestPublicExecuteDeadlinePreservesLiveCompilation(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	const query = "MATCH (n:PublicDeadlineCompilation) RETURN count(n) AS count"
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := executor.Execute(expired, query, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, result)
	require.False(t, executor.semanticValidationCache.contains(query))
	require.False(t, executor.matchSemanticValidationCache.contains(query))
	result, err = executor.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"count"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}
