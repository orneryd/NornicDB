package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalScopeRecordingCache(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	ctx := context.Background()
	source := "WITH $v AS value CALL (value) { RETURN value AS innerValue } RETURN value, innerValue"
	_, first, err := exec.compileLogicalScopes(ctx, source)
	require.NoError(t, err)
	require.True(t, first.sealed)
	_, second, err := exec.compileLogicalScopes(ctx, source)
	require.NoError(t, err)
	require.Same(t, first, second)
	columns, known, err := first.resultColumns()
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, []string{"value", "innerValue"}, columns)
	columns[0] = "caller owns this slice"
	columns, known, err = second.resultColumns()
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, []string{"value", "innerValue"}, columns)
	for _, value := range []interface{}{"first", int64(42)} {
		result, err := exec.Execute(ctx, source, map[string]interface{}{"v": value})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{value, value}}, result.Rows)
	}
	_, literalOne, err := exec.compileLogicalScopes(ctx, "WITH 1 AS v RETURN v")
	require.NoError(t, err)
	_, literalTwo, err := exec.compileLogicalScopes(ctx, "WITH 2 AS v RETURN v")
	require.NoError(t, err)
	require.NotSame(t, literalOne, literalTwo)
	exec.fabricRecordBindings = map[string]interface{}{"external": int64(1)}
	_, boundOne, err := exec.compileLogicalScopes(ctx, "RETURN external")
	require.NoError(t, err)
	_, boundTwo, err := exec.compileLogicalScopes(ctx, "RETURN external")
	require.NoError(t, err)
	require.NotSame(t, boundOne, boundTwo)
	require.False(t, boundOne.sealed)
	exec.fabricRecordBindings = nil
	_, _, err = exec.compileLogicalScopes(ctx, "RETURN external")
	require.Error(t, err)
	// A scope-only recording cannot admit an unknown function.
	_, partial := exec.logicalScopeRecording("RETURN missingScopeCacheFunction() AS v", false)
	require.NoError(t, exec.validateMatchSemanticScopes(partial.source, false, partial))
	require.Equal(t, []string{"v"}, exec.StatementColumns("RETURN missingScopeCacheFunction() AS v"))
	_, err = exec.Execute(ctx, "RETURN missingScopeCacheFunction() AS v", nil)
	require.Error(t, err)
	// Pagination still checks each invocation's actual parameter value.
	query := "UNWIND [1,2] AS v RETURN v LIMIT $limit"
	_, err = exec.Execute(ctx, query, map[string]interface{}{"limit": int64(1)})
	require.NoError(t, err)
	_, err = exec.Execute(ctx, query, map[string]interface{}{"limit": int64(-1)})
	require.Error(t, err)
}

func TestLogicalScopeRecordingConcurrentMetadata(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	source := "UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN * UNION RETURN 'outer' AS seed"
	_, record, err := exec.compileLogicalScopes(context.Background(), source)
	require.NoError(t, err)
	require.True(t, record.sealed)
	// The first projection materialization also races, through the same record.
	var wait sync.WaitGroup
	errors := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for repeat := 0; repeat < 50; repeat++ {
				columns, known, err := record.resultColumns()
				if err != nil || !known || len(columns) != 1 || columns[0] != "seed" {
					errors <- fmt.Errorf("columns = %v", columns)
					return
				}
				columns[0] = "private mutation"
			}
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestLogicalScopeMetadataWarmAllocationBound(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	source := "UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN * UNION RETURN 'outer' AS seed"
	_, record, err := exec.compileLogicalScopes(context.Background(), source)
	require.NoError(t, err)
	warm, known, err := record.resultColumns()
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, []string{"seed"}, warm)
	var columns []string
	allocations := testing.AllocsPerRun(100, func() { columns, _, _ = record.resultColumns() })
	require.Equal(t, []string{"seed"}, columns)
	// The only recurring allocation is the caller's own one-column slice.
	require.LessOrEqual(t, allocations, float64(1))
}

// Column readers also consume relative rewrite bodies, whose outer imports
// are unavailable here. They must not create a complete-statement verdict.
func TestStatementColumnReadersDoNotAdmitPartialScopes(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	body := "WITH true AS local RETURN *"
	require.Empty(t, exec.StatementColumns(body))
	require.Nil(t, exec.matchSemanticValidationCache.recording(semanticCacheKey(body, false)))
}
