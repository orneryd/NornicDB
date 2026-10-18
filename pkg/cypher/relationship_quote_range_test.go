package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelationshipQuotedStringDoesNotPanic(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_relationship"))
		ctx := context.Background()
		if explicit {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		for _, expression := range []string{"'D):Z|Q{*..'", "\"D):Z|Q{*..\"", "{t:'D):Z|Q{*..'}.t"} {
			query := "WITH 1 AS i, 'DZ' AS t, ['DZ','X'] AS ts CREATE (a:A {id:1})-[r:$(" + expression + ") {w:1}]->(b:B {id:2}) RETURN type(r)"
			assert.NotPanics(t, func() {
				result, err := exec.Execute(ctx, query, nil)
				if assert.NoError(t, err) {
					assert.Equal(t, [][]interface{}{{"D):Z|Q{*.."}}, result.Rows)
				}
			})
			declaration := relationshipDeclarationOf("r:$(" + expression + ") {w:1}")
			assert.Equal(t, []string{"$(" + expression + ")"}, declaration.types)
			assert.Equal(t, "{w:1}", declaration.properties)
		}
		if explicit {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
	}
}

func TestDynamicRelationshipRangeNeo4j526(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_range_526"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:A)-[:`D{Z}..*` {w:1}]->(:B)-[:DZ {w:1}]->(:C)", nil)
		require.NoError(t, err)
		if explicit {
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		result, err := exec.Execute(ctx, "MATCH (a:A)-[r:$('D{Z}..*')*1..2 {w:1}]->() RETURN size(r) AS hops ORDER BY hops", nil)
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
		if explicit {
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
	}
}

// Neo4j 5.26.30 ignores a dynamic type on a legacy range, including the first
// hop and row values that would be invalid as single-hop dynamic types. The
// property map still filters every hop, and static types still filter normally.
func TestDynamicRelationshipRangeModes(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_modes"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:A)-[:R {w:1}]->(:B)-[:S {w:1}]->(:C), (a)-[:T {w:1}]->(:D)-[:T {w:2}]->(:E)", nil)
		require.NoError(t, err)
		if explicit {
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		for _, typeText := range []string{"$('R')", "$('NOPE')", "$all(['R'])", "$any(['R'])", "$(t)"} {
			for _, clause := range []string{"MATCH", "OPTIONAL MATCH"} {
				query := "WITH 'R' AS t " + clause + " (a:A)-[r:" + typeText + "*1..2 {w:1}]->() RETURN size(r) AS hops ORDER BY hops"
				result, err := exec.Execute(ctx, query, nil)
				if assert.NoError(t, err, query) {
					assert.Equal(t, [][]interface{}{{int64(1)}, {int64(1)}, {int64(2)}}, result.Rows, query)
				}
			}
		}
		for _, typeText := range []string{"R", "$('R')"} {
			length := ""
			if typeText == "R" {
				length = "*1..2"
			}
			result, err := exec.Execute(ctx, "MATCH (a:A)-[r:" + typeText + length + " {w:1}]->() RETURN count(r)", nil)
			require.NoError(t, err)
			assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		}
		result, err := exec.Execute(ctx, "UNWIND ['R',123] AS t MATCH (a:A)-[r:$(t)*1..2 {w:1}]->() RETURN count(r)", nil)
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(6)}}, result.Rows)
		result, err = exec.Execute(ctx, "UNWIND ['R',null] AS t MATCH (a:A)-[r:$(t)*0..2 {w:1}]->() RETURN count(r)", nil)
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(8)}}, result.Rows)
		if explicit {
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
	}
}
