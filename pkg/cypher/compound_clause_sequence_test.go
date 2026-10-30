package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnwindWithExecutesRemainingClauses(t *testing.T) {
	executor := setupTestExecutor(t)
	result := executeBehaviorQuery(t, executor,
		"UNWIND [1, 2] AS v WITH v CREATE (:Value {number: v}) RETURN v")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)

	count := executeBehaviorQuery(t, executor, "MATCH (n:Value) RETURN count(n)")
	require.Equal(t, [][]interface{}{{int64(2)}}, count.Rows)
}

func TestUnwindWithExecutesMergeForEveryRow(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor,
		"UNWIND [1, 2, 2] AS v WITH v MERGE (n:MergedValue {number: v})")

	result := executeBehaviorQuery(t, executor,
		"MATCH (n:MergedValue) RETURN n.number ORDER BY n.number")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
}

func TestStringPredicateFollowedByCreateExecutesPerMatch(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (:Name {value: 'Alice'}), (:Name {value: 'Bob'})")

	executeBehaviorQuery(t, executor,
		"MATCH (n:Name) WHERE n.value STARTS WITH 'A' CREATE (:Hit {value: n.value})")

	result := executeBehaviorQuery(t, executor, "MATCH (n:Hit) RETURN n.value")
	require.Equal(t, [][]interface{}{{"Alice"}}, result.Rows)
}

func TestStringSuffixPredicateFollowedByCreateExecutesPerMatch(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (:Name {value: 'Alice'}), (:Name {value: 'Bob'})")

	executeBehaviorQuery(t, executor,
		"MATCH (n:Name) WHERE n.value ENDS WITH 'b' CREATE (:SuffixHit {value: n.value})")

	result := executeBehaviorQuery(t, executor, "MATCH (n:SuffixHit) RETURN n.value")
	require.Equal(t, [][]interface{}{{"Bob"}}, result.Rows)
}
