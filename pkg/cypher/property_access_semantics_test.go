package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMissingIDPropertyReturnsNull(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (:Document {name: 'without-id'})")

	result := executeBehaviorQuery(t, executor,
		"MATCH (n:Document {name: 'without-id'}) RETURN n.id AS propertyID, id(n) AS internalID")

	require.Len(t, result.Rows, 1)
	require.Nil(t, result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])
}

func TestNullPredicateOnPropertyOfNullOptionalBinding(t *testing.T) {
	executor := setupTestExecutor(t)

	result := executeBehaviorQuery(t, executor,
		"OPTIONAL MATCH (n) RETURN n.missing IS NULL AS missingIsNull")

	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}

func TestLabelsOnNullOptionalBindingReturnsNull(t *testing.T) {
	executor := setupTestExecutor(t)

	result := executeBehaviorQuery(t, executor,
		"OPTIONAL MATCH (n:DoesNotExist) RETURN labels(n), labels(null)")

	require.Equal(t, [][]interface{}{{nil, nil}}, result.Rows)
}

func TestCollectCaseSkipsNullOptionalBinding(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (:Seed {id: 'seed'})")

	result := executeBehaviorQuery(t, executor, `
		MATCH (seed:Seed {id: 'seed'})
		OPTIONAL MATCH (seed)-[:RELATES_TO]->(n)
		RETURN collect(CASE WHEN n IS NULL THEN null ELSE {value: n.value} END) AS values`)

	require.Equal(t, [][]interface{}{{[]interface{}{}}}, result.Rows)
}

func TestUnwindCollectCaseSkipsNullOptionalBinding(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, `
		CREATE (first:Seed {id: 'first', alternate: 'first-alt'}),
		       (second:Seed {id: 'second', alternate: 'second-alt'}),
		       (target:Target {language: 'es', value: 'present'})
		CREATE (first)-[:RELATES_TO]->(target)`)

	result := executeBehaviorQuery(t, executor, `
		UNWIND ['first', 'second'] AS key
		MATCH (seed:Seed) WHERE seed.id = key OR seed.alternate = key
		OPTIONAL MATCH (seed)-[:RELATES_TO]->(n:Target {language: 'es'})
		RETURN key, collect(CASE WHEN n IS NULL THEN null ELSE {
			id: elementId(n),
			language: n.language,
			value: coalesce(n.value, elementId(n))
		} END) AS values`)

	require.Len(t, result.Rows, 2)
	rowsByKey := make(map[string][]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		rowsByKey[row[0].(string)] = row
	}
	require.Len(t, rowsByKey["first"][1], 1)
	require.Equal(t, []interface{}{}, rowsByKey["second"][1])
}
