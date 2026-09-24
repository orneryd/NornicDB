package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLabelsRejectsStaticallyKnownPathArguments(t *testing.T) {
	err := validateGraphFunctionSemanticTypes("labels(path)", matchSemanticScope{"path": matchBindingPath})
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "InvalidArgumentType", semanticError.Detail)
}

func TestLabelsRejectsNonNodeValuesResolvedAtRuntime(t *testing.T) {
	executor := &StorageExecutor{}
	err := executor.validateRowGraphFunctionArguments("labels(items[1])", pipelineRow{
		"items": []interface{}{&storage.Node{}, int64(1)},
	})
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "InvalidArgumentValue", semanticError.Detail)
}

func TestTypeRejectsStaticallyKnownNodeArguments(t *testing.T) {
	err := validateGraphFunctionSemanticTypes("type(node)", matchSemanticScope{"node": matchBindingNode})
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "InvalidArgumentType", semanticError.Detail)
}

func TestTypeValidatesEveryListComprehensionValueAtRuntime(t *testing.T) {
	executor := &StorageExecutor{}
	err := executor.validateRowGraphFunctionArguments("[value IN items | type(value)]", pipelineRow{
		"items": []interface{}{&storage.Edge{Type: "T"}, int64(1)},
	})
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "InvalidArgumentValue", semanticError.Detail)
}

func TestPropertiesRejectsStaticallyIncompatibleArguments(t *testing.T) {
	for expression, message := range map[string]string{
		"properties(1)":             "Type mismatch: expected Map, Node or Relationship but was Integer",
		"properties('text')":        "Type mismatch: expected Map, Node or Relationship but was String",
		"properties([true, false])": "Type mismatch: expected Map, Node or Relationship but was List<Boolean>",
	} {
		err := validateStaticGraphFunctionArguments("RETURN " + expression)
		require.Error(t, err)
		var semanticError *SemanticError
		require.ErrorAs(t, err, &semanticError)
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
		require.Contains(t, err.Error(), message)
	}
	require.NoError(t, validateStaticGraphFunctionArguments("RETURN properties({name: 'Popeye'})"))
	require.NoError(t, validateStaticGraphFunctionArguments("RETURN properties(null)"))
}

// A graph function's literal argument is type-checked wherever it appears in
// the statement, with Neo4j's compile-time message (#580).
func TestStaticGraphFunctionArgumentsCheckedEverywhere(t *testing.T) {
	rejected := map[string]string{
		"MATCH (n) WHERE labels('x') = [] RETURN count(n)":                     "expected Node but was String",
		"MATCH (n) WHERE size(labels('x')) = 0 RETURN n":                       "expected Node but was String",
		"MATCH (n) WHERE type(1) = 'R' RETURN n":                               "expected Relationship but was Integer",
		"MATCH (n) WITH n WHERE labels('x') = [] RETURN n":                     "expected Node but was String",
		"MATCH (n) RETURN CASE WHEN labels('x') = [] THEN 1 ELSE 0 END AS c":   "expected Node but was String",
		"MATCH (n) WHERE exists { MATCH (n) WHERE labels([1]) = [] } RETURN n": "expected Node but was List<Integer>",
		"MATCH (n) SET n.x = keys(1)":                                          "expected Map, Node or Relationship but was Integer",
		"RETURN id({a: 1})":                                                    "expected Node or Relationship but was Map",
		"RETURN startNode(true), endNode(1)":                                   "expected Relationship but was Boolean",
		"RETURN length(1 + 1)":                                                 "expected Path but was Integer",
		"RETURN nodes('a' + 'b')":                                              "expected Path but was String",
		"RETURN relationships([[1]])":                                          "expected Path but was List<List<Integer>>",
		"RETURN elementId([])":                                                 "expected Node or Relationship but was List<T>",
		"RETURN labels([1, 2.5])":                                              "expected Node but was List<Float>, List<Integer> or List<Number>",
		"RETURN labels((1.5))":                                                 "expected Node but was Float",
		"UNWIND [1] AS i CALL { WITH i RETURN type('x') AS t } RETURN t":       "expected Relationship but was String",
	}
	for statement, message := range rejected {
		err := validateStaticGraphFunctionArguments(statement)
		require.Error(t, err, statement)
		require.Contains(t, err.Error(), "Type mismatch: "+message, statement)
	}
	for _, statement := range []string{
		"MATCH (n) WHERE labels(n) = ['T'] RETURN n",
		"RETURN labels(null), type(null), keys({a: 1}), properties({a: 1})",
		"RETURN 'labels(1)' AS s, n.labels AS p",
		"MATCH (n) RETURN labels($p), keys(n), length(p)",
		"RETURN apoc.labels(1), `labels`(n)",
		"RETURN [x IN [1] | labels(x)]",
	} {
		require.NoError(t, validateStaticGraphFunctionArguments(statement), statement)
	}
}

func TestPropertiesAcceptsMapsInSharedRowEvaluator(t *testing.T) {
	executor, _ := newUnitExecutor(t)
	value, evaluated := executor.evaluateRowExpression("properties({name: 'Popeye', level: 9001})", pipelineRow{})
	require.True(t, evaluated)
	require.Equal(t, map[string]interface{}{"name": "Popeye", "level": int64(9001)}, value)
}

func TestLengthRejectsGraphEntitiesAndAcceptsPaths(t *testing.T) {
	for variable, kind := range map[string]matchBindingKind{
		"node":         matchBindingNode,
		"relationship": matchBindingRelationship,
	} {
		err := validateGraphFunctionSemanticTypes("length("+variable+")", matchSemanticScope{variable: kind})
		require.Error(t, err)
		var semanticError *SemanticError
		require.ErrorAs(t, err, &semanticError)
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
	}
	require.NoError(t, validateGraphFunctionSemanticTypes("length(path)", matchSemanticScope{"path": matchBindingPath}))
}
