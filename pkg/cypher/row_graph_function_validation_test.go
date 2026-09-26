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
	for _, expression := range []string{"properties(1)", "properties('text')", "properties([true, false])"} {
		err := validateGraphFunctionSemanticTypes(expression, nil)
		require.Error(t, err)
		var semanticError *SemanticError
		require.ErrorAs(t, err, &semanticError)
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
	}
	require.NoError(t, validateGraphFunctionSemanticTypes("properties({name: 'Popeye'})", nil))
	require.NoError(t, validateGraphFunctionSemanticTypes("properties(null)", nil))
}

func TestPropertiesAcceptsMapsInSharedRowEvaluator(t *testing.T) {
	executor, _ := newUnitExecutor(t)
	value, evaluated := rowValue(t, executor, "properties({name: 'Popeye', level: 9001})", pipelineRow{})
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
