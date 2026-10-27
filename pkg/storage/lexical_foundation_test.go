package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConstraintLexicalFoundation(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	node := &Node{ID: "test:lexical", Labels: []string{"Person"}, Properties: map[string]any{"kind": "a;b"}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	for _, expression := range []string{
		"COUNT\u00a0{\u00a0(n)-[:OWNS]->(:Item)\u00a0}\u00a0<=\u00a00",
		"NOT/*gap*/EXISTS\u00a0{ (n)-[:OWNS]->() }",
		"n.kind\u00a0IN\u00a0['a;b']",
	} {
		value, err := evaluateNodeConstraintContractExpressionEngine(engine, node, expression)
		require.NoError(t, err)
		require.True(t, value)
	}
	name, end := ccScanIdent("`café key`\u00a0tail", 0)
	require.Equal(t, "café key", name)
	require.Equal(t, len("`café key`"), end)
}
