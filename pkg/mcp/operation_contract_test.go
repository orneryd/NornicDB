package mcp

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestEveryMCPToolHasExpectedEffect(t *testing.T) {
	expected := map[string]cypher.PrivilegeClass{
		ToolStore:    cypher.PrivilegeRead | cypher.PrivilegeWrite,
		ToolRecall:   cypher.PrivilegeRead,
		ToolDiscover: cypher.PrivilegeRead,
		ToolLink:     cypher.PrivilegeRead | cypher.PrivilegeWrite,
		ToolTasks:    cypher.PrivilegeRead,
	}
	require.Equal(t, expected, toolDatabaseAccessByName)
	definitions := GetToolDefinitions()
	require.Len(t, definitions, len(expected))
	for _, definition := range definitions {
		require.True(t, expected[definition.Name].Valid(), definition.Name)
	}
}
