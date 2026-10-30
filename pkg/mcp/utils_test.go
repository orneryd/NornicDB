package mcp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// types.go utility functions
// ============================================================================

func TestIsValidTaskStatus(t *testing.T) {
	// Actual valid statuses: pending, active, completed, blocked
	valid := []string{"pending", "active", "completed", "blocked"}
	for _, s := range valid {
		assert.True(t, IsValidTaskStatus(s), "expected %q to be valid", s)
	}

	invalid := []string{"", "todo", "done", "in_progress", "COMPLETED", "unknown"}
	for _, s := range invalid {
		assert.False(t, IsValidTaskStatus(s), "expected %q to be invalid", s)
	}
}

func TestIsValidTaskPriority(t *testing.T) {
	valid := []string{"low", "medium", "high", "critical"}
	for _, s := range valid {
		assert.True(t, IsValidTaskPriority(s), "expected %q to be valid", s)
	}

	invalid := []string{"", "urgent", "normal", "HIGH", "unknown"}
	for _, s := range invalid {
		assert.False(t, IsValidTaskPriority(s), "expected %q to be invalid", s)
	}
}

func TestDefaultIfEmpty(t *testing.T) {
	assert.Equal(t, "fallback", DefaultIfEmpty("", "fallback"))
	assert.Equal(t, "value", DefaultIfEmpty("value", "fallback"))
	assert.Equal(t, "   ", DefaultIfEmpty("   ", "fallback")) // whitespace is not empty
}

func TestDefaultIntIfZero(t *testing.T) {
	assert.Equal(t, 10, DefaultIntIfZero(0, 10))
	assert.Equal(t, 5, DefaultIntIfZero(5, 10))
	assert.Equal(t, -1, DefaultIntIfZero(-1, 10)) // negative is not zero
}

func TestDefaultFloatIfZero(t *testing.T) {
	assert.Equal(t, 1.5, DefaultFloatIfZero(0.0, 1.5))
	assert.Equal(t, 0.5, DefaultFloatIfZero(0.5, 1.5))
	assert.Equal(t, -0.1, DefaultFloatIfZero(-0.1, 1.5))
}

// ============================================================================
// context.go
// ============================================================================

func TestContextWithDatabase(t *testing.T) {
	ctx := context.Background()

	// Before setting: should return empty string
	db := DatabaseFromContext(ctx)
	assert.Empty(t, db)

	// After setting: should return the set value
	ctx2 := ContextWithDatabase(ctx, "my-database")
	db2 := DatabaseFromContext(ctx2)
	assert.Equal(t, "my-database", db2)

	// Original context unchanged
	assert.Empty(t, DatabaseFromContext(ctx))
}

func TestContextWithDatabase_Overwrite(t *testing.T) {
	ctx := ContextWithDatabase(context.Background(), "db1")
	ctx2 := ContextWithDatabase(ctx, "db2")
	assert.Equal(t, "db2", DatabaseFromContext(ctx2))
	assert.Equal(t, "db1", DatabaseFromContext(ctx))
}

// ============================================================================
// tools.go utility functions
// ============================================================================

func TestAllTools(t *testing.T) {
	tools := AllTools()
	assert.NotEmpty(t, tools)
	for _, name := range tools {
		assert.NotEmpty(t, name, "tool name should not be empty")
	}
	// All known tools should be in the list
	for _, expected := range []string{ToolStore, ToolRecall, ToolDiscover, ToolLink, ToolTasks} {
		assert.Contains(t, tools, expected)
	}
}

func TestIsValidTool(t *testing.T) {
	// Known valid tool names
	validTools := []string{
		ToolStore, ToolRecall, ToolDiscover, ToolLink, ToolTasks,
	}
	for _, name := range validTools {
		assert.True(t, IsValidTool(name), "expected %q to be a valid tool", name)
	}

	// Invalid tool names
	invalidTools := []string{"", "unknown_tool", "STORE", "store_data"}
	for _, name := range invalidTools {
		assert.False(t, IsValidTool(name), "expected %q to be invalid tool", name)
	}
}

func TestInferOperation(t *testing.T) {
	// No args
	assert.Equal(t, "create", InferOperation(ToolStore, nil))
	assert.Equal(t, "read", InferOperation(ToolRecall, nil))
	assert.Equal(t, "read", InferOperation(ToolDiscover, nil))
	assert.Equal(t, "create", InferOperation(ToolLink, nil))
	assert.Equal(t, "read", InferOperation(ToolTasks, nil))
	assert.Equal(t, "unknown", InferOperation("unknown_tool", nil))

	// Tasks: create (no id, mutation args present)
	assert.Equal(t, "create", InferOperation(ToolTasks, map[string]interface{}{"title": "t"}))
	// Tasks: update (has id, no delete)
	assert.Equal(t, "update", InferOperation(ToolTasks, map[string]interface{}{"id": "123"}))
	// Tasks: delete (has id and delete=true)
	assert.Equal(t, "delete", InferOperation(ToolTasks, map[string]interface{}{"id": "123", "delete": true}))
	// Tasks: listing filters alone stay a read
	assert.Equal(t, "read", InferOperation(ToolTasks, map[string]interface{}{"status": []string{"pending"}}))
}

func TestExtractResourceType(t *testing.T) {
	// No args - defaults
	assert.Equal(t, "memory", ExtractResourceType(ToolStore, nil))
	assert.Equal(t, "*", ExtractResourceType(ToolRecall, nil))
	assert.Equal(t, "*", ExtractResourceType(ToolDiscover, nil))
	assert.Equal(t, "edge", ExtractResourceType(ToolLink, nil))
	assert.Equal(t, "task", ExtractResourceType(ToolTasks, nil))
	assert.Equal(t, "*", ExtractResourceType("unknown_tool", nil))

	// Store with explicit type
	assert.Equal(t, "document", ExtractResourceType(ToolStore, map[string]interface{}{"type": "document"}))

	// Recall/Discover with type array
	assert.Equal(t, "Person", ExtractResourceType(ToolRecall, map[string]interface{}{"type": []interface{}{"Person"}}))
}
