package mcp

import (
	"encoding/json"
	"testing"
)

func TestGetToolDefinitions(t *testing.T) {
	tools := GetToolDefinitions()

	if len(tools) != 8 {
		t.Errorf("Expected 8 tools, got %d", len(tools))
	}

	// Check all tools are present
	toolNames := map[string]bool{
		ToolStore:    false,
		ToolRecall:   false,
		ToolDiscover: false,
		ToolLink:     false,
		ToolIndex:    false,
		ToolUnindex:  false,
		ToolTask:     false,
		ToolTasks:    false,
	}

	for _, tool := range tools {
		if _, exists := toolNames[tool.Name]; exists {
			toolNames[tool.Name] = true
		} else {
			t.Errorf("Unexpected tool: %s", tool.Name)
		}
	}

	for name, found := range toolNames {
		if !found {
			t.Errorf("Missing tool: %s", name)
		}
	}
}

func TestToolConstants(t *testing.T) {
	// Verify tool name constants
	if ToolStore != "store" {
		t.Error("ToolStore should be 'store'")
	}
	if ToolRecall != "recall" {
		t.Error("ToolRecall should be 'recall'")
	}
	if ToolDiscover != "discover" {
		t.Error("ToolDiscover should be 'discover'")
	}
	if ToolLink != "link" {
		t.Error("ToolLink should be 'link'")
	}
	if ToolIndex != "index" {
		t.Error("ToolIndex should be 'index'")
	}
	if ToolUnindex != "unindex" {
		t.Error("ToolUnindex should be 'unindex'")
	}
	if ToolTask != "task" {
		t.Error("ToolTask should be 'task'")
	}
	if ToolTasks != "tasks" {
		t.Error("ToolTasks should be 'tasks'")
	}
}

func TestToolPermissions(t *testing.T) {
	// Verify ToolPermissions map is populated
	if len(ToolPermissions) != 8 {
		t.Errorf("Expected 8 tool permissions, got %d", len(ToolPermissions))
	}

	// Check each tool has a permission
	tools := []string{ToolStore, ToolRecall, ToolDiscover, ToolLink, ToolIndex, ToolUnindex, ToolTask, ToolTasks}
	for _, tool := range tools {
		if _, exists := ToolPermissions[tool]; !exists {
			t.Errorf("Missing permission for tool: %s", tool)
		}
	}
}

func TestToolDefinitionStructure(t *testing.T) {
	tools := GetToolDefinitions()

	for _, tool := range tools {
		// Each tool should have name and description
		if tool.Name == "" {
			t.Error("Tool has empty name")
		}
		if tool.Description == "" {
			t.Errorf("Tool %s has empty description", tool.Name)
		}
		if len(tool.InputSchema) == 0 {
			t.Errorf("Tool %s has empty InputSchema", tool.Name)
		}

		// Parse schema to verify structure
		var schema map[string]interface{}
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil {
			t.Errorf("Tool %s has invalid JSON schema: %v", tool.Name, err)
			continue
		}

		if schema["type"] != "object" {
			t.Errorf("Tool %s schema type should be 'object'", tool.Name)
		}
		if _, hasProps := schema["properties"]; !hasProps {
			t.Errorf("Tool %s schema missing 'properties'", tool.Name)
		}
	}
}

func TestStoreToolSchema(t *testing.T) {
	tools := GetToolDefinitions()
	var storeTool *Tool
	for i, tool := range tools {
		if tool.Name == ToolStore {
			storeTool = &tools[i]
			break
		}
	}

	if storeTool == nil {
		t.Fatal("store tool not found")
	}

	var schema map[string]interface{}
	if err := json.Unmarshal(storeTool.InputSchema, &schema); err != nil {
		t.Fatalf("Invalid schema: %v", err)
	}

	// Check required fields
	required, ok := schema["required"].([]interface{})
	if !ok {
		t.Fatal("required field should be array")
	}

	hasContent := false
	for _, r := range required {
		if r == "content" {
			hasContent = true
			break
		}
	}
	if !hasContent {
		t.Error("store tool should require 'content'")
	}
}

func TestLinkToolSchema(t *testing.T) {
	tools := GetToolDefinitions()
	var linkTool *Tool
	for i, tool := range tools {
		if tool.Name == ToolLink {
			linkTool = &tools[i]
			break
		}
	}

	if linkTool == nil {
		t.Fatal("link tool not found")
	}

	var schema map[string]interface{}
	if err := json.Unmarshal(linkTool.InputSchema, &schema); err != nil {
		t.Fatalf("Invalid schema: %v", err)
	}

	// Check required fields
	required, ok := schema["required"].([]interface{})
	if !ok {
		t.Fatal("required field should be array")
	}

	// Link tool should require from, to, and relation
	requiredFields := map[string]bool{"from": false, "to": false, "relation": false}
	for _, r := range required {
		if str, ok := r.(string); ok {
			requiredFields[str] = true
		}
	}

	for field, found := range requiredFields {
		if !found {
			t.Errorf("link tool should require '%s'", field)
		}
	}
}

func TestIsValidRelation(t *testing.T) {
	// Valid relations (from types.go ValidRelations)
	validRelations := []string{
		"depends_on", "relates_to", "implements", "caused_by",
		"blocks", "contains", "references", "uses",
		"evolved_from", "contradicts",
	}

	for _, rel := range validRelations {
		if !IsValidRelation(rel) {
			t.Errorf("Expected %s to be valid", rel)
		}
	}

	// Invalid relations
	if IsValidRelation("invalid_relation") {
		t.Error("Expected 'invalid_relation' to be invalid")
	}
	if IsValidRelation("") {
		t.Error("Expected empty string to be invalid")
	}
}

func TestValidRelations(t *testing.T) {
	if len(ValidRelations) != 10 {
		t.Errorf("Expected 10 valid relations, got %d", len(ValidRelations))
	}
}
