package mcp

import (
	"encoding/json"
	"testing"
)

func TestNodeSerialization(t *testing.T) {
	node := Node{
		ID:      "node-123",
		Type:    "Memory",
		Title:   "Test Node",
		Content: "Test content",
		Properties: map[string]interface{}{
			"key": "value",
		},
	}

	// Test JSON serialization
	data, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	var decoded Node
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal node: %v", err)
	}

	if decoded.ID != node.ID {
		t.Errorf("ID mismatch: got %s, want %s", decoded.ID, node.ID)
	}
	if decoded.Type != node.Type {
		t.Errorf("Type mismatch: got %s, want %s", decoded.Type, node.Type)
	}
}

func TestStoreResultSerialization(t *testing.T) {
	result := StoreResult{
		ID:       "node-123",
		Title:    "Test",
		Embedded: true,
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded StoreResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if !decoded.Embedded {
		t.Error("Embedded should be true")
	}
}

func TestRecallResultSerialization(t *testing.T) {
	result := RecallResult{
		Nodes: []Node{
			{ID: "node-1", Type: "Memory"},
			{ID: "node-2", Type: "Task"},
		},
		Count: 2,
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded RecallResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Count != 2 {
		t.Errorf("Count mismatch: got %d, want 2", decoded.Count)
	}
	if len(decoded.Nodes) != 2 {
		t.Errorf("Nodes length mismatch: got %d, want 2", len(decoded.Nodes))
	}
}

func TestDiscoverResultSerialization(t *testing.T) {
	result := DiscoverResult{
		Results: []SearchResult{
			{ID: "r-1", Title: "Result 1", Similarity: 0.95},
		},
		Method: "vector",
		Total:  1,
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded DiscoverResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Method != "vector" {
		t.Errorf("Method mismatch: got %s, want vector", decoded.Method)
	}
}

func TestSearchResultSerialization(t *testing.T) {
	result := SearchResult{
		ID:             "sr-1",
		Type:           "Memory",
		Title:          "Test Result",
		ContentPreview: "Preview...",
		Similarity:     0.85,
		Properties: map[string]interface{}{
			"source": "test",
		},
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded SearchResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Similarity != 0.85 {
		t.Errorf("Similarity mismatch: got %f, want 0.85", decoded.Similarity)
	}
}

func TestLinkResultSerialization(t *testing.T) {
	result := LinkResult{
		EdgeID: "edge-123",
		From:   Node{ID: "from-1", Type: "Memory"},
		To:     Node{ID: "to-1", Type: "Task"},
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded LinkResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.EdgeID != "edge-123" {
		t.Errorf("EdgeID mismatch: got %s, want edge-123", decoded.EdgeID)
	}
}

func TestIndexResultSerialization(t *testing.T) {
	result := IndexResult{
		WatchID:     "watch-123",
		FilesQueued: 42,
		Status:      "indexing",
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded IndexResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.FilesQueued != 42 {
		t.Errorf("FilesQueued mismatch: got %d, want 42", decoded.FilesQueued)
	}
}

func TestUnindexResultSerialization(t *testing.T) {
	result := UnindexResult{
		RemovedFiles:  10,
		RemovedChunks: 50,
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded UnindexResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.RemovedFiles != 10 {
		t.Errorf("RemovedFiles mismatch: got %d, want 10", decoded.RemovedFiles)
	}
}

func TestTaskResultSerialization(t *testing.T) {
	result := TaskResult{
		Task: Node{
			ID:    "task-123",
			Type:  "Task",
			Title: "Test Task",
		},
		NextAction: "Review",
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded TaskResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Task.ID != "task-123" {
		t.Errorf("Task ID mismatch: got %s, want task-123", decoded.Task.ID)
	}
}

func TestTasksResultSerialization(t *testing.T) {
	result := TasksResult{
		Tasks: []Node{
			{ID: "task-1"},
			{ID: "task-2"},
		},
		Stats: TaskStats{
			Total:      2,
			ByStatus:   map[string]int{"pending": 1, "active": 1},
			ByPriority: map[string]int{"high": 1, "medium": 1},
		},
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded TasksResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Stats.Total != 2 {
		t.Errorf("Total mismatch: got %d, want 2", decoded.Stats.Total)
	}
}

func TestMCPRequestResponseTypes(t *testing.T) {
	// Test InitRequest
	initReq := InitRequest{
		ProtocolVersion: "2024-11-05",
		ClientInfo: ClientInfo{
			Name:    "test-client",
			Version: "1.0",
		},
	}
	_, err := json.Marshal(initReq)
	if err != nil {
		t.Errorf("InitRequest marshal failed: %v", err)
	}

	// Test InitResponse
	initResp := InitResponse{
		ProtocolVersion: "2024-11-05",
		ServerInfo: ServerInfo{
			Name:    "test",
			Version: "1.0",
		},
	}
	_, err = json.Marshal(initResp)
	if err != nil {
		t.Errorf("InitResponse marshal failed: %v", err)
	}

	// Test ListToolsResponse
	listResp := ListToolsResponse{
		Tools: GetToolDefinitions(),
	}
	_, err = json.Marshal(listResp)
	if err != nil {
		t.Errorf("ListToolsResponse marshal failed: %v", err)
	}

	// Test CallToolRequest
	callReq := CallToolRequest{
		Name:      "store",
		Arguments: map[string]interface{}{"content": "test"},
	}
	_, err = json.Marshal(callReq)
	if err != nil {
		t.Errorf("CallToolRequest marshal failed: %v", err)
	}

	// Test CallToolResponse
	callResp := CallToolResponse{
		Content: []Content{{Type: "text", Text: "result"}},
		IsError: false,
	}
	_, err = json.Marshal(callResp)
	if err != nil {
		t.Errorf("CallToolResponse marshal failed: %v", err)
	}
}

func TestContentType(t *testing.T) {
	content := Content{
		Type: "text",
		Text: "Hello, World!",
	}

	data, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded Content
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Type != "text" {
		t.Errorf("Type mismatch: got %s, want text", decoded.Type)
	}
	if decoded.Text != "Hello, World!" {
		t.Errorf("Text mismatch")
	}
}
