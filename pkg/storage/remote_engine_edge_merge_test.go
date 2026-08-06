package storage

import (
	"context"
	"strings"
	"testing"
)

func TestRemoteEngineCreateEdgeMergesByID(t *testing.T) {
	var statement string
	var params map[string]interface{}
	engine := &RemoteEngine{
		transport: &fakeRemoteTransport{
			queryFn: func(_ context.Context, gotStatement string, gotParams map[string]interface{}) ([][]interface{}, error) {
				statement = gotStatement
				params = gotParams
				return [][]interface{}{{map[string]interface{}{"id": "merge-id"}}}, nil
			},
		},
	}

	edge := &Edge{
		ID:        "merge-id",
		Type:      "ASSERTS",
		StartNode: "source",
		EndNode:   "target",
		Properties: map[string]interface{}{
			"scope_id": "scope-a",
		},
	}
	if err := engine.CreateEdge(edge); err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}

	if !strings.Contains(statement, "MERGE (a)-[r:`ASSERTS` {id: $id}]->(b)") {
		t.Fatalf("expected edge ID to be part of remote MERGE identity, got %q", statement)
	}
	if strings.Contains(statement, " CREATE (a)-[r:") {
		t.Fatalf("remote edge create must not use a duplicate-prone CREATE: %q", statement)
	}
	if got := params["id"]; got != "merge-id" {
		t.Fatalf("expected merge ID parameter, got %#v", got)
	}
	props, ok := params["props"].(map[string]interface{})
	if !ok || props["id"] != "merge-id" || props["scope_id"] != "scope-a" {
		t.Fatalf("expected edge properties to retain the identity and mutable fields, got %#v", params["props"])
	}
}

func TestRemoteEngineCreateEdgeRejectsNilInput(t *testing.T) {
	engine := &RemoteEngine{transport: &fakeRemoteTransport{}}
	if err := engine.CreateEdge(nil); err != ErrInvalidData {
		t.Fatalf("expected ErrInvalidData for nil edge, got %v", err)
	}
}
