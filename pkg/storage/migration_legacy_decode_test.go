package storage

import (
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// legacyNodeSnapshot represents a Node as it was serialized before Phase 7.
// It includes DecayScore, LastAccessed, AccessCount which the new Node struct
// still has (until Step 2 removes them). This test verifies that msgpack
// silently ignores unknown keys when those fields are eventually removed.
type legacyNodeSnapshot struct {
	ID           NodeID                 `msgpack:"ID"`
	Labels       []string               `msgpack:"Labels"`
	Properties   map[string]interface{} `msgpack:"Properties"`
	CreatedAt    time.Time              `msgpack:"CreatedAt"`
	UpdatedAt    time.Time              `msgpack:"UpdatedAt"`
	DecayScore   float64                `msgpack:"DecayScore"`
	LastAccessed time.Time              `msgpack:"LastAccessed"`
	AccessCount  int64                  `msgpack:"AccessCount"`
}

func TestLegacyDecode_MsgpackIgnoresUnknownFields(t *testing.T) {
	// Simulate what happens when we decode old bytes that contain
	// DecayScore/LastAccessed/AccessCount into the new Node struct that no
	// longer has those fields. Msgpack should silently ignore unknown keys.
	legacy := legacyNodeSnapshot{
		ID:           "nornic:old",
		Labels:       []string{"Thing"},
		Properties:   map[string]interface{}{"name": "legacy"},
		CreatedAt:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		DecayScore:   0.67,
		LastAccessed: time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		AccessCount:  200,
	}

	data, err := msgpack.Marshal(&legacy)
	if err != nil {
		t.Fatal(err)
	}

	var node Node
	if err := msgpack.Unmarshal(data, &node); err != nil {
		t.Fatal(err)
	}

	// Verify that known fields decoded correctly and unknown fields were ignored.
	if string(node.ID) != "nornic:old" {
		t.Errorf("expected ID nornic:old, got %s", node.ID)
	}
	if len(node.Labels) != 1 || node.Labels[0] != "Thing" {
		t.Errorf("unexpected labels: %v", node.Labels)
	}
	if node.Properties["name"] != "legacy" {
		t.Errorf("expected Properties[name]=legacy, got %v", node.Properties["name"])
	}
}

func TestLegacyDecode_EncodeDecodeRoundTrip(t *testing.T) {
	// Encode a node through the storage engine, read it back,
	// verify core fields survive the roundtrip.
	eng := newTestEngine(t)

	node := &Node{
		ID:         "nornic:rt",
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"val": "check"},
	}
	if _, err := eng.CreateNode(node); err != nil {
		t.Fatal(err)
	}

	got, err := eng.GetNode("nornic:rt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.ID) != "nornic:rt" {
		t.Errorf("expected ID nornic:rt, got %s", got.ID)
	}
	if got.Properties["val"] != "check" {
		t.Errorf("expected Properties[val]=check, got %v", got.Properties["val"])
	}
}

func TestLegacyDecode_NewFieldsDefaultToZero(t *testing.T) {
	// Decode old bytes that don't have VisibilitySuppressed —
	// the field should default to false.
	legacy := legacyNodeSnapshot{
		ID:         "nornic:noflag",
		Labels:     []string{"Thing"},
		Properties: map[string]interface{}{},
	}

	data, err := msgpack.Marshal(&legacy)
	if err != nil {
		t.Fatal(err)
	}

	var node Node
	if err := msgpack.Unmarshal(data, &node); err != nil {
		t.Fatal(err)
	}

	if node.VisibilitySuppressed {
		t.Error("expected VisibilitySuppressed=false on old data")
	}
}
