package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFulltextIndex_RelationshipScope_RoundTrip asserts that a fulltext
// index declared on a relationship type round-trips through the JSON
// persistence layer with no on-disk schema-version bump. Two independent
// invariants are tested deeply:
//
//  1. Round-trip preserves every field.
//  2. The on-disk JSON for a node-only index does NOT contain the new
//     relationship_types key (omitempty), so an older binary opening
//     a database written by the new binary sees exactly the bytes it
//     would have written itself.
//
// The forward-compat case (new binary loading old JSON without the
// relationship_types field) is covered by TestFulltextIndex_LoadLegacy
// below.
func TestFulltextIndex_RelationshipScope_RoundTrip(t *testing.T) {
	def := &SchemaDefinition{
		FulltextIndexes: []FulltextIndex{
			{
				Name:              "rel_idx",
				RelationshipTypes: []string{"KNOWS", "WORKS_WITH"},
				Properties:        []string{"note", "since"},
			},
		},
	}
	encoded, err := json.Marshal(def)
	require.NoError(t, err)

	// Round-trip.
	var decoded SchemaDefinition
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.FulltextIndexes, 1)
	idx := decoded.FulltextIndexes[0]
	assert.Equal(t, "rel_idx", idx.Name)
	assert.Equal(t, []string{"KNOWS", "WORKS_WITH"}, idx.RelationshipTypes)
	assert.Equal(t, []string{"note", "since"}, idx.Properties)
	assert.Empty(t, idx.Labels, "relationship-scoped index must not carry labels")
}

// TestFulltextIndex_NodeOnly_OmitsRelationshipTypesField is the
// backwards-compatibility guarantee: existing databases written before
// this PR (and the corresponding new-binary writes for node-only
// indexes) must serialize without a relationship_types field. An older
// binary's json.Unmarshal silently ignores fields it doesn't know;
// preserving the old shape means no behavior change for old readers.
func TestFulltextIndex_NodeOnly_OmitsRelationshipTypesField(t *testing.T) {
	def := &SchemaDefinition{
		FulltextIndexes: []FulltextIndex{
			{
				Name:       "node_idx",
				Labels:     []string{"Memory"},
				Properties: []string{"name", "type"},
			},
		},
	}
	encoded, err := json.Marshal(def)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "relationship_types",
		"node-only index must NOT serialize a relationship_types field; doing so breaks old-binary read compatibility (got %s)", string(encoded))
	assert.Contains(t, string(encoded), `"labels":["Memory"]`)
}

// TestFulltextIndex_LoadLegacy is the forward-compatibility guarantee:
// JSON written by an older binary (no relationship_types key) must
// load cleanly into the new struct, with RelationshipTypes left as nil
// (which downstream code treats as "node-scoped index, fall back to
// label filtering").
func TestFulltextIndex_LoadLegacy(t *testing.T) {
	legacy := `{
		"fulltext_indexes": [
			{"name": "old_idx", "labels": ["Person"], "properties": ["name"]}
		]
	}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(legacy), &def))
	require.Len(t, def.FulltextIndexes, 1)
	idx := def.FulltextIndexes[0]
	assert.Equal(t, "old_idx", idx.Name)
	assert.Equal(t, []string{"Person"}, idx.Labels)
	assert.Empty(t, idx.RelationshipTypes,
		"missing relationship_types field must load as empty / nil")
	assert.Equal(t, []string{"name"}, idx.Properties)
}

// TestSchemaManager_AddFulltextRelationshipIndex_RoundTrip drives the
// full export/import cycle through SchemaManager so the persistence
// helpers are exercised end-to-end.
func TestSchemaManager_AddFulltextRelationshipIndex_RoundTrip(t *testing.T) {
	var persisted *SchemaDefinition
	sm := NewSchemaManager()
	sm.SetPersister(func(def *SchemaDefinition) error {
		// Take a deep copy via JSON so we observe exactly what would be
		// written to disk.
		raw, _ := json.Marshal(def)
		var clone SchemaDefinition
		_ = json.Unmarshal(raw, &clone)
		persisted = &clone
		return nil
	})

	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_search", []string{"KNOWS"}, []string{"note"}))

	// In-memory get returns the new fields.
	idx, ok := sm.GetFulltextIndex("rel_search")
	require.True(t, ok)
	assert.Equal(t, []string{"KNOWS"}, idx.RelationshipTypes)
	assert.Empty(t, idx.Labels)
	assert.Equal(t, []string{"note"}, idx.Properties)

	// Persistence saw the relationship-scope shape.
	require.NotNil(t, persisted, "persist callback must have fired")
	require.Len(t, persisted.FulltextIndexes, 1)
	assert.Equal(t, []string{"KNOWS"}, persisted.FulltextIndexes[0].RelationshipTypes)
	assert.Empty(t, persisted.FulltextIndexes[0].Labels)

	// Re-load the persisted def into a fresh manager — round-trip drops
	// nothing.
	sm2 := NewSchemaManager()
	require.NoError(t, sm2.ReplaceFromDefinition(persisted))
	idx2, ok := sm2.GetFulltextIndex("rel_search")
	require.True(t, ok)
	assert.Equal(t, []string{"KNOWS"}, idx2.RelationshipTypes)
	assert.Equal(t, []string{"note"}, idx2.Properties)
}

// TestFulltextIndex_BothScopesNeverMixed asserts the convention that
// a single index does not carry both labels and relationship_types.
// The runtime relies on this to decide which storage scan to drive;
// breaking it silently produces queries that traverse the wrong axis.
func TestFulltextIndex_BothScopesNeverMixed(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddFulltextIndex("node_idx", []string{"Person"}, []string{"name"}))
	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_idx", []string{"KNOWS"}, []string{"note"}))

	nodeIdx, _ := sm.GetFulltextIndex("node_idx")
	relIdx, _ := sm.GetFulltextIndex("rel_idx")

	require.NotEmpty(t, nodeIdx.Labels)
	require.Empty(t, nodeIdx.RelationshipTypes)

	require.Empty(t, relIdx.Labels)
	require.NotEmpty(t, relIdx.RelationshipTypes)
}

// TestFulltextIndex_LegacyTextRetainsExactShape uses a hand-written
// legacy JSON document and asserts the round-trip preserves byte
// equivalence for the node-only indexes (no relationship_types key
// added by the marshal step). This catches accidental field-default
// changes that would silently rewrite every existing schema document
// on next startup.
func TestFulltextIndex_LegacyTextRetainsExactShape(t *testing.T) {
	legacy := `{"fulltext_indexes":[{"name":"old_idx","labels":["Person"],"properties":["name"]}]}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(legacy), &def))

	out, err := json.Marshal(def)
	require.NoError(t, err)
	// The re-emitted JSON shouldn't contain relationship_types because
	// the index is node-scoped.
	assert.False(t, strings.Contains(string(out), "relationship_types"),
		"node-only index must round-trip without introducing relationship_types: got %s", string(out))
}

// =============================================================================
// Edge cases — added v1.1.2 to harden the FulltextIndex schema landing.
// =============================================================================

// TestFulltextIndex_LoadLegacyEmptyArrays — an older binary may serialize
// `labels: []` rather than omitting the field. The new struct must load
// it as a non-nil empty slice (treated identically to nil by every
// downstream consumer that does len(labels) == 0 checks).
func TestFulltextIndex_LoadLegacyEmptyArrays(t *testing.T) {
	legacy := `{
		"fulltext_indexes": [
			{"name": "x", "labels": [], "properties": ["name"]}
		]
	}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(legacy), &def))
	require.Len(t, def.FulltextIndexes, 1)
	idx := def.FulltextIndexes[0]
	assert.Equal(t, []string{"name"}, idx.Properties)
	// Either nil or empty slice is acceptable here — both signal the
	// same "no scope" semantics. The test pins that downstream consumers
	// can use the standard len() == 0 check.
	assert.Equal(t, 0, len(idx.Labels))
	assert.Equal(t, 0, len(idx.RelationshipTypes))
}

// TestFulltextIndex_LoadFutureFields — an older binary loading JSON
// written by a future binary that adds new top-level fields (e.g.
// analyzer settings, eventual_consistency, refresh_interval) must
// not crash. Go's encoding/json silently discards unknown fields,
// so this is a regression-safety pin against a future "let's switch
// to a stricter decoder" change.
func TestFulltextIndex_LoadFutureFields(t *testing.T) {
	future := `{
		"fulltext_indexes": [
			{
				"name": "future_idx",
				"labels": ["Memory"],
				"properties": ["name"],
				"analyzer": "english",
				"eventual_consistency": true,
				"refresh_interval_seconds": 5
			}
		]
	}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(future), &def))
	require.Len(t, def.FulltextIndexes, 1)
	idx := def.FulltextIndexes[0]
	assert.Equal(t, "future_idx", idx.Name)
	assert.Equal(t, []string{"Memory"}, idx.Labels)
	assert.Equal(t, []string{"name"}, idx.Properties)
}

// TestFulltextIndex_LoadFutureFields_OnFulltextIndexLevel — older binaries
// loading JSON with future fields INSIDE a single FulltextIndex element
// (case-sensitivity test, score boost, etc.) must also load cleanly.
func TestFulltextIndex_LoadFutureFields_OnFulltextIndexLevel(t *testing.T) {
	future := `{
		"fulltext_indexes": [
			{
				"name": "n",
				"labels": ["L"],
				"properties": ["p"],
				"score_boost": 2.5,
				"future_array": [1, 2, 3],
				"future_obj": {"a": 1}
			}
		]
	}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(future), &def))
	require.Len(t, def.FulltextIndexes, 1)
	assert.Equal(t, "n", def.FulltextIndexes[0].Name)
}

// TestFulltextIndex_LoadMalformedJSON — unrelated corruption (truncated
// JSON, embedded null bytes) returns a clean unmarshal error rather
// than panicking. Pins SchemaDefinition's resilience against on-disk
// corruption in the schema persistence file.
func TestFulltextIndex_LoadMalformedJSON(t *testing.T) {
	for name, raw := range map[string]string{
		"truncated":     `{"fulltext_indexes": [{"name": "n",`,
		"empty_string":  ``,
		"wrong_type":    `{"fulltext_indexes": "not an array"}`,
		"only_brace":    `{`,
		"trailing_junk": `{"fulltext_indexes": []}garbage`,
	} {
		t.Run(name, func(t *testing.T) {
			var def SchemaDefinition
			err := json.Unmarshal([]byte(raw), &def)
			assert.Error(t, err, "%s should fail to unmarshal", name)
		})
	}
}

// TestSchemaManager_AddFulltextRelationshipIndex_PersistError pins the
// rollback contract: when the persist callback fails, the in-memory map
// must NOT carry the half-added entry. Older binaries reading a
// half-written persist file would otherwise see an index that the new
// binary treats as nonexistent.
func TestSchemaManager_AddFulltextRelationshipIndex_PersistError(t *testing.T) {
	sm := NewSchemaManager()
	sentinel := errors.New("disk full")
	sm.SetPersister(func(def *SchemaDefinition) error {
		return sentinel
	})

	err := sm.AddFulltextRelationshipIndex("rel_idx", []string{"KNOWS"}, []string{"note"})
	require.ErrorIs(t, err, sentinel,
		"persist failure must surface to caller verbatim")

	_, exists := sm.GetFulltextIndex("rel_idx")
	assert.False(t, exists,
		"persist failure must roll back the in-memory entry; otherwise the in-memory and on-disk states diverge")
}

// TestSchemaManager_AddFulltextRelationshipIndex_DuplicateName pins the
// idempotent-add contract. The legacy AddFulltextIndex returns nil for
// a duplicate (no-op); AddFulltextRelationshipIndex must do the same so
// admin-API retries don't surface spurious errors.
func TestSchemaManager_AddFulltextRelationshipIndex_DuplicateName(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_idx", []string{"KNOWS"}, []string{"note"}))
	// Second call with different scope is a no-op (preserves first call's data).
	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_idx", []string{"DIFFERENT"}, []string{"different"}))

	idx, ok := sm.GetFulltextIndex("rel_idx")
	require.True(t, ok)
	assert.Equal(t, []string{"KNOWS"}, idx.RelationshipTypes,
		"duplicate AddFulltextRelationshipIndex must NOT overwrite the first call's scope")
	assert.Equal(t, []string{"note"}, idx.Properties)
}

// TestSchemaManager_AddFulltextRelationshipIndex_NameCollidesWithNodeIndex —
// node and relationship fulltext indexes share the same `fulltextIndexes`
// map, so a name collision between the two (e.g., admin creates a node
// index `search`, then later wants a relationship index also named
// `search`) returns no-op rather than corrupting the node index.
func TestSchemaManager_AddFulltextRelationshipIndex_NameCollidesWithNodeIndex(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.AddFulltextIndex("search", []string{"Memory"}, []string{"name"}))
	// Try to add a relationship index with the same name — must be a no-op.
	require.NoError(t, sm.AddFulltextRelationshipIndex("search", []string{"KNOWS"}, []string{"note"}))

	idx, ok := sm.GetFulltextIndex("search")
	require.True(t, ok)
	assert.Equal(t, []string{"Memory"}, idx.Labels,
		"name collision must preserve original (node) scope")
	assert.Empty(t, idx.RelationshipTypes,
		"name collision must NOT inject relationship-type scope into the existing node index")
}

// TestFulltextIndex_DropIndex_PreservesSiblings — DropIndex on a
// relationship index must not disturb a sibling node index of the same
// SchemaManager. The two share `fulltextIndexes` so a buggy delete
// could in theory wipe both.
func TestFulltextIndex_DropIndex_PreservesSiblings(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddFulltextIndex("node_idx", []string{"Memory"}, []string{"name"}))
	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_idx", []string{"KNOWS"}, []string{"note"}))

	require.NoError(t, sm.DropIndex("rel_idx"))

	_, relExists := sm.GetFulltextIndex("rel_idx")
	assert.False(t, relExists, "rel_idx must be gone after DropIndex")

	nodeIdx, nodeExists := sm.GetFulltextIndex("node_idx")
	require.True(t, nodeExists, "node_idx must survive DropIndex on rel_idx")
	assert.Equal(t, []string{"Memory"}, nodeIdx.Labels)
	assert.Equal(t, []string{"name"}, nodeIdx.Properties)
}

// TestFulltextIndex_DropIndex_DropNodePreservesRel — symmetric of the
// previous test: dropping the node-side index must leave the
// relationship-side intact.
func TestFulltextIndex_DropIndex_DropNodePreservesRel(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddFulltextIndex("node_idx", []string{"Memory"}, []string{"name"}))
	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_idx", []string{"KNOWS"}, []string{"note"}))

	require.NoError(t, sm.DropIndex("node_idx"))

	_, nodeExists := sm.GetFulltextIndex("node_idx")
	assert.False(t, nodeExists)

	relIdx, relExists := sm.GetFulltextIndex("rel_idx")
	require.True(t, relExists)
	assert.Equal(t, []string{"KNOWS"}, relIdx.RelationshipTypes)
	assert.Empty(t, relIdx.Labels)
}

// TestFulltextIndex_LoadLegacy_RespectsSecondaryFields — older binaries
// reading new JSON must accept extra fields silently. Inverse: new
// binaries reading older JSON that lacks the relationship_types field
// MUST treat it as nil (an empty/missing array). This pins the
// "missing field is identical to empty slice" contract every consumer
// relies on.
func TestFulltextIndex_LoadLegacy_RespectsSecondaryFields(t *testing.T) {
	old := `{
		"fulltext_indexes": [
			{"name": "n1", "labels": ["A"], "properties": ["x"]},
			{"name": "n2", "labels": ["B", "C"], "properties": ["y", "z"]}
		]
	}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(old), &def))
	require.Len(t, def.FulltextIndexes, 2)

	for i, idx := range def.FulltextIndexes {
		assert.Empty(t, idx.RelationshipTypes,
			"index %d (%s): legacy JSON without relationship_types must load empty", i, idx.Name)
	}
}

// TestFulltextIndex_RoundTripIdempotent — Add → ExportDefinition →
// ReplaceFromDefinition → ExportDefinition must be byte-identical at
// the JSON level (modulo map ordering, which Go normalizes for slices
// of structs that we sort during export). Catches accidental encoding
// drift introduced by future struct changes.
func TestFulltextIndex_RoundTripIdempotent(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddFulltextIndex("node_idx", []string{"A", "B"}, []string{"p", "q"}))
	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_idx", []string{"R1", "R2"}, []string{"x"}))

	def1 := sm.ExportDefinition()
	bytes1, err := json.Marshal(def1)
	require.NoError(t, err)

	sm2 := NewSchemaManager()
	require.NoError(t, sm2.ReplaceFromDefinition(def1))

	def2 := sm2.ExportDefinition()
	bytes2, err := json.Marshal(def2)
	require.NoError(t, err)

	assert.JSONEq(t, string(bytes1), string(bytes2),
		"export → import → export must be JSON-equal; otherwise a noisy upgrade rewrites every persisted schema document")
}

// TestFulltextIndex_LargeScopeList — pin reasonable behavior at the
// upper bound: a fulltext index with thousands of declared labels or
// relationship types still serializes and deserializes cleanly. No
// fixed size limit is documented, so this just guards against an
// O(n²) regression during validation.
func TestFulltextIndex_LargeScopeList(t *testing.T) {
	largeLabels := make([]string, 1000)
	for i := range largeLabels {
		largeLabels[i] = fmt.Sprintf("Label%d", i)
	}
	largeProps := make([]string, 1000)
	for i := range largeProps {
		largeProps[i] = fmt.Sprintf("prop%d", i)
	}

	def := &SchemaDefinition{
		FulltextIndexes: []FulltextIndex{
			{Name: "big", Labels: largeLabels, Properties: largeProps},
		},
	}
	encoded, err := json.Marshal(def)
	require.NoError(t, err)

	var decoded SchemaDefinition
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.FulltextIndexes, 1)
	assert.Len(t, decoded.FulltextIndexes[0].Labels, 1000)
	assert.Len(t, decoded.FulltextIndexes[0].Properties, 1000)
}
