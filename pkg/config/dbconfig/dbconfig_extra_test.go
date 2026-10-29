package dbconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// dbNameFromNodeID
// ============================================================================

func TestDbNameFromNodeID_WithPrefix(t *testing.T) {
	result := dbNameFromNodeID(dbConfigPrefix + "mydb")
	assert.Equal(t, "mydb", result)
}

func TestDbNameFromNodeID_WithoutPrefix(t *testing.T) {
	result := dbNameFromNodeID("nornic:node-xyz")
	assert.Equal(t, "", result)
}

func TestDbNameFromNodeID_Empty(t *testing.T) {
	result := dbNameFromNodeID("")
	assert.Equal(t, "", result)
}

// ============================================================================
// IsAllowedKey
// ============================================================================

func TestIsAllowedKey_Allowed(t *testing.T) {
	// NORNICDB_EMBEDDING_DIMENSIONS and NORNICDB_SEARCH_MIN_SIMILARITY are known allowed keys
	assert.True(t, IsAllowedKey("NORNICDB_EMBEDDING_DIMENSIONS"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_MIN_SIMILARITY"))
}

func TestIsAllowedKey_NotAllowed(t *testing.T) {
	assert.False(t, IsAllowedKey("RANDOM_UNKNOWN_KEY"))
	assert.False(t, IsAllowedKey(""))
}

// ============================================================================
// overridesFromProperties
// ============================================================================

func TestOverridesFromProperties_Nil(t *testing.T) {
	result := overridesFromProperties(nil)
	assert.Nil(t, result)
}

func TestOverridesFromProperties_NoOverridesKey(t *testing.T) {
	result := overridesFromProperties(map[string]any{"other": "val"})
	assert.Nil(t, result)
}

func TestOverridesFromProperties_WrongType(t *testing.T) {
	result := overridesFromProperties(map[string]any{"overrides": 42})
	assert.Nil(t, result)
}

func TestOverridesFromProperties_InvalidJSON(t *testing.T) {
	result := overridesFromProperties(map[string]any{"overrides": "{not-json"})
	assert.Nil(t, result)
}

func TestIsAllowedKey_Excluded(t *testing.T) {
	KeysExcludedFromPerDB["NORNICDB_EMBEDDING_MODEL"] = true
	t.Cleanup(func() {
		delete(KeysExcludedFromPerDB, "NORNICDB_EMBEDDING_MODEL")
	})

	assert.False(t, IsAllowedKey("NORNICDB_EMBEDDING_MODEL"))
}
