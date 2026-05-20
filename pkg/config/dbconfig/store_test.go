package dbconfig

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Load_Get_Set(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	err := store.Load(ctx)
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides("mydb"))

	// Set overrides
	err = store.SetOverrides(ctx, "mydb", map[string]string{
		"NORNICDB_EMBEDDING_MODEL":       "bge-m3",
		"NORNICDB_SEARCH_MIN_SIMILARITY": "0.7",
	})
	require.NoError(t, err)
	o := store.GetOverrides("mydb")
	require.NotNil(t, o)
	assert.Equal(t, "bge-m3", o["NORNICDB_EMBEDDING_MODEL"])
	assert.Equal(t, "0.7", o["NORNICDB_SEARCH_MIN_SIMILARITY"])

	// Reload from storage (simulate restart)
	store2 := NewStore(eng)
	err = store2.Load(ctx)
	require.NoError(t, err)
	o2 := store2.GetOverrides("mydb")
	require.NotNil(t, o2)
	assert.Equal(t, "bge-m3", o2["NORNICDB_EMBEDDING_MODEL"])
}

func TestStore_SetOverrides_EmptyClears(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	err := store.SetOverrides(ctx, "mydb", map[string]string{"K": "v"})
	require.NoError(t, err)
	assert.NotNil(t, store.GetOverrides("mydb"))

	err = store.SetOverrides(ctx, "mydb", nil)
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides("mydb"))

	err = store.SetOverrides(ctx, "mydb", map[string]string{})
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides("mydb"))
}

func TestStore_EmptyDbNameIgnored(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)
	err := store.SetOverrides(ctx, "", map[string]string{"K": "v"})
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides(""))
}

func TestStore_LoadFiltersAndParsesConfigNodes(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()

	_, err := eng.CreateNode(&storage.Node{
		ID:     "db_config:alpha",
		Labels: []string{"_DbConfig", "_System"},
		Properties: map[string]any{
			"overrides": `{"NORNICDB_EMBEDDING_MODEL":"bge-m3"}`,
		},
	})
	require.NoError(t, err)

	_, err = eng.CreateNode(&storage.Node{
		ID:     "db_config:broken",
		Labels: []string{"_DbConfig"},
		Properties: map[string]any{
			"overrides": `{not-json`,
		},
	})
	require.NoError(t, err)

	_, err = eng.CreateNode(&storage.Node{
		ID:     "db_config:no-label",
		Labels: []string{"Other"},
		Properties: map[string]any{
			"overrides": `{"IGNORED":"true"}`,
		},
	})
	require.NoError(t, err)

	store := NewStore(eng)
	require.NoError(t, store.Load(ctx))
	assert.Equal(t, map[string]string{"NORNICDB_EMBEDDING_MODEL": "bge-m3"}, store.GetOverrides("alpha"))
	assert.Nil(t, store.GetOverrides("broken"))
	assert.Nil(t, store.GetOverrides("no-label"))
}

func TestStore_SetOverrides_UpdateAndTrimmedDbName(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	require.NoError(t, store.SetOverrides(ctx, "  mydb  ", map[string]string{"K": "v1"}))
	first, err := eng.GetNode(storage.NodeID("db_config:mydb"))
	require.NoError(t, err)

	require.NoError(t, store.SetOverrides(ctx, "mydb", map[string]string{"K": "v2"}))
	second, err := eng.GetNode(storage.NodeID("db_config:mydb"))
	require.NoError(t, err)

	assert.Equal(t, first.CreatedAt, second.CreatedAt)
	assert.Equal(t, map[string]string{"K": "v2"}, store.GetOverrides("mydb"))
}

// TestLoadWithYAMLDefaults_SeedsOnFirstBoot — yaml-declared per-DB overrides
// land in the store on first boot when no row exists for that database.
func TestLoadWithYAMLDefaults_SeedsOnFirstBoot(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	yamlOverrides := map[string]map[string]string{
		"analytics": {
			"NORNICDB_SEARCH_BM25_ENABLED":   "false",
			"NORNICDB_SEARCH_VECTOR_WARMING": "lazy",
		},
	}
	require.NoError(t, store.LoadWithYAMLDefaults(ctx, yamlOverrides))

	got := store.GetOverrides("analytics")
	require.NotNil(t, got)
	assert.Equal(t, "false", got["NORNICDB_SEARCH_BM25_ENABLED"])
	assert.Equal(t, "lazy", got["NORNICDB_SEARCH_VECTOR_WARMING"])

	// Reload — values must persist across "restart".
	store2 := NewStore(eng)
	require.NoError(t, store2.Load(ctx))
	got2 := store2.GetOverrides("analytics")
	require.NotNil(t, got2)
	assert.Equal(t, "false", got2["NORNICDB_SEARCH_BM25_ENABLED"])
}

// TestLoadWithYAMLDefaults_DoesNotClobberAdminEdits — once an admin has set
// a value via SetOverrides, a subsequent LoadWithYAMLDefaults call (e.g. on
// the next boot) MUST NOT overwrite it. Yaml is a one-time seed; admin
// edits are authoritative across restarts.
func TestLoadWithYAMLDefaults_DoesNotClobberAdminEdits(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()

	// Boot 1: yaml seeds initial values.
	store1 := NewStore(eng)
	require.NoError(t, store1.LoadWithYAMLDefaults(ctx, map[string]map[string]string{
		"analytics": {"NORNICDB_SEARCH_BM25_ENABLED": "false"},
	}))
	got := store1.GetOverrides("analytics")
	assert.Equal(t, "false", got["NORNICDB_SEARCH_BM25_ENABLED"])

	// Admin flips the flag back via the admin API.
	require.NoError(t, store1.SetOverrides(ctx, "analytics", map[string]string{
		"NORNICDB_SEARCH_BM25_ENABLED": "true",
	}))

	// Boot 2: same yaml. Admin's true must survive.
	store2 := NewStore(eng)
	require.NoError(t, store2.LoadWithYAMLDefaults(ctx, map[string]map[string]string{
		"analytics": {"NORNICDB_SEARCH_BM25_ENABLED": "false"},
	}))
	got2 := store2.GetOverrides("analytics")
	assert.Equal(t, "true", got2["NORNICDB_SEARCH_BM25_ENABLED"],
		"admin-API edit must survive subsequent yaml-default load")
}

// TestLoadWithYAMLDefaults_FillsMissingKeysOnly — yaml can supply a key the
// admin has never set, even when other keys for the same DB are stored.
func TestLoadWithYAMLDefaults_FillsMissingKeysOnly(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	// Admin pre-set the BM25 flag.
	require.NoError(t, store.SetOverrides(ctx, "analytics", map[string]string{
		"NORNICDB_SEARCH_BM25_ENABLED": "true",
	}))

	// Yaml introduces a NEW key.
	require.NoError(t, store.LoadWithYAMLDefaults(ctx, map[string]map[string]string{
		"analytics": {
			"NORNICDB_SEARCH_BM25_ENABLED":   "false", // already set, must not clobber
			"NORNICDB_SEARCH_VECTOR_WARMING": "lazy",  // new, gets seeded
		},
	}))

	got := store.GetOverrides("analytics")
	assert.Equal(t, "true", got["NORNICDB_SEARCH_BM25_ENABLED"], "admin's value preserved")
	assert.Equal(t, "lazy", got["NORNICDB_SEARCH_VECTOR_WARMING"], "yaml fills missing key")
}

// TestLoadWithYAMLDefaults_RejectsDisallowedKeys — the seed path applies
// the same allow-list as the admin API; yaml typos don't get persisted.
func TestLoadWithYAMLDefaults_RejectsDisallowedKeys(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	require.NoError(t, store.LoadWithYAMLDefaults(ctx, map[string]map[string]string{
		"analytics": {
			"NORNICDB_NOT_A_REAL_KEY":      "anything",
			"NORNICDB_SEARCH_BM25_ENABLED": "false",
		},
	}))

	got := store.GetOverrides("analytics")
	require.NotNil(t, got)
	_, present := got["NORNICDB_NOT_A_REAL_KEY"]
	assert.False(t, present, "disallowed key must not have been persisted")
	assert.Equal(t, "false", got["NORNICDB_SEARCH_BM25_ENABLED"])
}
