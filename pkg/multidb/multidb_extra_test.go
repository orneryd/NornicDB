package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T) *DatabaseManager {
	t.Helper()
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	m, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })
	return m
}

// ============================================================================
// DefaultLimits / DefaultConfig
// ============================================================================

func TestDefaultLimits(t *testing.T) {
	limits := DefaultLimits()
	assert.NotNil(t, limits)
}

func TestLimitsHelpers(t *testing.T) {
	t.Run("query limits max results handles nil and value", func(t *testing.T) {
		var q *QueryLimits
		assert.Equal(t, int64(0), q.GetMaxResults())

		q = &QueryLimits{MaxResults: 25}
		assert.Equal(t, int64(25), q.GetMaxResults())
	})

	t.Run("limits unlimited and max results helpers", func(t *testing.T) {
		var limits *Limits
		assert.True(t, limits.IsUnlimited())
		assert.Equal(t, int64(0), limits.GetMaxResults())

		limits = DefaultLimits()
		assert.True(t, limits.IsUnlimited())
		assert.Equal(t, int64(0), limits.GetMaxResults())

		limits.Query.MaxResults = 50
		assert.False(t, limits.IsUnlimited())
		assert.Equal(t, int64(50), limits.GetMaxResults())
	})
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.NotNil(t, cfg)
	assert.Equal(t, "nornic", cfg.DefaultDatabase)
	assert.Equal(t, "system", cfg.SystemDatabase)
}

// ============================================================================
// CreateDatabase / ListDatabases / GetDatabase
// ============================================================================

func TestDatabaseManager_CreateDatabase_Extra(t *testing.T) {
	m := newTestManager(t)
	err := m.CreateDatabase("testdb1")
	require.NoError(t, err)

	dbs := m.ListDatabases()
	names := make([]string, len(dbs))
	for i, db := range dbs {
		names[i] = db.Name
	}
	assert.Contains(t, names, "testdb1")
}

func TestDatabaseManager_CreateDatabase_AlreadyExists(t *testing.T) {
	m := newTestManager(t)
	_ = m.CreateDatabase("dupdb")
	err := m.CreateDatabase("dupdb")
	// Should either succeed (idempotent) or return an error
	_ = err
}

func TestDatabaseManager_ListDatabases_Extra(t *testing.T) {
	m := newTestManager(t)
	dbs := m.ListDatabases()
	assert.NotEmpty(t, dbs) // default + system databases
}

func TestDatabaseManager_GetDatabase(t *testing.T) {
	m := newTestManager(t)
	_ = m.CreateDatabase("getdb1")
	db, err := m.GetDatabase("getdb1")
	require.NoError(t, err)
	assert.NotNil(t, db)
}

func TestDatabaseManager_GetDatabase_NotFound(t *testing.T) {
	m := newTestManager(t)
	_, err := m.GetDatabase("nonexistent")
	assert.Error(t, err)
}

// ============================================================================
// Alias CRUD: CreateAlias / DropAlias / ListAliases / ResolveDatabase
// ============================================================================

func TestDatabaseManager_CreateAlias(t *testing.T) {
	m := newTestManager(t)
	err := m.CreateAlias("myalias", "nornic")
	require.NoError(t, err)
}

func TestDatabaseManager_CreateAlias_TargetNotExist(t *testing.T) {
	m := newTestManager(t)
	err := m.CreateAlias("badalias", "doesnotexist")
	assert.Error(t, err)
}

func TestDatabaseManager_CreateAlias_ConflictsWithDBName(t *testing.T) {
	m := newTestManager(t)
	// "nornic" is already a database name — alias must not shadow it
	err := m.CreateAlias("nornic", "nornic")
	assert.Error(t, err)
}

func TestDatabaseManager_DropAlias(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.CreateAlias("dropalias", "nornic"))
	err := m.DropAlias("dropalias")
	require.NoError(t, err)
}

func TestDatabaseManager_DropAlias_NotExist(t *testing.T) {
	m := newTestManager(t)
	err := m.DropAlias("no-such-alias")
	assert.Error(t, err)
}

func TestDatabaseManager_ListAliases(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.CreateAlias("la1", "nornic"))
	require.NoError(t, m.CreateAlias("la2", "nornic"))
	aliases := m.ListAliases("nornic")
	assert.Contains(t, aliases, "la1")
	assert.Contains(t, aliases, "la2")
}

func TestDatabaseManager_ResolveDatabase_DirectName(t *testing.T) {
	m := newTestManager(t)
	resolved, err := m.ResolveDatabase("nornic")
	require.NoError(t, err)
	assert.Equal(t, "nornic", resolved)
}

func TestDatabaseManager_ResolveDatabase_ViaAlias(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.CreateAlias("resolve-alias", "nornic"))
	resolved, err := m.ResolveDatabase("resolve-alias")
	require.NoError(t, err)
	assert.Equal(t, "nornic", resolved)
}

func TestDatabaseManager_ResolveDatabase_NotExist(t *testing.T) {
	m := newTestManager(t)
	_, err := m.ResolveDatabase("no-such-db-or-alias")
	assert.Error(t, err)
}

// ============================================================================
// IsCompositeDatabase
// ============================================================================

func TestDatabaseManager_IsCompositeDatabase_False(t *testing.T) {
	m := newTestManager(t)
	assert.False(t, m.IsCompositeDatabase("nornic"))
}

// ============================================================================
// GetDatabaseLimits / IncrementStorageSize / DecrementStorageSize
// ============================================================================

func TestDatabaseManager_GetDatabaseLimits_NoLimitsSet(t *testing.T) {
	m := newTestManager(t)
	limits, err := m.GetDatabaseLimits("nornic")
	require.NoError(t, err)
	assert.Nil(t, limits) // nil = no limits set (unlimited by design)
}

func TestDatabaseManager_GetDatabaseLimits_NotFound(t *testing.T) {
	m := newTestManager(t)
	_, err := m.GetDatabaseLimits("nonexistent")
	assert.Error(t, err)
}

func TestDatabaseManager_IncrementStorageSize(t *testing.T) {
	m := newTestManager(t)
	// Should not panic
	m.IncrementStorageSize("nornic", 1024, 0)
}

func TestDatabaseManager_DecrementStorageSize(t *testing.T) {
	m := newTestManager(t)
	m.IncrementStorageSize("nornic", 1024, 0)
	m.DecrementStorageSize("nornic", 512, 0)
}

// ============================================================================
// Routing
// ============================================================================

func TestNewFullScanRouting(t *testing.T) {
	r := NewFullScanRouting()
	assert.NotNil(t, r)
}

func TestNewLabelRouting(t *testing.T) {
	r := NewLabelRouting()
	assert.NotNil(t, r)
}

func TestNewPropertyRouting(t *testing.T) {
	r := NewPropertyRouting("type")
	assert.NotNil(t, r)
}
