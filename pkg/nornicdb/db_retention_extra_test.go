package nornicdb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNodeToDataRecord_SubjectIDFromProperties(t *testing.T) {
	node := &storage.Node{
		ID:        "test:n1",
		Labels:    []string{"User"},
		CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
		Properties: map[string]any{
			"user_id": "u-123",
			"email":   "test@example.com",
		},
	}

	record := nodeToDataRecord(node, []string{"user_id", "email"})
	require.Equal(t, "test:n1", record.ID)
	require.Equal(t, "u-123", record.SubjectID, "first matching key wins")
	require.Equal(t, retention.CategoryUser, record.Category)
	require.Equal(t, node.CreatedAt, record.CreatedAt)
	require.Equal(t, node.UpdatedAt, record.LastAccessedAt, "UpdatedAt > CreatedAt populates LastAccessedAt")
}

func TestNodeToDataRecord_SubjectIDFallbackToSecondKey(t *testing.T) {
	node := &storage.Node{
		ID:         "test:n",
		Properties: map[string]any{"email": "x@example.com"},
	}
	record := nodeToDataRecord(node, []string{"user_id", "email"})
	require.Equal(t, "x@example.com", record.SubjectID)
}

func TestNodeToDataRecord_NoSubjectMatch(t *testing.T) {
	node := &storage.Node{
		ID:         "test:n",
		Properties: map[string]any{"unrelated": "v"},
	}
	record := nodeToDataRecord(node, []string{"user_id"})
	require.Empty(t, record.SubjectID)
}

func TestNodeToDataRecord_NoUpdatedAtKeepsZeroLastAccessed(t *testing.T) {
	created := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	node := &storage.Node{
		ID:        "test:n",
		CreatedAt: created,
		// UpdatedAt not after CreatedAt → LastAccessedAt stays zero.
	}
	record := nodeToDataRecord(node, nil)
	require.True(t, record.LastAccessedAt.IsZero())
}

func TestInferCategory_LabelMappings_Comprehensive(t *testing.T) {
	cases := []struct {
		label    string
		expected retention.DataCategory
	}{
		{"AuditLog", retention.CategoryAudit},
		{"Audit", retention.CategoryAudit},
		{"PHI", retention.CategoryPHI},
		{"HealthRecord", retention.CategoryPHI},
		{"PII", retention.CategoryPII},
		{"PersonalData", retention.CategoryPII},
		{"Financial", retention.CategoryFinancial},
		{"Transaction", retention.CategoryFinancial},
		{"Analytics", retention.CategoryAnalytics},
		{"Metric", retention.CategoryAnalytics},
		{"Telemetry", retention.CategoryAnalytics},
		{"System", retention.CategorySystem},
		{"Config", retention.CategorySystem},
		{"Schema", retention.CategorySystem},
		{"Archive", retention.CategoryArchive},
		{"Backup", retention.CategoryBackup},
		{"Legal", retention.CategoryLegal},
		{"LegalDocument", retention.CategoryLegal},
	}
	for _, tc := range cases {
		got := inferCategory(&storage.Node{Labels: []string{tc.label}})
		require.Equal(t, tc.expected, got, "label=%q", tc.label)
	}
}

func TestInferCategory_FallbackToPropertyDataCategory(t *testing.T) {
	got := inferCategory(&storage.Node{
		Labels:     []string{"NoMatch"},
		Properties: map[string]any{"data_category": "MARKETING"},
	})
	require.Equal(t, retention.DataCategory("MARKETING"), got)
}

func TestInferCategory_DefaultsToUserCategory(t *testing.T) {
	got := inferCategory(&storage.Node{
		Labels:     []string{"NoMatch"},
		Properties: map[string]any{},
	})
	require.Equal(t, retention.CategoryUser, got)
}

func TestRetentionContext_FallbackChain(t *testing.T) {
	// Explicit context wins.
	type contextKey struct{}
	want := context.WithValue(context.Background(), contextKey{}, "x")
	db := &DB{}
	require.Equal(t, want, db.retentionContext(want))

	// A background context remains unchanged when no build context exists.
	got := db.retentionContext(context.Background())
	require.Equal(t, context.Background(), got)

	// An explicit context takes precedence over buildCtx.
	build := context.WithValue(context.Background(), contextKey{}, "build")
	db.buildCtx = build
	require.Equal(t, want, db.retentionContext(want))
}

func TestRetentionExcludedLabels_TrimWhitespaceAndDropEmpty(t *testing.T) {
	require.Nil(t, (*DB)(nil).retentionExcludedLabels())

	// No config → nil.
	db := &DB{}
	require.Nil(t, db.retentionExcludedLabels())

	// Whitespace-only labels are pruned.
	cfg := &Config{}
	cfg.Retention.ExcludedLabels = []string{"  AuditLog  ", "", " ", "System"}
	db.config = cfg
	got := db.retentionExcludedLabels()
	require.Equal(t, map[string]struct{}{
		"AuditLog": {},
		"System":   {},
	}, got)
}

func TestRetentionMaxSweepRecords_DefaultsAndOverride(t *testing.T) {
	require.Equal(t, defaultRetentionMaxSweepRecords, (*DB)(nil).retentionMaxSweepRecords())

	db := &DB{config: &Config{}}
	require.Equal(t, defaultRetentionMaxSweepRecords, db.retentionMaxSweepRecords())

	db.config.Retention.MaxSweepRecords = 100
	require.Equal(t, 100, db.retentionMaxSweepRecords())
}

func TestStartRetentionSweep_NoOpWithoutManager(t *testing.T) {
	// No retention manager installed → startRetentionSweep is a no-op.
	db := &DB{}
	db.startRetentionSweep(context.Background())
}

func TestRetentionSweep_ProcessesExpiredAndSkipsExcluded(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	namespaced := storage.NewNamespacedEngine(engine, "nornic")
	now := time.Now().UTC()
	_, err := namespaced.CreateNode(&storage.Node{ID: "old", Labels: []string{"User"}, CreatedAt: now.Add(-2 * time.Hour), Properties: map[string]any{"owner_id": "alice"}})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&storage.Node{ID: "recent", Labels: []string{"User"}, CreatedAt: now, Properties: map[string]any{"owner_id": "bob"}})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&storage.Node{ID: "audit", Labels: []string{"AuditLog"}, CreatedAt: now.Add(-2 * time.Hour), Properties: map[string]any{"owner_id": "carol"}})
	require.NoError(t, err)

	manager := retention.NewManager()
	require.NoError(t, manager.AddPolicy(&retention.Policy{
		ID:       "user-short",
		Category: retention.CategoryUser,
		RetentionPeriod: retention.RetentionPeriod{
			Duration: time.Hour,
		},
		Active: true,
	}))
	var deleted []string
	manager.SetDeleteCallback(func(record *retention.DataRecord) error {
		deleted = append(deleted, record.ID)
		return namespaced.DeleteNode(storage.NodeID(record.ID))
	})

	db := &DB{storage: namespaced, baseStorage: engine, config: &Config{}, retentionManager: manager, searchServices: make(map[string]*dbSearchService)}
	db.config.Retention.ExcludedLabels = []string{"AuditLog"}
	db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) { return false, false, "startup", "startup" })
	db.runRetentionSweep(context.Background())

	require.Equal(t, []string{"old"}, deleted)
	_, err = namespaced.GetNode("old")
	require.Error(t, err)
	_, err = namespaced.GetNode("recent")
	require.NoError(t, err)
	_, err = namespaced.GetNode("audit")
	require.NoError(t, err)
}

func TestRetentionSweep_BudgetAndStartBranches(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	namespaced := storage.NewNamespacedEngine(engine, "nornic")
	_, err := namespaced.CreateNode(&storage.Node{ID: "first", Labels: []string{"User"}, CreatedAt: time.Now().Add(-2 * time.Hour), Properties: map[string]any{"owner_id": "alice"}})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&storage.Node{ID: "second", Labels: []string{"User"}, CreatedAt: time.Now().Add(-2 * time.Hour), Properties: map[string]any{"owner_id": "bob"}})
	require.NoError(t, err)

	manager := retention.NewManager()
	require.NoError(t, manager.AddPolicy(&retention.Policy{ID: "user-short", Category: retention.CategoryUser, RetentionPeriod: retention.RetentionPeriod{Duration: time.Hour}, Active: true}))
	db := &DB{storage: namespaced, baseStorage: engine, config: &Config{}, retentionManager: manager, searchServices: make(map[string]*dbSearchService)}
	db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) { return false, false, "startup", "startup" })
	db.config.Retention.MaxSweepRecords = 1
	db.runRetentionSweep(context.Background())

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	db.config.Retention.SweepIntervalSeconds = 1
	db.startRetentionSweep(canceled)
	db.bgWg.Wait()

	db.closed = true
	db.startRetentionSweep(context.Background())
}
