package nornicdb

import (
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

	// nil context + no buildCtx → context.Background.
	got := db.retentionContext(nil)
	require.Equal(t, context.Background(), got)

	// nil context + buildCtx → buildCtx.
	build := context.WithValue(context.Background(), contextKey{}, "build")
	db.buildCtx = build
	require.Equal(t, build, db.retentionContext(nil))
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
	db.startRetentionSweep(nil)
}
