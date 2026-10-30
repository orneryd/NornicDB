package nornicdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasExcludedLabel(t *testing.T) {
	node := &storage.Node{Labels: []string{"User", "AuditLog"}}
	assert.True(t, hasExcludedLabel(node, map[string]struct{}{"AuditLog": {}}))
	assert.False(t, hasExcludedLabel(node, map[string]struct{}{"System": {}}))
}

func TestInferCategoryPrefersLabels(t *testing.T) {
	node := &storage.Node{
		Labels: []string{"PII", "User"},
		Properties: map[string]any{
			"data_category": "AUDIT",
		},
	}
	assert.Equal(t, retention.CategoryPII, inferCategory(node))
}

func TestNodeToDataRecordUsesConfiguredSubjectSelectors(t *testing.T) {
	now := time.Now().Add(-time.Hour)
	node := &storage.Node{
		ID:        "n1",
		CreatedAt: now,
		UpdatedAt: now.Add(10 * time.Minute),
		Labels:    []string{"Financial"},
		Properties: map[string]any{
			"account_id": "acct-123",
		},
	}
	record := nodeToDataRecord(node, []string{"owner_id", "account_id"})
	require.NotNil(t, record)
	assert.Equal(t, "acct-123", record.SubjectID)
	assert.Equal(t, retention.CategoryFinancial, record.Category)
	assert.Equal(t, node.UpdatedAt, record.LastAccessedAt)
}

func TestRetentionExcludedLabelsConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Retention.ExcludedLabels = []string{"AuditLog", "System"}
	db := &DB{config: cfg}
	labels := db.retentionExcludedLabels()
	require.Len(t, labels, 2)
	_, hasAudit := labels["AuditLog"]
	_, hasSystem := labels["System"]
	assert.True(t, hasAudit)
	assert.True(t, hasSystem)
}

func TestRetentionContextPrefersExplicitContext(t *testing.T) {
	db := &DB{}
	explicit := context.WithValue(context.Background(), struct{}{}, "explicit")
	resolved := db.retentionContext(explicit)
	assert.Same(t, explicit, resolved)
}

func TestRetentionContextFallsBackToBuildContext(t *testing.T) {
	buildCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db := &DB{buildCtx: buildCtx}
	resolved := db.retentionContext(nilTestContext())
	assert.Same(t, buildCtx, resolved)
	cancel()
	select {
	case <-resolved.Done():
	default:
		t.Fatal("expected resolved retention context to be cancelled with build context")
	}
}

func nilTestContext() context.Context {
	return nil
}

func TestRetentionDisabledByDefaultLeavesManagerAndPolicyFileAbsent(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	require.False(t, cfg.Compliance.RetentionEnabled)

	db, err := Open(dataDir, cfg)
	require.NoError(t, err)
	require.Nil(t, db.GetRetentionManager())

	require.NoError(t, db.Close())
	_, err = os.Stat(filepath.Join(dataDir, "retention-policies.json"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
