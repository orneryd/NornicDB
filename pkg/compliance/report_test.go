package compliance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/stretchr/testify/require"
)

func TestComplianceReporter_HIPAAAndSOC2(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	arch, err := kms.NewAuditArchiver(kms.AuditArchiverConfig{
		LocalPath: path,
	})
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, arch.Archive(context.Background(), kms.AuditEvent{
		EventType: "KEY_GENERATED",
		KeyURI:    "kms://local/test",
		Status:    "SUCCESS",
		Timestamp: now,
	}))
	require.NoError(t, arch.Archive(context.Background(), kms.AuditEvent{
		EventType: "KEY_DECRYPT_FAILED",
		KeyURI:    "kms://local/test",
		Status:    "FAILURE",
		ErrorCode: "DEK_DECRYPT_FAILED",
		Timestamp: now,
	}))

	r := NewComplianceReporter(path)
	hipaa, err := r.ExportHIPAAEvidence(now.Add(-time.Minute), now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 2, hipaa.Summary.Total)
	require.Equal(t, 1, hipaa.Summary.Success)
	require.Equal(t, 1, hipaa.Summary.Failure)

	soc2, err := r.ExportSOC2Evidence(now.Add(-time.Minute), now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 2, soc2.Summary.Total)
}
