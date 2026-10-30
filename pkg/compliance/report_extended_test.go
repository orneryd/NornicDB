package compliance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComplianceReporter_HIPAAEvidence_FileNotFound(t *testing.T) {
	t.Parallel()
	r := NewComplianceReporter("/nonexistent/audit.jsonl")
	_, err := r.ExportHIPAAEvidence(time.Time{}, time.Time{})
	require.Error(t, err)
}

func TestComplianceReporter_SOC2Evidence_FileNotFound(t *testing.T) {
	t.Parallel()
	r := NewComplianceReporter("/nonexistent/audit.jsonl")
	_, err := r.ExportSOC2Evidence(time.Time{}, time.Time{})
	require.Error(t, err)
}

func TestComplianceReporter_DateFiltering(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	arch, err := kms.NewAuditArchiver(kms.AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	baseTime := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	// Write events at different times
	for i := 0; i < 5; i++ {
		require.NoError(t, arch.Archive(context.Background(), kms.AuditEvent{
			EventType: "KEY_GENERATED",
			Status:    "SUCCESS",
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
		}))
	}

	r := NewComplianceReporter(path)

	// Filter to middle 3 events (hours 1-3)
	report, err := r.ExportHIPAAEvidence(
		baseTime.Add(1*time.Hour),
		baseTime.Add(3*time.Hour),
	)
	require.NoError(t, err)
	assert.Equal(t, 3, report.Summary.Total)
	assert.Equal(t, baseTime.Add(1*time.Hour), report.Summary.From)
	assert.Equal(t, baseTime.Add(3*time.Hour), report.Summary.To)

	// Zero start date — includes everything up to end
	soc2Report, err := r.ExportSOC2Evidence(time.Time{}, baseTime.Add(2*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 3, soc2Report.Summary.Total)

	// Zero end date — includes everything from start onward
	report, err = r.ExportHIPAAEvidence(baseTime.Add(3*time.Hour), time.Time{})
	require.NoError(t, err)
	assert.Equal(t, 2, report.Summary.Total)

	// Both zero — includes everything
	report, err = r.ExportHIPAAEvidence(time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, 5, report.Summary.Total)
}

func TestComplianceReporter_MalformedLines(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")

	// Write a mix of valid and invalid JSON lines
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.WriteString(`{"event_type":"KEY_GENERATED","status":"SUCCESS","timestamp":"2026-01-15T12:00:00Z"}` + "\n")
	require.NoError(t, err)
	_, err = f.WriteString("this is not json\n")
	require.NoError(t, err)
	_, err = f.WriteString(`{"event_type":"KEY_DECRYPTED","status":"FAILURE","error_code":"DECRYPT_FAILED","timestamp":"2026-01-15T13:00:00Z"}` + "\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r := NewComplianceReporter(path)
	report, err := r.ExportHIPAAEvidence(time.Time{}, time.Time{})
	require.NoError(t, err)

	// Malformed line should be skipped, valid lines counted
	assert.Equal(t, 2, report.Summary.Total)
	assert.Equal(t, 1, report.Summary.Success)
	assert.Equal(t, 1, report.Summary.Failure)
	assert.Equal(t, 1, report.Summary.ByEventType["KEY_GENERATED"])
	assert.Equal(t, 1, report.Summary.ByEventType["KEY_DECRYPTED"])
	assert.Equal(t, 1, report.Summary.ByStatusCode["DECRYPT_FAILED"])
}

func TestComplianceReporter_EmptyFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "empty.jsonl")
	require.NoError(t, os.WriteFile(path, []byte{}, 0600))

	r := NewComplianceReporter(path)
	report, err := r.ExportHIPAAEvidence(time.Time{}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, 0, report.Summary.Total)
	assert.Equal(t, 0, report.Summary.Success)
	assert.Equal(t, 0, report.Summary.Failure)
}

func TestComplianceReporter_ErrorCodeAggregation(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	arch, err := kms.NewAuditArchiver(kms.AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	ts := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	// Multiple failures with different error codes
	for _, code := range []string{"DECRYPT_FAILED", "DECRYPT_FAILED", "KEY_NOT_FOUND"} {
		require.NoError(t, arch.Archive(context.Background(), kms.AuditEvent{
			EventType: "KEY_DECRYPTED",
			Status:    "FAILURE",
			ErrorCode: code,
			Timestamp: ts,
		}))
	}
	// Success event (no error code)
	require.NoError(t, arch.Archive(context.Background(), kms.AuditEvent{
		EventType: "KEY_GENERATED",
		Status:    "SUCCESS",
		Timestamp: ts,
	}))

	r := NewComplianceReporter(path)
	report, err := r.ExportSOC2Evidence(time.Time{}, time.Time{})
	require.NoError(t, err)

	assert.Equal(t, 4, report.Summary.Total)
	assert.Equal(t, 1, report.Summary.Success)
	assert.Equal(t, 3, report.Summary.Failure)
	assert.Equal(t, 2, report.Summary.ByStatusCode["DECRYPT_FAILED"])
	assert.Equal(t, 1, report.Summary.ByStatusCode["KEY_NOT_FOUND"])
	// Success events should not appear in ByStatusCode
	_, hasEmpty := report.Summary.ByStatusCode[""]
	assert.False(t, hasEmpty)
}
