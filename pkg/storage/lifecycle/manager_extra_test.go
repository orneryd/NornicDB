package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// stubLifecycleStorageEngine is the minimum LifecycleStorageEngine
// implementation to construct an MVCCLifecycleManager. The lifecycle
// loop will call it but for the methods exercised in these unit
// tests (Pause/Resume/Status/etc.) the implementation does not need
// to do anything beyond returning safe defaults.
type stubLifecycleStorageEngine struct {
	freeSpaceErr error
	freeBytes    int64
}

func (s *stubLifecycleStorageEngine) IterateMVCCHeads(ctx context.Context, yield func(logicalKey []byte, head storage.MVCCHead) error) error {
	return nil
}
func (s *stubLifecycleStorageEngine) IterateMVCCVersions(ctx context.Context, logicalKey []byte, yield func(version storage.MVCCVersion, tombstoned bool, sizeBytes int64) error) error {
	return nil
}
func (s *stubLifecycleStorageEngine) DeleteMVCCVersion(ctx context.Context, logicalKey []byte, version storage.MVCCVersion) error {
	return nil
}
func (s *stubLifecycleStorageEngine) WriteMVCCHead(ctx context.Context, logicalKey []byte, head storage.MVCCHead) error {
	return nil
}
func (s *stubLifecycleStorageEngine) ReadMVCCHead(ctx context.Context, logicalKey []byte) (storage.MVCCHead, error) {
	return storage.MVCCHead{}, nil
}
func (s *stubLifecycleStorageEngine) DataDirFreeSpace() (int64, error) {
	return s.freeBytes, s.freeSpaceErr
}

func newTestLifecycleManager(t *testing.T) *MVCCLifecycleManager {
	t.Helper()
	cfg := LifecycleConfig{
		Enabled:       true,
		CycleInterval: time.Hour, // long enough to never tick during tests
	}
	mgr := NewMVCCLifecycleManager(cfg, &stubLifecycleStorageEngine{freeBytes: 1 << 30})
	t.Cleanup(func() { mgr.StopLifecycle() })
	return mgr
}

func TestMVCCLifecycleManager_PauseResume(t *testing.T) {
	mgr := newTestLifecycleManager(t)

	// Initially not paused.
	require.False(t, mgr.isPaused())

	mgr.PauseLifecycle()
	require.True(t, mgr.isPaused())

	mgr.ResumeLifecycle()
	require.False(t, mgr.isPaused())
}

func TestMVCCLifecycleManager_IsLifecycleEnabled(t *testing.T) {
	mgr := newTestLifecycleManager(t)
	require.True(t, mgr.IsLifecycleEnabled())

	disabled := NewMVCCLifecycleManager(LifecycleConfig{Enabled: false}, &stubLifecycleStorageEngine{})
	t.Cleanup(func() { disabled.StopLifecycle() })
	require.False(t, disabled.IsLifecycleEnabled())
}

func TestMVCCLifecycleManager_ReaderRegistry_DelegateRegister(t *testing.T) {
	mgr := newTestLifecycleManager(t)

	registry := mgr.ReaderRegistry()
	require.NotNil(t, registry)
	require.Equal(t, int64(0), registry.ActiveCount())

	dereg := mgr.RegisterSnapshotReader(storage.SnapshotReaderInfo{ReaderID: "r1"})
	require.NotNil(t, dereg)
	require.Equal(t, int64(1), registry.ActiveCount())
	dereg()
	require.Equal(t, int64(0), registry.ActiveCount())
}

func TestMVCCLifecycleManager_LifecycleStatus(t *testing.T) {
	mgr := newTestLifecycleManager(t)
	mgr.PauseLifecycle()

	status := mgr.LifecycleStatus()
	require.True(t, status["enabled"].(bool))
	require.True(t, status["paused"].(bool))
	require.NotNil(t, status["pressure_band"])
	require.NotNil(t, status["cycle_interval"])
}

func TestMVCCLifecycleManager_TopLifecycleDebtKeys_EmptyByDefault(t *testing.T) {
	mgr := newTestLifecycleManager(t)
	got := mgr.TopLifecycleDebtKeys(10)
	require.Empty(t, got, "no plan run yet ⇒ no debt keys")
}

func TestMVCCLifecycleManager_TriggerPruneNow_Disabled(t *testing.T) {
	disabled := NewMVCCLifecycleManager(LifecycleConfig{Enabled: false}, &stubLifecycleStorageEngine{})
	t.Cleanup(func() { disabled.StopLifecycle() })

	err := disabled.TriggerPruneNow(context.Background())
	require.NoError(t, err, "prune-now is a no-op when lifecycle is disabled")
}

func TestMVCCLifecycleManager_AcquireSnapshotReader_Allowed(t *testing.T) {
	mgr := newTestLifecycleManager(t)
	dereg, err := mgr.AcquireSnapshotReader(storage.SnapshotReaderInfo{
		ReaderID:  "ok-reader",
		StartTime: time.Now(),
	})
	require.NoError(t, err)
	require.NotNil(t, dereg)
	dereg()
}

func TestMVCCLifecycleManager_EvaluateSnapshotReader_BoundaryCases(t *testing.T) {
	mgr := newTestLifecycleManager(t)
	graceful, hard := mgr.EvaluateSnapshotReader(storage.SnapshotReaderInfo{
		ReaderID:  "fresh",
		StartTime: time.Now(),
	})
	require.False(t, graceful, "fresh reader is not yet over the limit")
	require.False(t, hard)
}

func TestMVCCLifecycleManager_SetLifecycleSchedule_Validation(t *testing.T) {
	mgr := newTestLifecycleManager(t)
	require.NoError(t, mgr.SetLifecycleSchedule(0)) // 0 → disable automatic
	require.NoError(t, mgr.SetLifecycleSchedule(5*time.Minute))

	// Negative is rejected.
	require.Error(t, mgr.SetLifecycleSchedule(-time.Second))
}
