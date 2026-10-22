package storage

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

type r4WALInnerClose struct {
	Engine
	failure error
}

func (e *r4WALInnerClose) Close() error { return errors.Join(e.failure, e.Engine.Close()) }
func TestWALR4CloseRetainsSiblingOutcomes(t *testing.T) {
	cfg := DefaultWALConfig()
	cfg.Dir = t.TempDir()
	cfg.SyncMode = "immediate"
	wal, err := NewWAL(cfg.Dir, cfg)
	require.NoError(t, err)
	// Native I/O failure setup only; the workload calls the public composed owner.
	// Closing the os.File marks its descriptor closed, so no descriptor is reused.
	require.NoError(t, wal.file.Close())
	sibling := errors.New("r4 underlying engine close failure")
	engine := NewWALEngine(&r4WALInnerClose{Engine: fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed)), failure: sibling}, wal)
	result := engine.Close()
	require.ErrorIs(t, result, os.ErrClosed, "WAL disposal outcome was lost")
	require.ErrorIs(t, result, sibling, "the sibling engine outcome was lost")
}
