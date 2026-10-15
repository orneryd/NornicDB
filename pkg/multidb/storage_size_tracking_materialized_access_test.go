package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type materializedAccessRecordingEngine struct {
	*storage.MemoryEngine
	ids []string
}

func (e *materializedAccessRecordingEngine) RecordMaterializedAccess(entityID string) {
	e.ids = append(e.ids, entityID)
}

func TestSizeTrackingEnginePreservesMaterializedAccess(t *testing.T) {
	inner := &materializedAccessRecordingEngine{MemoryEngine: storage.NewMemoryEngine()}
	t.Cleanup(func() { _ = inner.Close() })
	wrapped := newSizeTrackingEngine(inner, nil, "nornic")
	recorder, ok := wrapped.(interface{ RecordMaterializedAccess(string) })
	require.True(t, ok)
	recorder.RecordMaterializedAccess("nornic:a")
	require.Equal(t, []string{"nornic:a"}, inner.ids)
}
