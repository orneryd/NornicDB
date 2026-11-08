package search

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorCheckpointReplacementFailureRetainsCause(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vectors")
	v, err := NewVectorFileStore(base, 4)
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })
	require.NoError(t, v.Add("committed", []float32{1, 0, 0, 0}))
	require.NoError(t, v.Save())
	require.NoError(t, v.Add("uncommitted", []float32{0, 1, 0, 0}))
	// Load must retire the unsaved tail through replacement. A real filesystem
	// failure after closing the append handle leaves the publication poisoned.
	blocker := base + ".meta.next.tmp"
	require.NoError(t, os.Mkdir(blocker, 0o700))
	publicationErr := v.Load()
	require.Error(t, publicationErr)
	require.Nil(t, v.file)
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"add", func() error { return v.Add("later", []float32{0, 0, 1, 0}) }},
		{"save", v.Save}, {"load", v.Load}, {"sync", v.Sync},
		{"relocate", func() error { return v.relocateCheckpoint(base + "-new") }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			var err error
			require.NotPanics(t, func() { err = operation.run() })
			require.ErrorIs(t, err, publicationErr)
		})
	}
	require.NoError(t, os.Remove(blocker))
	require.NoError(t, v.Close())
	restored, err := NewVectorFileStore(base, 4)
	require.NoError(t, err)
	defer restored.Close()
	require.NoError(t, restored.Load())
	require.Equal(t, 1, restored.Count())
	got, ok := restored.GetVector("committed")
	require.True(t, ok)
	require.Equal(t, []float32{1, 0, 0, 0}, got)
	require.False(t, restored.Has("uncommitted"))
	require.NoError(t, restored.Add("after-recovery", []float32{0, 0, 0, 1}))
	require.NoError(t, restored.Save())
}
