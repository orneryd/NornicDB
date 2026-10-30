package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngineDataDirFreeSpace(t *testing.T) {
	t.Run("engine data dir reports free space", func(t *testing.T) {
		engine, _ := createTestBadgerEngineOnDisk(t)
		t.Cleanup(func() {
			require.NoError(t, engine.Close())
		})

		free, err := engine.DataDirFreeSpace()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, free, int64(0))
	})

	t.Run("direct helper returns error for missing path", func(t *testing.T) {
		free, err := dataDirFreeSpace("/definitely-not-a-real-dir/nornicdb")
		require.Error(t, err)
		assert.Equal(t, int64(0), free)
	})
}
