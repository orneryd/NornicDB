package nornicdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplicationMemoryPolicyFollowsDatabaseLifetime(t *testing.T) {
	config := DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.ApplicationSoftMax = 256 << 20
	config.Memory.SearchBM25Enabled = false
	config.Memory.SearchVectorEnabled = false
	config.Memory.SearchBM25Warming = "lazy"
	config.Memory.SearchVectorWarming = "lazy"
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	require.NotNil(t, db.MemoryPolicy())
	require.NoError(t, db.Close())
	require.NoError(t, db.Close(), "repeated Close waits for the sampler owner")
}
