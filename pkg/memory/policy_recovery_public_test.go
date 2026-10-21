package memory_test

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestApplicationMemoryReclaimsCompletedWork(t *testing.T) {
	policy := memorypressure.New(t)
	require.NoError(t, policy.Refresh())
	work, err := policy.Reserve(64 << 10)
	require.NoError(t, err)
	memorypressure.Garbage()
	work.Release()
	require.NoError(t, policy.Refresh())
	_, err = policy.Reserve(64 << 10)
	require.ErrorIs(t, err, memory.ErrLimitExceeded)
	// The existing periodic sampler must restore availability without an
	// allocation from a new request or an operator's explicit GC request.
	require.NoError(t, policy.Refresh())
	lease, err := policy.Reserve(64 << 10)
	require.NoError(t, err, "completed work must not pin admission until the next allocation-triggered GC")
	lease.Release()
}
