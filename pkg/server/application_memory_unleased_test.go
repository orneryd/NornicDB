package server

import (
	"context"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/stretchr/testify/require"
)

func TestHTTPApplicationMemoryReclaimsUnleasedWork(t *testing.T) {
	server, client, _ := startMemoryHTTPServer(t)
	status, body, _ := client.post("/db/nornic/tx/commit", "RETURN 1 AS ok")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body.Errors)

	var clockNanos atomic.Int64
	clockNanos.Store(time.Unix(100, 0).UnixNano())
	policy := memorypressure.NewWithClock(t, func() time.Time {
		return time.Unix(0, clockNanos.Load())
	})
	forcedGC := func() uint32 {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return stats.NumForcedGC
	}
	memorypressure.Garbage()
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	status, body, _ = client.post("/db/nornic/tx/commit", "RETURN 1 AS ok")
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Len(t, body.Errors, 1)
	require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", body.Errors[0].Code)
	before := forcedGC()
	require.NoError(t, policy.Refresh())
	require.Equal(t, before+1, forcedGC(), "the first sampler attempt must really collect")
	require.Zero(t, policy.Reserved())

	// No successful request or lease release occurs between the two episodes.
	memorypressure.Garbage()
	clockNanos.Add(int64(30*time.Second - time.Nanosecond))
	require.NoError(t, policy.Refresh())
	for i := 0; i < 10; i++ {
		status, body, _ = client.post("/db/nornic/tx/commit", "RETURN 1 AS ok")
		require.Equal(t, http.StatusServiceUnavailable, status)
		require.Len(t, body.Errors, 1)
		require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", body.Errors[0].Code)
	}
	require.Equal(t, before+1, forcedGC(), "HTTP retries must not force GC")

	clockNanos.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	ticks := time.NewTicker(25 * time.Millisecond)
	done := make(chan struct{})
	go func() { defer close(done); policy.Run(ctx, ticks.C) }()
	t.Cleanup(func() { cancel(); ticks.Stop(); <-done })
	require.Eventually(t, func() bool {
		status, body, _, err := client.do("/db/nornic/tx/commit", "RETURN 1 AS ok")
		return err == nil && status == http.StatusOK && len(body.Errors) == 0 &&
			len(body.Results) == 1 && len(body.Results[0].Data) == 1 && body.Results[0].Data[0].Row[0] == float64(1)
	}, 3*time.Second, 25*time.Millisecond, "small HTTP queries must recover from unleased garbage without manual GC")
	require.Equal(t, before+2, forcedGC(), "only the sampler may perform the second collection")
	require.Zero(t, policy.Reserved())
}
