package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestHTTPSlowQueryLogZeroThresholdDisablesIt (NOR-284): a zero slow-query
// threshold disables the HTTP slow-query log, as it does the Cypher
// executor's and the MERGE wait's reports; before, it logged every query.
func TestHTTPSlowQueryLogZeroThresholdDisablesIt(t *testing.T) {
	server, _ := setupTestServer(t)
	server.config.SlowQueryEnabled = true
	server.config.Logging.SlowQueryThreshold = 0
	server.logSlowQuery("MATCH (n) RETURN n", nil, time.Hour, nil)
	require.Zero(t, server.slowQueryCount.Load())

	server.config.Logging.SlowQueryThreshold = time.Millisecond
	server.logSlowQuery("MATCH (n) RETURN n", nil, time.Second, nil)
	require.Equal(t, int64(1), server.slowQueryCount.Load())
}
