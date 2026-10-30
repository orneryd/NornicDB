package observability

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 — runtime collector PRESENCE assertion (Pitfall 8).
//
// MET-17 (Go runtime + process collectors) was already shipped by Phase 1
// in pkg/observability/registry.go:34-35. Re-registering the collectors
// here would panic with prometheus.AlreadyRegisteredError (RESEARCH §Q15
// + Pitfall 8). 04-01 ships a presence test ONLY, never a re-registration.
//
// The TestEnv constructor (testenv.go:75-77) registers the same collectors
// against its isolated registry, so this test exercises BOTH the production
// init path and the test-fixture init path — they must agree.

// TestRuntimeCollectorsRegistered asserts the Go runtime + process
// collectors are present in the registry produced by NewTestEnv. The
// presence shape is "go_*" prefixes for the Go collector and "process_*"
// for the process collector — these are stdlib client_golang names and
// are NOT in the nornicdb_ namespace.
func TestRuntimeCollectorsRegistered(t *testing.T) {
	te := NewTestEnv(t)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)

	var sawGo, sawProcess bool
	for _, n := range names {
		switch {
		case strings.HasPrefix(n, "go_"):
			sawGo = true
		case strings.HasPrefix(n, "process_"):
			sawProcess = true
		}
	}
	assert.True(t, sawGo,
		"MET-17: at least one go_* runtime metric must register (Phase 1 registry.go:34)")
	assert.True(t, sawProcess,
		"MET-17: at least one process_* metric must register (Phase 1 registry.go:35)")
}
