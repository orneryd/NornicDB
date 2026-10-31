package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/orneryd/nornicdb/testing/capturecensus"
	"github.com/stretchr/testify/require"
)

// The compiled population determines readers, including optional capabilities
// and new declarations outside storage. Behavioral tests prove their capture
// and publication schedules; a source reachability witness alone does not.
func TestSnapshotOverlayPublicReadsReachCapture(t *testing.T) {
	facts, err := capturecensus.Extract(workcensus.Config{Dir: "../..", Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Env: append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")}, capturecensus.StoragePolicy())
	require.NoError(t, err)
	require.NotEmpty(t, facts.Reads)
	require.NotEmpty(t, facts.Consumers)
	if os.Getenv("PRINT_CAPTURE_CENSUS") == "1" {
		data, err := json.Marshal(facts)
		require.NoError(t, err)
		t.Log(string(data))
	}
	// Every fresh request context reaching the capture owners, with the reason
	// no caller context exists there; the census fails any other placement.
	for _, site := range facts.ContextSites {
		t.Logf("context site %s:%d %s -> %s: %s", site.File, site.Line, site.Function, site.Callee, site.Reason)
	}
	for _, finding := range facts.NilContextsElsewhere {
		t.Logf("nil context outside the capture family: %s", finding)
	}
	require.Empty(t, facts.Violations)
}
