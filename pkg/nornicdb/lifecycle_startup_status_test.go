package nornicdb

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Startup still uses the process logger. Capture it only in an isolated child,
// so concurrent tests never have their logs redirected or inspected here.
func TestLifecycleStartupLoggingMatchesStatus(t *testing.T) {
	const childMode = "NORNIC_TEST_LIFECYCLE_STARTUP_MODE"
	if mode := os.Getenv(childMode); mode != "" {
		checkLifecycleStartupStatus(t, mode)
		return
	}
	for _, mode := range []string{"disabled", "periodic", "manual"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLifecycleStartupLoggingMatchesStatus$", "-test.timeout=25s", "-test.v")
			cmd.Env = append(os.Environ(), childMode+"="+mode)
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), "startup child exceeded its budget: %s", output)
			require.NoError(t, err, "%s", output)
		})
	}
}

type lifecycleStartupLog struct {
	mu      sync.Mutex
	bytes   bytes.Buffer
	ready   chan struct{}
	started sync.Once
}

func (l *lifecycleStartupLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.bytes.Write(p)
	// This event occurs after the lifecycle start and its diagnostic in the
	// same startup task. It proves an absent enabled claim was not checked
	// before that task reached the lifecycle branch.
	if strings.Contains(string(p), "Search disabled for database") {
		l.started.Do(func() { close(l.ready) })
	}
	return n, err
}

func (l *lifecycleStartupLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bytes.String()
}

func checkLifecycleStartupStatus(t *testing.T, mode string) {
	t.Helper()
	cfg := DefaultConfig()
	// Search warmup supplies a completion observation, without building any
	// indexes or changing the database's retention/lifecycle defaults.
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchVectorEnabled = false
	switch mode {
	case "disabled":
		require.Zero(t, cfg.Database.MVCCRetentionMaxVersions)
		require.Zero(t, cfg.Database.MVCCRetentionTTL)
	case "periodic":
		cfg.Database.MVCCRetentionMaxVersions = 2
	case "manual":
		cfg.Database.MVCCRetentionMaxVersions = 2
		cfg.Database.MVCCLifecycleCycleInterval = 0
	default:
		t.Fatalf("unknown startup mode %q", mode)
	}
	output := &lifecycleStartupLog{ready: make(chan struct{})}
	previous := log.Writer()
	log.SetOutput(output)
	defer log.SetOutput(previous)
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	select {
	case <-output.ready:
	case <-time.After(10 * time.Second):
		t.Fatalf("startup did not pass lifecycle initialization: %s", output.text())
	}
	status, err := db.LifecycleStatus()
	require.NoError(t, err)
	require.Equal(t, mode != "disabled", status["enabled"])
	require.Equal(t, mode == "periodic", status["running"])
	logs := output.text()
	switch mode {
	case "disabled":
		require.NotContains(t, logs, "MVCC lifecycle manager enabled")
	case "periodic":
		require.Contains(t, logs, "MVCC lifecycle manager enabled (cycle="+cfg.Database.MVCCLifecycleCycleInterval.String()+")")
		require.NotContains(t, logs, "MVCC lifecycle manager enabled in manual-only mode")
	case "manual":
		require.Contains(t, logs, "MVCC lifecycle manager enabled in manual-only mode")
		require.NotContains(t, logs, "MVCC lifecycle manager enabled (cycle=")
	}
}
