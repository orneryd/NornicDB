package storage

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-recovery-pending-marking).
// Test-side only, no deadlock. The settlement callback runs on the goroutine
// that submitEntityCommit starts as accepted work of the base engine
// (wal_entity_commit.go:146-154: work.Enter ... defer work.Leave, the receipt
// resolve -> settle closure at :99-105). WALEngine.Close (wal_engine.go:1048)
// disposes through BadgerEngine.Close (badger_stats.go:258-266), whose
// JoinStopped waits for that work to leave (lifecycle/work_lifetime.go:91-102,
// 129-132). The callback here starts Close on another goroutine, holds the
// callback open for 3 s and reports whether Close finished meanwhile, then
// returns and reports when Close finishes.

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRCASettlementCallbackRunsInsideEngineWork(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	fixture := newWALSubmitEntityCommitSettlementCloseEngine(t, nil)
	id := NodeID("test:rca-settlement")
	prepared := walSettlementClosePrepare(t, fixture.base, id)
	closed := make(chan error, 1)
	report := make(chan string, 4)
	_, err := fixture.owner.SubmitEntityCommit(context.Background(), prepared, func(outcome CommitOutcome) {
		buf := make([]byte, 1<<16)
		stack := string(buf[:runtime.Stack(buf, false)])
		inside := strings.Contains(stack, "submitEntityCommit.func")
		report <- "RCA_SETTLE callback outcome=" + fmt.Sprint(outcome.State) + " running on submitEntityCommit work goroutine=" + rcaBoolText(inside)
		start := time.Now()
		go func() { closed <- fixture.owner.Close() }()
		select {
		case err := <-closed:
			report <- "RCA_SETTLE Close finished while callback still running after " + time.Since(start).String() + " err=" + rcaErrText(err)
			closed <- err
		case <-time.After(3 * time.Second):
			report <- "RCA_SETTLE Close still blocked after 3s while the callback (inside the engine work) is running"
		}
	})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		select {
		case line := <-report:
			t.Log(line)
		case <-time.After(20 * time.Second):
			t.Fatalf("no callback report:\n%s", walSettlementCloseStacks())
		}
	}
	returned := time.Now()
	select {
	case err := <-closed:
		t.Logf("RCA_SETTLE after the callback returned, Close finished in %s err=%v", time.Since(returned), err)
	case <-time.After(20 * time.Second):
		t.Logf("RCA_SETTLE Close still blocked 20s after the callback returned:\n%s", walSettlementCloseStacks())
	}
}

func rcaBoolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func rcaErrText(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}
