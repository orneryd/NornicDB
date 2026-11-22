package replication

// Report gap S11 (v1.5.2 verification campaign, test-side only): the
// replication half of "Closing the engine or replicator from inside a
// commit's settlement callback blocks". The journal half was proven by a run
// (work.Active() inside the callback, the callback's wait passed on to
// Close). This diagnostic runs the same measurement on the two replication
// paths:
//
//   standalone: StandaloneReplicator.SubmitAcceptedCommand launches the
//     application and its join (which runs the settlement callback) as
//     accepted work of the replicator (accepted_entity_command.go:73-87,
//     work.Enter before the goroutine, Leave deferred after the join);
//     Shutdown joins that work (replicator.go:488-491).
//   raft: the recorded application is settled through the native engine's
//     accepted work (storage.AcceptedWorkFor(base) is the BadgerEngine's
//     work lifetime); BadgerEngine.Close joins it.
//
// For each path the settlement callback: logs the owner's Active() count
// and its own goroutine's creation site; starts the owner's Close/Shutdown on
// another goroutine; waits 2 s and records whether Close returned; logs the
// closing goroutine's stack; then returns and measures how long Close needs
// after the callback returned. Control: the same owner closed from outside
// after the callback returned (Active() there, and the Close duration).
// Diagnostic: fails only on harness errors.

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const gapS11Hold = 2 * time.Second

func gapS11Prepare(t *testing.T, base *storage.BadgerEngine, id storage.NodeID) *storage.PreparedEntityCommit {
	t.Helper()
	tx, err := base.BeginTransaction(t.Context(), storage.OrdinaryBegin)
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: id, Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	return prepared
}

// gapS11Frames returns the function frames of the current goroutine and its
// "created by" line.
func gapS11Frames() string {
	buf := make([]byte, 1<<16)
	s := string(buf[:runtime.Stack(buf, false)])
	var parts []string
	lines := strings.Split(s, "\n")
	for i := 1; i < len(lines); i++ {
		l := strings.TrimSpace(lines[i])
		if l == "" || strings.HasPrefix(l, "/") {
			continue
		}
		if strings.HasPrefix(l, "created by ") {
			loc := ""
			if i+1 < len(lines) {
				loc = strings.TrimSpace(lines[i+1])
				if j := strings.Index(loc, " +0x"); j > 0 {
					loc = loc[:j]
				}
				if j := strings.Index(loc, "/pkg/"); j > 0 {
					loc = loc[j+1:]
				}
			}
			parts = append(parts, l+" @ "+loc)
			continue
		}
		if j := strings.Index(l, "("); j > 0 {
			l = l[:j]
		}
		if k := strings.LastIndex(l, "/"); k >= 0 {
			l = l[k+1:]
		}
		parts = append(parts, l)
	}
	return strings.Join(parts, " <- ")
}

// gapS11Goroutine returns the stack of the goroutine whose stack contains
// marker (first match), trimmed to its function lines with file:line.
func gapS11Goroutine(marker string) string {
	buf := make([]byte, 1<<22)
	all := string(buf[:runtime.Stack(buf, true)])
	for _, g := range strings.Split(all, "\n\n") {
		if !strings.Contains(g, marker) {
			continue
		}
		var out []string
		lines := strings.Split(g, "\n")
		out = append(out, strings.TrimSpace(lines[0]))
		for i := 1; i+1 < len(lines); i += 2 {
			fn := strings.TrimSpace(lines[i])
			if j := strings.Index(fn, "("); j > 0 {
				fn = fn[:j]
			}
			loc := strings.TrimSpace(lines[i+1])
			if j := strings.Index(loc, " +0x"); j > 0 {
				loc = loc[:j]
			}
			if j := strings.Index(loc, "/pkg/"); j > 0 {
				loc = loc[j+1:]
			}
			out = append(out, fn+" "+loc)
			if len(out) > 14 {
				break
			}
		}
		return strings.Join(out, " | ")
	}
	return "<no goroutine with " + marker + ">"
}

type gapS11Obs struct {
	activeInCallback   int
	callbackOrigin     string
	closedWhileHeld    bool
	closeStack         string
	afterReturn        time.Duration
	closeErr           error
	closeReturnedAtAll bool
}

// gapS11InCallback runs inside the settlement callback.
func gapS11InCallback(work *lifecycle.WorkLifetime, closeFn func() error, stackMarker string) (*gapS11Obs, chan struct{}) {
	o := &gapS11Obs{activeInCallback: work.Active(), callbackOrigin: gapS11Frames()}
	done := make(chan struct{})
	go func() {
		o.closeErr = closeFn()
		close(done)
	}()
	select {
	case <-done:
		o.closedWhileHeld = true
	case <-time.After(gapS11Hold):
		o.closeStack = gapS11Goroutine(stackMarker)
	}
	return o, done
}

func gapS11Report(t *testing.T, path string, o *gapS11Obs, done chan struct{}, returned time.Time) {
	select {
	case <-done:
		o.closeReturnedAtAll = true
		o.afterReturn = time.Since(returned)
	case <-time.After(30 * time.Second):
	}
	t.Logf("GAPS11 path=%s callback: owner.Active()=%d callback_goroutine=%q", path, o.activeInCallback, o.callbackOrigin)
	t.Logf("GAPS11 path=%s close_from_callback: returned_within_%v=%t close_goroutine_while_held=%q", path, gapS11Hold, o.closedWhileHeld, o.closeStack)
	t.Logf("GAPS11 path=%s close_from_callback: returned_after_callback_returned=%t after=%v err=%v", path, o.closeReturnedAtAll, o.afterReturn, o.closeErr)
}

func TestGapS11ReplicationCloseFromSettlementCallback(t *testing.T) {
	t.Run("standalone-shutdown", func(t *testing.T) {
		for _, fromCallback := range []bool{true, false} {
			dir := t.TempDir()
			base, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			config := DefaultConfig()
			config.Mode = ModeStandalone
			config.NodeID = "gap-s11-standalone"
			rep, err := NewReplicator(config, adapter)
			require.NoError(t, err)
			require.NoError(t, rep.Start(context.Background()))
			standalone, ok := rep.(*StandaloneReplicator)
			require.True(t, ok, "%T", rep)
			engine := NewReplicatedEngine(base, rep, 5*time.Second)
			prepared := gapS11Prepare(t, base, storage.NodeID(fmt.Sprintf("test:gap-s11-standalone-%t", fromCallback)))
			obsCh := make(chan *gapS11Obs, 1)
			doneCh := make(chan chan struct{}, 1)
			returnedCh := make(chan time.Time, 1)
			activeCh := make(chan int, 1)
			receipt, err := engine.SubmitEntityCommit(context.Background(), prepared, func(outcome storage.CommitOutcome) {
				if !fromCallback {
					activeCh <- standalone.work.Active()
					return
				}
				o, done := gapS11InCallback(&standalone.work, rep.Shutdown, "StandaloneReplicator).Shutdown")
				obsCh <- o
				doneCh <- done
				returnedCh <- time.Now()
			})
			require.NoError(t, err)
			if fromCallback {
				o := <-obsCh
				gapS11Report(t, "standalone", o, <-doneCh, <-returnedCh)
			} else {
				inCb := <-activeCh
				waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				require.NoError(t, receipt.Wait(waitCtx))
				cancel()
				time.Sleep(50 * time.Millisecond)
				outside := standalone.work.Active()
				start := time.Now()
				shutdownErr := rep.Shutdown()
				t.Logf("GAPS11 path=standalone CONTROL callback: owner.Active()=%d; after callback returned: owner.Active()=%d; Shutdown from outside took %v err=%v", inCb, outside, time.Since(start), shutdownErr)
			}
			_ = adapter.Close()
			_ = base.Close()
		}
	})
	t.Run("raft-engine-close", func(t *testing.T) {
		for _, fromCallback := range []bool{true, false} {
			dir := t.TempDir()
			base, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			raft, _ := newCoverageRaftReplicator(t)
			raft.storage = adapter
			raft.started.Store(true)
			raft.mu.Lock()
			raft.state, raft.currentTerm = StateLeader, 1
			raft.mu.Unlock()
			engine := NewReplicatedEngine(base, raft, 5*time.Second)
			acceptedWork, err := storage.AcceptedWorkFor(base)
			require.NoError(t, err)
			work, ok := acceptedWork.(*lifecycle.WorkLifetime)
			require.True(t, ok, "%T", acceptedWork)
			prepared := gapS11Prepare(t, base, storage.NodeID(fmt.Sprintf("test:gap-s11-raft-%t", fromCallback)))
			obsCh := make(chan *gapS11Obs, 1)
			doneCh := make(chan chan struct{}, 1)
			returnedCh := make(chan time.Time, 1)
			activeCh := make(chan int, 1)
			receipt, err := engine.SubmitEntityCommit(context.Background(), prepared, func(outcome storage.CommitOutcome) {
				if !fromCallback {
					activeCh <- work.Active()
					return
				}
				o, done := gapS11InCallback(work, base.Close, "BadgerEngine).Close")
				obsCh <- o
				doneCh <- done
				returnedCh <- time.Now()
			})
			require.NoError(t, err)
			var future *applyFuture
			select {
			case future = <-raft.applyCh:
			case <-time.After(20 * time.Second):
				t.Fatalf("the accepted command did not reach the raft apply queue")
			}
			applyDone := make(chan error, 1)
			go func() { applyDone <- raft.processApply(context.Background(), future) }()
			if fromCallback {
				o := <-obsCh
				gapS11Report(t, "raft", o, <-doneCh, <-returnedCh)
			} else {
				inCb := <-activeCh
				waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				require.NoError(t, receipt.Wait(waitCtx))
				cancel()
				time.Sleep(50 * time.Millisecond)
				outside := work.Active()
				start := time.Now()
				closeErr := base.Close()
				t.Logf("GAPS11 path=raft CONTROL callback: owner.Active()=%d; after callback returned: owner.Active()=%d; Close from outside took %v err=%v", inCb, outside, time.Since(start), closeErr)
			}
			select {
			case err := <-applyDone:
				t.Logf("GAPS11 path=raft fromCallback=%t processApply err=%v", fromCallback, err)
			case <-time.After(30 * time.Second):
				t.Logf("GAPS11 path=raft fromCallback=%t processApply did not return within 30s", fromCallback)
			}
			_ = adapter.Close()
			_ = base.Close()
		}
	})
}
