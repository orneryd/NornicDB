//go:build closeout_l2_e53_15

package storage

// Report entry 53.15: closing the engine from a journaled commit's settlement
// callback blocks until the callback returns. WALEngine.SubmitEntityCommit
// with a callback that starts base.Close() in a goroutine and waits up to 2 s
// for it: the callback waited the full 2.0 s and Close finished only after
// the callback returned. Control: base.SubmitEntityCommit (no journal)
// finishes at once. Expected: Close finishes while the callback waits.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

const (
	// l2E53_15CallbackWait is the entry's bound: the callback waits this long
	// for Close.
	l2E53_15CallbackWait = 2 * time.Second
	// l2E53_15CloseFast is the threshold: a Close that is not blocked by the
	// callback finishes in milliseconds on an almost empty store; the defect
	// keeps it waiting for the full 2 s callback bound. 1 s leaves a wide
	// margin on both sides.
	l2E53_15CloseFast = time.Second
)

type l2E53_15Result struct {
	submitErr        error
	callbackRan      bool
	closedInCallback bool
	waitedInCallback time.Duration
	activeInCallback int
	closeErr         error
	closeReturned    bool
	outcomeState     CommitOutcomeState
}

func (r l2E53_15Result) String() string {
	return fmt.Sprintf("submitErr=%v callbackRan=%v closedWhileCallbackWaited=%v callbackWaited=%s workActiveInCallback=%d closeReturned=%v closeErr=%v outcomeState=%d",
		r.submitErr, r.callbackRan, r.closedInCallback, r.waitedInCallback.Round(time.Millisecond), r.activeInCallback, r.closeReturned, r.closeErr, r.outcomeState)
}

// l2E53_15Run creates node test:callback-close in a native transaction,
// prepares its entity commit and submits it, journaled or not, with a
// settlement callback that closes the engine.
func l2E53_15Run(t *testing.T, journaled bool) l2E53_15Result {
	t.Helper()
	ctx := context.Background()
	base, err := NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	var wal *WAL
	if journaled {
		walDir := filepath.Join(t.TempDir(), "wal")
		wal, err = NewWAL(walDir, &WALConfig{Dir: walDir, SyncMode: "immediate"})
		if err != nil {
			_ = base.Close()
			t.Fatalf("wal: %v", err)
		}
		t.Cleanup(func() { _ = wal.Close() })
	}
	tx, err := base.BeginTransaction(ctx, OrdinaryBegin)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.SetNamespace("test"); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if _, err := tx.CreateNode(ctx, &Node{ID: "test:callback-close", Properties: map[string]any{"v": "stored"}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	p, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "zzei2-callback-close"})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	var r l2E53_15Result
	closeDone := make(chan error, 1)
	callbackDone := make(chan struct{})
	cb := func(CommitOutcome) {
		defer close(callbackDone)
		r.callbackRan = true
		r.activeInCallback = base.work.Active()
		start := time.Now()
		go func() { closeDone <- base.Close() }()
		select {
		case err := <-closeDone:
			r.closedInCallback = true
			r.closeErr = err
			r.closeReturned = true
		case <-time.After(l2E53_15CallbackWait):
		}
		r.waitedInCallback = time.Since(start)
	}
	var receipt *CommitReceipt
	if journaled {
		receipt, r.submitErr = NewWALEngine(base, wal).SubmitEntityCommit(ctx, p, cb)
	} else {
		receipt, r.submitErr = base.SubmitEntityCommit(ctx, p, cb)
	}
	if r.submitErr != nil {
		p.Abort()
		_ = base.Close()
		return r
	}
	select {
	case <-callbackDone:
	case <-time.After(30 * time.Second):
		t.Fatalf("settlement callback did not run within 30 s")
	}
	if !r.closeReturned {
		select {
		case err := <-closeDone:
			r.closeErr = err
			r.closeReturned = true
		case <-time.After(30 * time.Second):
			t.Errorf("Close did not return within 30 s after the callback returned")
		}
	}
	if receipt != nil {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		r.outcomeState = receipt.WaitOutcome(wctx).State
		cancel()
	}
	return r
}

func TestL2_e53_15_Repro(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	r := l2E53_15Run(t, true)
	t.Logf("journaled: %s", r)
	if r.submitErr != nil || !r.callbackRan {
		t.Fatalf("setup: submission did not reach its callback: %s", r)
	}
	if !r.closedInCallback || r.waitedInCallback >= l2E53_15CloseFast {
		t.Logf("L2RESULT E53.15 DEFECT callbackWaited=%s closedWhileCallbackWaited=%v workActiveInCallback=%d threshold=%s",
			r.waitedInCallback.Round(time.Millisecond), r.closedInCallback, r.activeInCallback, l2E53_15CloseFast)
		t.Errorf("Close blocked by the journaled commit's settlement callback: %s", r)
		return
	}
	t.Logf("L2RESULT E53.15 OK callbackWaited=%s closedWhileCallbackWaited=%v workActiveInCallback=%d threshold=%s",
		r.waitedInCallback.Round(time.Millisecond), r.closedInCallback, r.activeInCallback, l2E53_15CloseFast)
}

// Control (the entry's): the same commit through base.SubmitEntityCommit,
// without the journal, closes while the callback waits.
func TestL2_e53_15_Control(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	r := l2E53_15Run(t, false)
	t.Logf("not journaled: %s", r)
	if r.submitErr != nil || !r.callbackRan {
		t.Fatalf("control: submission did not reach its callback: %s", r)
	}
	if !r.closedInCallback || r.waitedInCallback >= l2E53_15CloseFast {
		t.Errorf("control: Close did not finish while the callback waited: %s", r)
	}
}

// Cause: wal_entity_commit.go:146-154 (pin) enters the engine's work lifetime
// before launching the native application and leaves it in a deferred call
// that runs after JoinContext, which resolves the receipt and so runs the
// caller's callback; Close waits for all registered work. This logs the
// engine's registered work count seen inside the callback for both routes:
// > 0 on the journaled route (the registration is still held while the
// callback runs), against the unjournaled route.
func TestL2_e53_15_Cause(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	journaled := l2E53_15Run(t, true)
	direct := l2E53_15Run(t, false)
	t.Logf("work.Active() inside the callback: journaled=%d direct=%d; callback waited journaled=%s direct=%s",
		journaled.activeInCallback, direct.activeInCallback,
		journaled.waitedInCallback.Round(time.Millisecond), direct.waitedInCallback.Round(time.Millisecond))
}
