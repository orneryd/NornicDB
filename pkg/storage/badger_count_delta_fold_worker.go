package storage

import (
	"context"
	"sync"
)

// countDeltaFoldBackgroundThreshold is how many appended derived-count delta
// keys make a background fold due. Appending deltas is a blind unique-key
// write (no mutex), so a pure-write load can accumulate millions of them
// before the first count read; that first read used to fold them inline,
// stalling for seconds (PR #1043 retest @39f0f03f, issue B). A background
// fold triggered by append volume keeps the backlog bounded without ever
// running on the read path.
const countDeltaFoldBackgroundThreshold = 50_000

// countDeltaFoldWorker folds accumulated count deltas into their base keys
// from a single background goroutine. Folds are serialized: at most one runs
// at a time, and appends continue to land while it runs (a later fold catches
// the remainder).
type countDeltaFoldWorker struct {
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// signal wakes the fold worker once a fold is due.
func (w *countDeltaFoldWorker) signal() {
	if w == nil {
		return
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// recordCountDeltaAppend counts one appended delta key and wakes the worker
// once the background threshold is crossed. The watermark resets at the
// crossing, so a fold that misses appends landing while it runs is followed by
// another fold once those appends cross the threshold again.
func (b *BadgerEngine) recordCountDeltaAppend() {
	if b == nil || b.countFold == nil {
		return
	}
	if b.countDeltaPending.Add(1) >= countDeltaFoldBackgroundThreshold {
		b.countDeltaPending.Store(0)
		b.countFold.signal()
	}
}

// startCountDeltaFoldWorker starts the background fold goroutine of an
// on-disk engine.
func (b *BadgerEngine) startCountDeltaFoldWorker() {
	w := &countDeltaFoldWorker{
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	b.countFold = w
	go func() {
		defer close(w.done)
		for {
			select {
			case <-w.stop:
				return
			case <-w.wake:
			}
			b.foldCountDeltasInBackground()
		}
	}()
}

// foldCountDeltasInBackground folds every persisted label and edge-type delta
// into its base key, chunked, in bounded transactions. Appends that land
// while the fold runs are not in its snapshot; they remain and the next
// threshold crossing folds them.
func (b *BadgerEngine) foldCountDeltasInBackground() {
	release, err := b.beginNamespaceMutation(context.Background(), nil, nil, false, namespacePublicationFresh)
	if err != nil { return }
	defer release()
	if err := b.foldAllLabelCountDeltas(); err != nil && b.log != nil {
		b.log.Warn("background label-count delta fold failed",
			"subsystem", "storage", "error", err)
	}
	if err := b.foldAllEdgeTypeCountDeltas(); err != nil && b.log != nil {
		b.log.Warn("background edge-type-count delta fold failed",
			"subsystem", "storage", "error", err)
	}
}

// stopCountDeltaFoldWorker stops the background fold goroutine and waits for
// it, including a fold in progress. Close calls it before taking the write
// barrier.
func (b *BadgerEngine) stopCountDeltaFoldWorker() {
	w := b.countFold
	if w == nil {
		return
	}
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
}
