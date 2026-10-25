package storage

// Write-behind generation lifecycle.
//
// One owner moves every generation through these states. Each resource a
// generation, or a caller ordered against it, holds is taken and released at
// one named transition:
//
//   - admitted: a buffered commit's targets are claimed and its MVCC version is
//     in flight (admitBufferedCommit, under the buffer lock). Refused once
//     admission is closed (closeAdmission).
//   - durable: its WAL record is written before any acknowledgement
//     (commitBufferedLocked).
//   - acknowledged: it is appended and visible; its claims and in-flight
//     version are released in the same critical section (appendReservedCommit).
//   - rotated, applying: its generation leaves the active slot (rotate) and
//     one applier claims it (claimDrain).
//   - applied: data, counts and caches are in Badger and its listener
//     notification is queued but held (replayWriteBehind).
//   - retired: it leaves the draining queue exactly once and its notification
//     is released (retire, settle); it stays in the frontier.
//   - consumed: the dispatcher delivered the notification; the generation
//     leaves the frontier.
//
// Flush and Close complete only once every generation acknowledged before
// them is consumed. A listener never runs on a goroutine that holds the
// write-behind order, a unique-key lease or a database admission lease.
// Producers reserve actual events and mark publication complete after their
// leases end. Retirement readies the event; only the dispatcher consumes it.

import (
	"bytes"
	"errors"
	"runtime"
	"strconv"
	"time"
)

// errWriteBehindAdmissionClosed refuses a buffered commit once Close has
// started its final drain: an acknowledgement after it would be lost.
var errWriteBehindAdmissionClosed = errors.New("write-behind admission closed")

// closeAdmission refuses every buffered commit not yet admitted. Commits
// already admitted finish; the caller waits for them before its final drain.
func (w *WriteBehindBuffer) closeAdmission() {
	w.mu.Lock()
	w.admissionClosed = true
	w.mu.Unlock()
}

// removeDrained retires an applied generation and delivers its listener
// notification.
func (w *WriteBehindBuffer) removeDrained(buf *CommitBuffer) {
	w.retire(buf, true)
}

// retire is the one settlement of an applied generation, whichever path
// applied it (a drain, Flush, an external applier) and whichever retires it:
// it leaves the draining queue exactly once, then its held listener
// notification is released (settle). Release never waits for the applier, so
// a generation retired before its external applier gives up its claim never
// blocks a flush.
func (w *WriteBehindBuffer) retire(buf *CommitBuffer, deliver bool) {
	if w.dequeueApplied(buf) {
		w.settle(buf, deliver)
	}
}

// settle records retirement once. Publication may finish before or after it;
// the final transition wakes the single dispatcher. Neither runs callbacks.
func (w *WriteBehindBuffer) settle(buf *CommitBuffer, deliver bool) {
	buf.settleOnce.Do(func() {
		if w.release != nil {
			for _, c := range buf.commits {
				w.release(c.notification, deliver)
			}
			w.release(buf.publication, deliver)
		}
	})
}

// delivered is closed once the generation's listeners have run; nil when it
// queued none.
func (b *CommitBuffer) delivered() <-chan struct{} {
	if b.publication == nil {
		return nil
	}
	return b.publication.consumed
}

// consumed reports whether the generation's listeners have run.
func (b *CommitBuffer) consumed() bool {
	delivered := b.delivered()
	if delivered == nil {
		return true
	}
	select {
	case <-delivered:
		return true
	default:
		return false
	}
}

// enterFrontierLocked keeps a just-retired generation until its notification
// is consumed, and drops the consumed ones. Caller holds w.mu.
func (w *WriteBehindBuffer) enterFrontierLocked(buf *CommitBuffer) {
	kept := w.frontier[:0]
	for _, g := range w.frontier {
		if !g.consumed() {
			kept = append(kept, g)
		}
	}
	clear(w.frontier[len(kept):])
	w.frontier = kept
	if buf.publication != nil {
		w.frontier = append(w.frontier, buf)
	}
}

// captureLocked returns every generation acknowledged so far and not yet
// consumed: those waiting to be applied and those retired whose listeners
// have not run. Caller holds w.mu.
func (w *WriteBehindBuffer) captureLocked() []*CommitBuffer {
	captured := make([]*CommitBuffer, 0, len(w.draining)+len(w.frontier))
	captured = append(captured, w.draining...)
	for _, g := range w.frontier {
		if !g.consumed() {
			captured = append(captured, g)
		}
	}
	return captured
}

// Flush rotates the active generation, drains every generation and waits
// until the listeners of every generation acknowledged before it have run,
// whoever applied, retired or delivers them. It retries failed applies and
// waits for a generation another drain is applying; if no generation makes
// progress for ~2s after an apply failed, it returns that error. Called from
// a listener, it returns once the generations are retired: their delivery is
// queued behind the listener running it.
func (w *WriteBehindBuffer) Flush() error {
	return w.flush(true)
}

// flush lands a fixed generation frontier. Only an explicit external barrier
// passes deliver=true to await consumption; internal landers never wait.
func (w *WriteBehindBuffer) flush(deliver bool) error {
	var captured []*CommitBuffer
	w.mu.Lock()
	w.rotateLocked()
	var last *CommitBuffer
	if len(w.draining) > 0 {
		last = w.draining[len(w.draining)-1]
	}
	if deliver {
		captured = w.captureLocked()
	}
	w.mu.Unlock()
	w.signal()
	lastPending := -1
	stalls := 0
	for last != nil {
		w.mu.RLock()
		present := false
		for _, g := range w.draining {
			if g == last {
				present = true
				break
			}
		}
		w.mu.RUnlock()
		if !present {
			break
		}
		w.drainOneFor(false)
		w.mu.RLock()
		pending := len(w.draining)
		w.mu.RUnlock()
		if pending == 0 {
			break
		}
		if pending == lastPending {
			stalls++
		} else {
			stalls = 0
		}
		lastPending = pending
		if stalls > 2000 {
			if err := w.LastErr(); err != nil {
				return err
			}
			// Another drain may still be applying a claimed generation:
			// without an apply error, its absence is not completion.
			stalls = 0
		}
		time.Sleep(time.Millisecond)
	}
	if deliver && w.isDispatcher != nil && w.isDispatcher() {
		return nil
	}
	for _, buf := range captured {
		if delivered := buf.delivered(); delivered != nil {
			<-delivered
		}
	}
	return nil
}

// LandWriteBehind applies every acknowledged generation to Badger without
// delivering their listeners on the calling goroutine: for a caller that
// holds a lock a listener's write-back may need. Their listeners run on
// another goroutine, in order, never under the caller's lock. It is a no-op
// when write-behind is disabled.
func (b *BadgerEngine) LandWriteBehind() error {
	if b.writeBehind == nil {
		return nil
	}
	if err := b.writeBehind.flush(false); err != nil {
		return err
	}
	return nil
}

// currentGoroutineID identifies the calling goroutine. Only the notification
// dispatcher and a flush use it, to recognise a flush its own listener runs.
func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	field := bytes.TrimPrefix(buf[:n], []byte("goroutine "))
	if i := bytes.IndexByte(field, ' '); i > 0 {
		field = field[:i]
	}
	id, _ := strconv.ParseUint(string(field), 10, 64)
	return id
}
