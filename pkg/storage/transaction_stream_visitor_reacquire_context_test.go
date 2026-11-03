package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A transaction stream hands each record to the caller's visitor with the
// transaction's lock released, then retakes that lock. The retake carries the
// caller's context like admission does, so a request canceled while its visitor
// ran ends instead of queueing behind an unrelated holder.
//
// Each case below holds the actual tx.mu after the visitor was entered and
// before it returns, so the retake after that callback must wait: the visitor
// cannot return until this test holds the lock. The wait is taken from the
// admission observer at tx.mu, which the retake reports with the context it
// waits on, and the cancellation follows it, so the bounded return is measured
// for a stream that is already waiting. No further callback runs, the holder
// keeps its hold (the stopped stream released nothing it no longer owned), the
// stopped stream left no hold of its own behind, and the same transaction still
// serves a healthy caller.
type transactionStreamVisitorCase struct {
	name string
	// visits is how many records the stream delivers for the fixture, so a
	// stopped stream can prove it ran no further callback.
	visits int
	// stop is what this stream returns to its caller when its visitor ends
	// iteration and the stream owns the lock again: its early-stop sentinel
	// unchanged, or nil where the stream treats that sentinel as success.
	stop error
	run  func(ctx context.Context, tx *BadgerTransaction, visit func() error) error
}

func transactionStreamVisitorCases() []transactionStreamVisitorCase {
	return []transactionStreamVisitorCase{
		{
			name:   "StreamNodesByLabelProjected",
			visits: 2,
			stop:   ErrIterationStopped,
			run: func(ctx context.Context, tx *BadgerTransaction, visit func() error) error {
				return tx.StreamNodesByLabelProjected(ctx, "Leaf", nil, func(*Node) error {
					return visit()
				})
			},
		},
		{
			name:   "StreamNodesWithOptions",
			visits: 3,
			stop:   nil,
			run: func(ctx context.Context, tx *BadgerTransaction, visit func() error) error {
				return tx.StreamNodesWithOptions(ctx, StreamNodesOptions{Label: "Hub"}, func(*Node) error {
					return visit()
				})
			},
		},
		{
			name:   "StreamEdgesByType",
			visits: 2,
			stop:   ErrIterationStopped,
			run: func(ctx context.Context, tx *BadgerTransaction, visit func() error) error {
				return tx.StreamEdgesByType(ctx, "R", func(*Edge) error {
					return visit()
				})
			},
		},
	}
}

// beginStreamVisitorTransaction starts the transaction one case drives and rolls
// it back after the case.
func beginStreamVisitorTransaction(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// streamLeftNoHold reports whether the transaction's lock is free, leaving it as
// it found it, so a case can prove its stream left no hold of its own behind.
func streamLeftNoHold(tx *BadgerTransaction) bool {
	if tx.mu.TryLock() {
		tx.mu.Unlock()
		return true
	}
	return false
}

func TestTransactionStreamsStopWhenTheirContextEndsDuringTheVisitor(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	_ = seedIncidentContextGraph(t, engine, 2)

	for _, stream := range transactionStreamVisitorCases() {
		t.Run(stream.name, func(t *testing.T) {
			tx := beginStreamVisitorTransaction(t, engine)

			healthy := 0
			require.NoError(t, stream.run(t.Context(), tx, func() error {
				healthy++
				return nil
			}), "healthy control streams the population")
			require.Equal(t, stream.visits, healthy, "healthy control visits every record")

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// Admission ran before this test held the lock, so a wait reported
			// at tx.mu here is the retake after the visitor.
			waited := make(chan context.Context, 8)
			restore := setAdmissionWaitObserver(func(owner any, observed context.Context) {
				if owner != &tx.mu {
					return
				}
				select {
				case waited <- observed:
				default:
				}
			})
			t.Cleanup(restore)

			visitStarted := make(chan struct{})
			var allowReturn sync.Once
			visitorMayReturn := make(chan struct{})
			var visits atomic.Int32
			done := make(chan error, 1)
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				done <- stream.run(ctx, tx, func() error {
					if visits.Add(1) > 1 {
						return nil
					}
					close(visitStarted)
					<-visitorMayReturn
					return nil
				})
			}()

			holding := false
			releaseHolder := func() {
				if holding {
					holding = false
					tx.mu.Unlock()
				}
			}
			t.Cleanup(func() {
				cancel()
				allowReturn.Do(func() { close(visitorMayReturn) })
				releaseHolder()
				select {
				case <-joined:
				case <-time.After(10 * time.Second):
					t.Error("the stream goroutine did not join after its lock holder released")
				}
			})

			select {
			case <-visitStarted:
			case <-time.After(10 * time.Second):
				t.Fatal("the stream never entered its visitor")
			}

			// The visitor runs with tx.mu released: take it, then let the
			// visitor return so the stream reaches its retake and waits there.
			tx.mu.Lock()
			holding = true
			allowReturn.Do(func() { close(visitorMayReturn) })
			var observed context.Context
			select {
			case observed = <-waited:
			case <-time.After(2 * time.Second):
			}
			// Cancel the request while the stream is waiting for the held lock.
			// It must report the cancellation rather than wait for this hold.
			cancel()
			started := time.Now()
			var runErr error
			select {
			case runErr = <-done:
			case <-time.After(100 * time.Millisecond):
				elapsed := time.Since(started)
				releaseHolder()
				select {
				case runErr = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("the canceled stream did not return after its lock holder released")
				}
				t.Fatalf("a canceled stream waited %v for the lock holder instead of returning within 100ms before the holder released it (returned %v)", elapsed, runErr)
			}
			elapsed := time.Since(started)
			<-joined
			require.Less(t, elapsed, 100*time.Millisecond,
				"a canceled stream returns without waiting for the lock holder")
			require.ErrorIs(t, runErr, context.Canceled, "the stream reports the request's cancellation")
			require.Equal(t, int32(1), visits.Load(), "a canceled stream runs no further callback")
			require.True(t, observed == ctx, "the post-visitor retake waits on the caller's context")

			// This test still holds the lock: the stopped stream must not have
			// released a hold it no longer owned.
			require.False(t, tx.mu.TryLock(), "the stopped stream released the holder's lock")

			releaseHolder()
			require.True(t, tx.mu.TryLock(), "the canceled stream left no hold behind")
			tx.mu.Unlock()

			retried := 0
			require.NoError(t, stream.run(t.Context(), tx, func() error {
				retried++
				return nil
			}), "the canceled stream leaves the transaction healthy")
			require.Equal(t, stream.visits, retried, "the healthy stream still serves every record")
		})
	}
}

// A stream that loses its post-visitor retake must report the visitor's outcome
// and the reason it lost the retake. The visitor's own error keeps its identity,
// the cancellation that ended the retake keeps its own, and neither a callback
// error is swallowed. Intentional ErrIterationStopped is different: the visitor
// has completed its requested prefix, so canceling that visitor's own context
// does not turn its existing early-stop contract into a failure. Transaction
// closure remains an error; ordinary cancellation without an early stop does too.
func TestTransactionStreamsKeepTheVisitorOutcomeAndTheCancellation(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	_ = seedIncidentContextGraph(t, engine, 2)

	callbackErr := errors.New("visitor failed")
	streams := transactionStreamVisitorCases()

	cases := []struct {
		name string
		// visit is what the visitor does on the one record it sees.
		visit func(cancel context.CancelFunc) error
		check func(t *testing.T, stream transactionStreamVisitorCase, err error)
	}{
		{
			name: "callback error",
			visit: func(cancel context.CancelFunc) error {
				cancel()
				return callbackErr
			},
			check: func(t *testing.T, _ transactionStreamVisitorCase, err error) {
				require.ErrorIs(t, err, callbackErr, "the visitor's error keeps its identity")
				require.ErrorIs(t, err, context.Canceled, "the cancellation that ended the retake is reported too")
			},
		},
		{
			name: "early stop",
			visit: func(cancel context.CancelFunc) error {
				cancel()
				return ErrIterationStopped
			},
			check: func(t *testing.T, stream transactionStreamVisitorCase, err error) {
				require.Equal(t, stream.stop, err, "intentional stop keeps the existing per-stream completion contract")
			},
		},
		{
			name: "healthy early stop",
			visit: func(context.CancelFunc) error {
				return ErrIterationStopped
			},
			check: func(t *testing.T, stream transactionStreamVisitorCase, err error) {
				if stream.stop == nil {
					require.NoError(t, err, "a stream that owns its hold keeps its early stop a success")
					return
				}
				require.Equal(t, stream.stop, err, "a stream that owns its hold returns its stop sentinel unchanged")
			},
		},
	}

	for _, stream := range streams {
		for _, testCase := range cases {
			t.Run(stream.name+"/"+testCase.name, func(t *testing.T) {
				tx := beginStreamVisitorTransaction(t, engine)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				visits := 0
				err := stream.run(ctx, tx, func() error {
					visits++
					return testCase.visit(cancel)
				})
				testCase.check(t, stream, err)
				require.Equal(t, 1, visits, "a stream that lost its retake runs no further callback")
				require.True(t, streamLeftNoHold(tx), "the stream left no hold behind")

				served := 0
				require.NoError(t, stream.run(t.Context(), tx, func() error {
					served++
					return nil
				}), "the stream leaves the transaction healthy")
				require.Equal(t, stream.visits, served, "the transaction still serves every record")
			})
		}
	}
}

// streamRetakeHold is one stream case run while the test holds tx.mu from the
// moment its first visitor is entered: the visitor returns only after the hold
// is this test's, so the stream's post-visitor retake must deal with a lock it
// does not own.
type streamRetakeHold struct {
	runErr  error
	visits  int32
	elapsed time.Duration
	// waited is true when the stream returned only after this test released the
	// lock, so it waited for that hold instead of ending on its own.
	waited bool
}

// runStreamHeldDuringRetake drives one case that way. afterVisit runs inside the
// first visitor just before it returns, which is where a case releases the
// client, closes the transaction or cancels the request.
func runStreamHeldDuringRetake(t *testing.T, tx *BadgerTransaction, stream transactionStreamVisitorCase, ctx context.Context, afterVisit func()) streamRetakeHold {
	t.Helper()
	holding := false
	releaseHolder := func() {
		if holding {
			holding = false
			tx.mu.Unlock()
		}
	}
	visitStarted := make(chan struct{})
	var allowReturn sync.Once
	visitorMayReturn := make(chan struct{})
	var visits atomic.Int32
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		done <- stream.run(ctx, tx, func() error {
			if visits.Add(1) > 1 {
				return nil
			}
			close(visitStarted)
			<-visitorMayReturn
			afterVisit()
			return nil
		})
	}()
	t.Cleanup(func() {
		allowReturn.Do(func() { close(visitorMayReturn) })
		releaseHolder()
		select {
		case <-joined:
		case <-time.After(10 * time.Second):
			t.Error("the stream goroutine did not join after its lock holder released")
		}
	})

	select {
	case <-visitStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream never entered its visitor")
	}
	tx.mu.Lock()
	holding = true
	allowReturn.Do(func() { close(visitorMayReturn) })

	started := time.Now()
	var hold streamRetakeHold
	select {
	case hold.runErr = <-done:
		hold.elapsed = time.Since(started)
	case <-time.After(100 * time.Millisecond):
		hold.waited = true
		releaseHolder()
		select {
		case hold.runErr = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the stream did not return after its lock holder released")
		}
	}
	<-joined
	hold.visits = visits.Load()
	releaseHolder()
	return hold
}

// A transaction the client closed or detached while its visitor ran must be
// reported, and no further record may be delivered. The detached cases prove the
// report comes before the retake waits: this test holds tx.mu from the moment the
// visitor is entered until after the stream returns, so a stream that waited for
// the lock instead of reporting the closure blocks here, and one that reported
// the request's cancellation instead lost the closure's precedence.
func TestTransactionStreamsReportACloseDuringTheVisitor(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	_ = seedIncidentContextGraph(t, engine, 2)

	detached := []struct {
		name  string
		visit func(tx *BadgerTransaction, cancel context.CancelFunc)
		check func(t *testing.T, err error)
	}{
		{
			name: "detached",
			visit: func(tx *BadgerTransaction, _ context.CancelFunc) {
				tx.clientDetached.Store(true)
			},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrTransactionClosed, "the stream reports the transaction's closure")
			},
		},
		{
			name: "detached and canceled",
			visit: func(tx *BadgerTransaction, cancel context.CancelFunc) {
				tx.clientDetached.Store(true)
				cancel()
			},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrTransactionClosed, "the stream reports the transaction's closure")
				require.NotErrorIs(t, err, context.Canceled,
					"the transaction's closure keeps its precedence over the request's cancellation")
			},
		},
	}

	for _, stream := range transactionStreamVisitorCases() {
		for _, testCase := range detached {
			t.Run(stream.name+"/"+testCase.name, func(t *testing.T) {
				tx := beginStreamVisitorTransaction(t, engine)
				// The released client is restored so the case's rollback can run.
				t.Cleanup(func() { tx.clientDetached.Store(false) })
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				hold := runStreamHeldDuringRetake(t, tx, stream, ctx, func() {
					testCase.visit(tx, cancel)
				})
				require.False(t, hold.waited,
					"a stream whose client released the transaction reports the closure without waiting for the lock holder")
				testCase.check(t, hold.runErr)
				require.Equal(t, int32(1), hold.visits, "a closed transaction receives no further callback")
				require.True(t, streamLeftNoHold(tx), "the stream left no hold behind")
			})
		}

		t.Run(stream.name+"/closed", func(t *testing.T) {
			tx := beginStreamVisitorTransaction(t, engine)

			visits := 0
			err := stream.run(t.Context(), tx, func() error {
				visits++
				// The transaction is closed by this visitor, so the retake
				// reowns the lock of a transaction that is no longer active.
				return tx.Rollback()
			})
			require.ErrorIs(t, err, ErrTransactionClosed, "the stream reports the closed transaction")
			require.Equal(t, 1, visits, "a closed transaction receives no further callback")
			require.True(t, streamLeftNoHold(tx), "the stream left no hold behind")
		})
	}
}

// A visitor that panics keeps its own panic, and the stream's deferred release
// must not release a lock it no longer holds: it releases the lock before the
// visitor runs, so its ownership flag has to be accurate before that call. The
// panic below is a real panic taken in the visitor, and the operation holding
// tx.mu while it happens stands for any concurrent transaction work; their hold
// must survive the unwind. The panic is recovered here, never in the stream.
func TestTransactionStreamVisitorPanicReleasesOnlyItsOwnHold(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	_ = seedIncidentContextGraph(t, engine, 2)

	for _, stream := range transactionStreamVisitorCases() {
		t.Run(stream.name, func(t *testing.T) {
			tx := beginStreamVisitorTransaction(t, engine)
			panicValue := &struct{ stream string }{stream: stream.name}

			holding := false
			releaseHolder := func() {
				if holding {
					holding = false
					tx.mu.Unlock()
				}
			}
			visitStarted := make(chan struct{})
			var allowPanic sync.Once
			visitorMayPanic := make(chan struct{})
			var visits atomic.Int32
			var recovered any
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				defer func() { recovered = recover() }()
				_ = stream.run(t.Context(), tx, func() error {
					if visits.Add(1) > 1 {
						return nil
					}
					close(visitStarted)
					<-visitorMayPanic
					panic(panicValue)
				})
			}()
			t.Cleanup(func() {
				allowPanic.Do(func() { close(visitorMayPanic) })
				releaseHolder()
				select {
				case <-joined:
				case <-time.After(10 * time.Second):
					t.Error("the panicking stream goroutine did not join")
				}
			})

			select {
			case <-visitStarted:
			case <-time.After(10 * time.Second):
				t.Fatal("the stream never entered its visitor")
			}
			tx.mu.Lock()
			holding = true
			allowPanic.Do(func() { close(visitorMayPanic) })
			select {
			case <-joined:
			case <-time.After(10 * time.Second):
				t.Fatal("the panicking stream did not unwind")
			}

			require.Same(t, panicValue, recovered, "the visitor's panic reaches its caller unchanged")
			require.Equal(t, int32(1), visits.Load(), "the panicking visitor ran once")

			// The stream released the lock before the visitor ran, so its
			// deferred release had nothing of its own to release: this hold
			// must have survived the panic.
			survived := !tx.mu.TryLock()
			if !survived {
				// The stream released this hold. Give it back so cleanup does
				// not release a hold it no longer has.
				tx.mu.Unlock()
				holding = false
			}
			require.True(t, survived, "the panicking stream released the holder's lock")

			releaseHolder()
			require.True(t, tx.mu.TryLock(), "the panicking stream left no hold behind")
			tx.mu.Unlock()

			served := 0
			require.NoError(t, stream.run(t.Context(), tx, func() error {
				served++
				return nil
			}), "the transaction still serves a stream after a visitor panicked")
			require.Equal(t, stream.visits, served, "the transaction still serves every record")
		})
	}
}

// A visitor can stop its prefix and cancel its own context. Completion must not
// wait for a foreign holder or turn that intentional stop into cancellation.
func TestTransactionStreamsIntentionalStopDoesNotWaitForCanceledRetake(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	_ = seedIncidentContextGraph(t, engine, 2)
	for _, original := range transactionStreamVisitorCases() {
		t.Run(original.name, func(t *testing.T) {
			tx := beginStreamVisitorTransaction(t, engine)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := original
			stream.run = func(ctx context.Context, tx *BadgerTransaction, visit func() error) error {
				return original.run(ctx, tx, func() error {
					if err := visit(); err != nil {
						return err
					}
					return ErrIterationStopped
				})
			}
			got := runStreamHeldDuringRetake(t, tx, stream, ctx, cancel)
			require.Equal(t, original.stop, got.runErr)
			require.Equal(t, int32(1), got.visits)
			require.False(t, got.waited, "intentional stop must not wait for the foreign lock")
			require.True(t, streamLeftNoHold(tx))
		})
	}
}
