package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func onceContractReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("native completion did not make progress")
	}
	var zero T
	return zero
}

func TestOnceCompletionKeepsCapturedRequestAndCallerFrame(t *testing.T) {
	type key struct{}
	request, cancel := context.WithCancel(context.WithValue(t.Context(), key{}, "request"))
	cancel()
	cause := errors.New("native outcome")
	var c OnceCompletion
	require.ErrorIs(t, c.Do(func() error {
		require.Equal(t, "request", request.Value(key{}))
		require.ErrorIs(t, request.Err(), context.Canceled)
		return cause
	}), cause)
	require.ErrorIs(t, c.Do(func() error { t.Error("operation replayed"); return nil }), cause)
	panicValue := errors.New("caller-frame panic")
	var panicking OnceCompletion
	require.PanicsWithValue(t, panicValue, func() { _ = panicking.Do(func() error { panic(panicValue) }) })
	require.ErrorContains(t, panicking.Do(func() error { t.Error("panicking operation replayed"); return nil }), panicValue.Error())
	require.Panics(t, func() { panicking.Run(func() { t.Error("Run replayed failed operation") }) })
	var run OnceCompletion
	calls := 0
	run.Run(func() { calls++ })
	run.Run(func() { calls++ })
	require.Equal(t, 1, calls)
}

func TestOnceCompletionConcurrentCallersJoinSameNativeOutcome(t *testing.T) {
	var c OnceCompletion
	entered, release := make(chan struct{}), make(chan struct{})
	var open sync.Once
	finish := func() { open.Do(func() { close(release) }) }
	t.Cleanup(finish)
	cause := errors.New("shared outcome")
	var calls atomic.Int64
	first := make(chan error, 1)
	go func() { first <- c.Do(func() error { calls.Add(1); close(entered); <-release; return cause }) }()
	onceContractReceive(t, entered)
	const losers = 3
	results := make(chan error, losers)
	var started sync.WaitGroup
	started.Add(losers)
	for i := 0; i < losers; i++ {
		go func() { started.Done(); results <- c.Do(func() error { calls.Add(1); return nil }) }()
	}
	started.Wait()
	select {
	case err := <-results:
		t.Fatalf("join returned before native operation: %v", err)
	default:
	}
	finish()
	require.ErrorIs(t, onceContractReceive(t, first), cause)
	for i := 0; i < losers; i++ {
		require.ErrorIs(t, onceContractReceive(t, results), cause)
	}
	require.Equal(t, int64(1), calls.Load())
	require.ErrorIs(t, c.Err(), cause)
}

func TestOnceCompletionWaitCancellationDoesNotReplaceOperationLifetime(t *testing.T) {
	type key struct{}
	work := context.WithValue(t.Context(), key{}, "owned work")
	wait, cancel := context.WithCancel(t.Context())
	defer cancel()
	var c OnceCompletion
	entered, release := make(chan struct{}), make(chan struct{})
	var open sync.Once
	finish := func() { open.Do(func() { close(release) }) }
	t.Cleanup(finish)
	cause := errors.New("completed after caller left")
	observed := make(chan error, 1)
	waited := make(chan error, 1)
	go func() {
		waited <- c.DoContext(wait, func() error { close(entered); <-release; observed <- work.Err(); return cause })
	}()
	onceContractReceive(t, entered)
	cancel()
	require.ErrorIs(t, onceContractReceive(t, waited), context.Canceled)
	require.Equal(t, "owned work", work.Value(key{}))
	select {
	case <-c.Done():
		t.Fatal("caller cancellation published operation completion")
	default:
	}
	finish()
	onceContractReceive(t, c.Done())
	require.NoError(t, onceContractReceive(t, observed))
	require.ErrorIs(t, c.Err(), cause)
	require.ErrorIs(t, c.Do(func() error { t.Error("operation replayed after canceled wait"); return nil }), cause)
	require.Equal(t, c.Done(), c.Go(func() error { t.Error("Go replayed completed operation"); return nil }))
	var untouched OnceCompletion
	require.Panics(t, func() {
		_ = untouched.DoContext(nil, func() error { t.Error("nil input dispatched operation"); return nil })
	})
	require.Nil(t, untouched.done, "invalid input cannot initialize or dispatch completion")
}

func TestOnceCompletionGoReturnsBeforeNativeCompletion(t *testing.T) {
	var c OnceCompletion
	entered, release := make(chan struct{}), make(chan struct{})
	var open sync.Once
	finish := func() { open.Do(func() { close(release) }) }
	t.Cleanup(finish)
	cause := errors.New("async native outcome")
	var calls atomic.Int64
	returned := make(chan (<-chan struct{}), 1)
	go func() { returned <- c.Go(func() error { calls.Add(1); close(entered); <-release; return cause }) }()
	onceContractReceive(t, entered)
	var done <-chan struct{}
	select {
	case done = <-returned:
	case <-time.After(time.Second):
		t.Fatal("Go blocked its caller on native completion")
	}
	require.Equal(t, done, c.Go(func() error { calls.Add(1); return nil }))
	select {
	case <-done:
		t.Fatal("Go completion published while operation held")
	default:
	}
	finish()
	onceContractReceive(t, done)
	require.Equal(t, int64(1), calls.Load())
	require.ErrorIs(t, c.Err(), cause)
}
