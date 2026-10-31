package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Like a native model, this provider owns a worker independently of the queue.
type shutdownOwnedEmbedder struct {
	countingEmbedder
	state   string
	entered chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	closes  atomic.Int32
}

func (e *shutdownOwnedEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	close(e.entered)
	if e.state == "failed health" {
		return nil, errors.New("unhealthy model")
	}
	if e.state == "health pending" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return []float32{0.1, 0.2, 0.3, 0.4}, nil
}

func (e *shutdownOwnedEmbedder) Close() error {
	e.closes.Add(1)
	e.once.Do(func() { close(e.stop) })
	<-e.done
	return nil
}

func checkEmbedderShutdownOwner(t *testing.T, state string, cacheSize int, stopFirst bool) {
	t.Helper()
	s, _ := setupTestServer(t)
	s.embedderRetryInitialBackoff = time.Hour
	s.embedderRetryMaxBackoff = time.Hour
	warned := make(chan struct{})
	s.log = slog.New(&failedHealthWarningHandler{warned: warned})
	e := &shutdownOwnedEmbedder{
		countingEmbedder: countingEmbedder{dims: 4},
		state:            state,
		entered:          make(chan struct{}),
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
	}
	go func() { defer close(e.done); <-e.stop }()
	t.Cleanup(func() {
		_ = s.Stop(context.Background())
		_ = s.db.Close()
		_ = e.Close() // Also clean up on the unfixed RED path.
	})
	constructing := make(chan struct{})
	s.newEmbedder = func(context.Context, *embed.Config) (embed.Embedder, error) {
		if state == "constructor pending" {
			close(constructing)
			// Model construction may ignore cancellation, then return a model.
			<-s.db.LifetimeContext().Done()
		}
		return e, nil
	}
	s.startEmbedderInit(&embed.Config{Provider: "local", Model: "owned", Dimensions: 4}, cacheSize, nil, s.db)
	var ready <-chan struct{}
	switch state {
	case "healthy":
		ready = s.embedderInit.Done()
	case "failed health":
		ready = warned
	case "health pending":
		ready = e.entered
	case "constructor pending":
		ready = constructing
	}
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("initialization did not reach the requested state")
	}
	if state == "healthy" {
		require.NotNil(t, s.db.GetCypherExecutor().GetEmbedder())
	} else {
		require.Nil(t, s.db.GetCypherExecutor().GetEmbedder())
	}
	if state == "failed health" {
		select {
		case <-e.done:
		default:
			t.Error("failed provider remains alive during retry backoff")
		}
	}
	if stopFirst {
		require.NoError(t, s.Stop(context.Background()))
	}
	// DB.Close owns shutdown, including an initializer not stopped by HTTP.
	require.NoError(t, s.db.Close())
	select {
	case <-s.embedderInit.Done():
	default:
		t.Error("initializer outlived DB.Close")
	}
	select {
	case <-e.done:
	default:
		t.Error("provider worker outlived DB.Close")
	}
	require.EqualValues(t, 1, e.closes.Load(), "each model must have exactly one release")
	require.NoError(t, s.Stop(context.Background()))
	require.NoError(t, s.db.Close())
	require.EqualValues(t, 1, e.closes.Load(), "Stop and repeated Close must not release twice")
}

func TestEmbedderShutdownOwnsEveryState(t *testing.T) {
	for _, state := range []string{"healthy", "failed health", "health pending", "constructor pending"} {
		for _, cacheSize := range []int{0, 16} {
			t.Run(fmt.Sprintf("%s/cache%d", state, cacheSize), func(t *testing.T) {
				checkEmbedderShutdownOwner(t, state, cacheSize, false)
			})
		}
	}
}

func TestEmbedderShutdownAfterServerStop(t *testing.T) {
	for _, cacheSize := range []int{0, 16} {
		t.Run(fmt.Sprintf("cache%d", cacheSize), func(t *testing.T) {
			checkEmbedderShutdownOwner(t, "healthy", cacheSize, true)
		})
	}
}

func shutdownGoroutines() map[uint64]string {
	buf := make([]byte, 4<<20)
	n := runtime.Stack(buf, true)
	stacks := make(map[uint64]string)
	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		var id uint64
		if _, err := fmt.Sscanf(stack, "goroutine %d ", &id); err == nil {
			stacks[id] = stack
		}
	}
	return stacks
}

func TestEmbedderShutdownLeavesNoGoroutines(t *testing.T) {
	// Warm global storage singletons before the census, including the real model
	// worker surrogate. Do not use Eventually: its predicate is a new goroutine.
	checkEmbedderShutdownOwner(t, "healthy", 16, false)
	before := shutdownGoroutines()
	for _, state := range []string{"healthy", "failed health", "health pending", "constructor pending"} {
		checkEmbedderShutdownOwner(t, state, 16, false)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var remaining []string
		for id, stack := range shutdownGoroutines() {
			if _, existed := before[id]; !existed {
				remaining = append(remaining, stack)
			}
		}
		if len(remaining) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines survived Close: %s", strings.Join(remaining, "\n\n"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
