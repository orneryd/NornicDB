package cypher

import (
	"context"
	"errors"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A boundary recovers only the owner's stop: it returns the stop's error and
// no value. Any other panic continues unchanged, with its value and with the
// stack of the frame that raised it.
func TestStopBoundaryRecoversOnlyTheOwnersStop(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	value, err := insideStopBoundary(func() ([]interface{}, error) {
		pass := bulk.NewCheckpoint(canceled)
		out := []interface{}{}
		for i := 0; i < 4; i++ {
			pass.Check()
			out = append(out, i)
		}
		return out, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, value, "a stopped pass returns no partial value")

	var recovered interface{}
	var stack string
	func() {
		defer func() {
			recovered = recover()
			stack = string(debug.Stack())
		}()
		_, _ = insideStopBoundary(func() (int, error) {
			stopBoundaryForeignPanic()
			return 0, nil
		})
	}()
	require.Equal(t, "foreign failure", recovered, "a foreign panic keeps its value")
	require.True(t, strings.Contains(stack, "stopBoundaryForeignPanic"), "a foreign panic keeps the raising frame's stack:\n%s", stack)
}

//go:noinline
func stopBoundaryForeignPanic() {
	panic("foreign failure")
}

// The owner's stop cannot cross goroutines: a worker recovers its own and
// hands the error to the goroutine waiting for it.
func TestStopBoundaryInGoroutine(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		var stopped error
		defer func() { done <- stopped }()
		defer bulk.RecoverStop(&stopped)
		pass := bulk.NewCheckpoint(canceled)
		pass.Check()
	}()
	require.ErrorIs(t, <-done, context.Canceled)
}

// A list or string function cancelled while it feeds a write leaves nothing
// written: the statement fails with the cancellation or deadline error, no
// node is created, and no property is set.
func TestCancelledFunctionFeedingWritePersistsNothing(t *testing.T) {
	values := make([]interface{}, 1<<20)
	for i := range values {
		values[i] = int64(len(values) - i)
	}
	text := strings.Repeat("a", 1<<23)
	cases := []struct {
		name, query, target string
		params              map[string]interface{}
	}{
		{"create_list", "CREATE (:Written {v: coll.distinct($values)})", ".fnCollDistinct", map[string]interface{}{"values": values}},
		{"set_list", "MATCH (n:Seed) SET n.v = coll.sort($values)", "/bulk.SliceStable", map[string]interface{}{"values": values}},
		{"create_string", "CREATE (:Written {t: replace($text, 'a', 'bc')})", "/bulk.Replace", map[string]interface{}{"text": text}},
		{"set_string", "MATCH (n:Seed) SET n.t = split($text, 'a')", "/bulk.SplitN", map[string]interface{}{"text": text}},
	}
	for _, test := range cases {
		for _, deadline := range []bool{false, true} {
			mode := "cancel"
			if deadline {
				mode = "deadline"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r9write"), 0, 0)
				_, err := executor.Execute(context.Background(), "CREATE (:Seed {id: 1})", nil)
				require.NoError(t, err)
				ctx := &r8ArmedContext{Context: context.Background(), done: make(chan struct{})}
				finished := make(chan error, 1)
				go func() {
					_, err := executor.Execute(ctx, test.query, test.params)
					finished <- err
				}()
				limit := time.Now().Add(30 * time.Second)
				for !r8HasFrame(test.target) {
					select {
					case err := <-finished:
						t.Fatalf("target %s not reached: %v", test.target, err)
					default:
					}
					if time.Now().After(limit) {
						ctx.arm(false)
						<-finished
						t.Fatal("target not reached in 30s")
					}
					runtime.Gosched()
				}
				ctx.arm(deadline)
				err = <-finished
				want := error(context.Canceled)
				if deadline {
					want = context.DeadlineExceeded
				}
				require.True(t, errors.Is(err, want), "outcome %v, want %v", err, want)

				result, err := executor.Execute(context.Background(), "MATCH (n:Written) RETURN count(n) AS written", nil)
				require.NoError(t, err)
				require.Equal(t, int64(0), result.Rows[0][0], "a cancelled CREATE persists no node")
				result, err = executor.Execute(context.Background(), "MATCH (n:Seed) RETURN n.v IS NULL AND n.t IS NULL AS untouched", nil)
				require.NoError(t, err)
				require.Equal(t, true, result.Rows[0][0], "a cancelled SET persists no property")
			})
		}
	}
}
