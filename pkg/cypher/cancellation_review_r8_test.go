package cypher

// Round-8 review repros (public Execute, both parsers): each cancels inside
// the named implementation frame and requires the cancellation or deadline
// error within 50 ms.

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type r8ArmedContext struct {
	context.Context
	mu    sync.Mutex
	inner context.Context
	done  chan struct{}
	fired time.Time
}

func (c *r8ArmedContext) Done() <-chan struct{} { return c.done }
func (c *r8ArmedContext) Err() error {
	c.mu.Lock()
	inner := c.inner
	c.mu.Unlock()
	if inner == nil {
		return nil
	}
	return inner.Err()
}
func (c *r8ArmedContext) Deadline() (time.Time, bool) {
	c.mu.Lock()
	inner := c.inner
	c.mu.Unlock()
	if inner == nil {
		return time.Time{}, false
	}
	return inner.Deadline()
}
func (c *r8ArmedContext) arm(deadline bool) {
	var inner context.Context
	var stop context.CancelFunc
	if deadline {
		inner, stop = context.WithTimeout(context.Background(), time.Millisecond)
	} else {
		inner, stop = context.WithCancel(context.Background())
		stop()
	}
	c.mu.Lock()
	c.inner = inner
	c.mu.Unlock()
	<-inner.Done()
	c.mu.Lock()
	c.fired = time.Now()
	close(c.done)
	c.mu.Unlock()
	stop()
}
func r8HasFrame(target string) bool {
	records := make([]runtime.StackRecord, 256)
	n, ok := runtime.GoroutineProfile(records)
	if !ok {
		records = make([]runtime.StackRecord, n+16)
		n, ok = runtime.GoroutineProfile(records)
		if !ok {
			return false
		}
	}
	for _, record := range records[:n] {
		frames := runtime.CallersFrames(record.Stack())
		for {
			frame, more := frames.Next()
			if strings.Contains(frame.Function, target) {
				return true
			}
			if !more {
				break
			}
		}
	}
	return false
}
func TestReviewR8PublicWorkCancellation(t *testing.T) {
	ints := make([]interface{}, 1<<20)
	for i := range ints {
		ints[i] = int64(len(ints) - i)
	}
	maps := make([]interface{}, 4096)
	for i := range maps {
		maps[i] = map[string]interface{}{"rank": int64(len(maps) - i)}
	}
	cases := []struct {
		name, query, target string
		values              interface{}
	}{
		{"coll_sort", "RETURN coll.sort($values)", ".sortedCypherValues", ints},
		{"apoc_sort", "RETURN apoc.coll.sort($values)", ".sortedCypherValues", ints},
		{"sortNodes", "RETURN apoc.coll.sortNodes($values,'rank')", ".apocCollSortNodesByProperty", maps},
		{"distinct", "RETURN coll.distinct($values)", ".fnCollDistinct", ints},
		{"flatten", "RETURN coll.flatten($values)", ".collFlatten", ints},
		{"indexOf", "RETURN coll.indexOf($values,-1)", ".fnCollIndexOf", ints},
		{"max", "RETURN coll.max($values)", ".fnCollExtreme.func1", ints},
	}
	for _, test := range cases {
		for _, deadline := range []bool{false, true} {
			mode := "cancel"
			if deadline {
				mode = "deadline"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r8"), 0, 0)
				ctx := &r8ArmedContext{Context: context.Background(), done: make(chan struct{})}
				finished := make(chan error, 1)
				go func() {
					_, err := executor.Execute(ctx, test.query, map[string]interface{}{"values": test.values})
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
				err := <-finished
				returned := time.Now()
				ctx.mu.Lock()
				fired := ctx.fired
				ctx.mu.Unlock()
				late := returned.Sub(fired)
				t.Logf("query=%s target=%s outcome=%v cancel_to_return=%v", test.query, test.target, err, late)
				want := error(context.Canceled)
				if deadline {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) {
					t.Errorf("outcome=%v want=%v", err, want)
				}
				if late > 50*time.Millisecond {
					t.Errorf("cancel-to-return %v exceeds 50ms", late)
				}
			})
		}
	}
}
