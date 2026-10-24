package cypher

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestReviewR8OwnerAndStringCancellation(t *testing.T) {
	ints := make([]interface{}, 1<<20)
	for i := range ints {
		ints[i] = int64(len(ints) - i)
	}
	text := strings.Repeat("a", 1<<23)
	cases := []struct {
		name, query, target string
		params              map[string]interface{}
	}{
		{"coll_sort_owner", "RETURN coll.sort($values)", "/bulk.SliceStable", map[string]interface{}{"values": ints}},
		{"apoc_sort_owner", "RETURN apoc.coll.sort($values)", "/bulk.SliceStable", map[string]interface{}{"values": ints}},
		{"split_string", "RETURN size(split($text,'a'))", "/bulk.SplitN", map[string]interface{}{"text": text}},
		{"replace_string", "RETURN size(replace($text,'a','bc'))", "/bulk.Replace", map[string]interface{}{"text": text}},
	}
	for _, test := range cases {
		for _, deadline := range []bool{false, true} {
			mode := "cancel"
			if deadline {
				mode = "deadline"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r8extra"), 0, 0)
				ctx := &r8ArmedContext{Context: context.Background(), done: make(chan struct{})}
				finished := make(chan error, 1)
				go func() { _, err := executor.Execute(ctx, test.query, test.params); finished <- err }()
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
