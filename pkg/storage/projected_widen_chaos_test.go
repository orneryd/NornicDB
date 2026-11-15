package storage

import (
	"fmt"
	"sync"
	"testing"
)

// Exercise cache widening and invalidation while the same node is being
// updated. Each returned image must belong to one committed version.
func TestProjectedReadsConcurrentWidenAndUpdates(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	id := NodeID("test:concurrent-widen")
	properties := func(n int64) map[string]any {
		return map[string]any{"sequence": n, "body": fmt.Sprintf("body-%d", n), "bytes": []byte{byte(n)}}
	}
	if _, err := b.CreateNode(ctx, &Node{ID: id, Properties: properties(0)}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errors := make(chan error, 9)
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			<-start
			for i := range 300 {
				fields := []string{"sequence", "body", "bytes", "missing"}
				if (i+worker)%2 == 0 {
					fields = []string{"sequence", "body"}
				}
				node, err := b.GetNodeProjected(ctx, id, fields)
				if err != nil {
					errors <- err
					return
				}
				sequence, ok := node.Properties["sequence"].(int64)
				if !ok || node.Properties["body"] != fmt.Sprintf("body-%d", sequence) {
					errors <- fmt.Errorf("inconsistent projected image: %#v", node.Properties)
					return
				}
				if value, exists := node.Properties["bytes"]; exists {
					bytes, ok := value.([]byte)
					if !ok || len(bytes) != 1 || bytes[0] != byte(sequence) {
						errors <- fmt.Errorf("inconsistent projected bytes: %#v", node.Properties)
						return
					}
					bytes[0]++ // A caller must not mutate the cached image.
				}
				if _, exists := node.Properties["missing"]; exists {
					errors <- fmt.Errorf("unexpected missing property: %#v", node.Properties)
					return
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for n := int64(1); n <= 150; n++ {
			if err := b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: id, Properties: properties(n)}}); err != nil {
				errors <- err
				return
			}
		}
	})
	close(start)
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
