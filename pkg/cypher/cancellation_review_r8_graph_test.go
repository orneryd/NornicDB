package cypher

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type r8MaterializedReader struct {
	storage.Engine
	edges []*storage.Edge
	nodes []*storage.Node
}

func (s *r8MaterializedReader) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if id == "root" {
		return s.edges, nil
	}
	return nil, nil
}
func (s *r8MaterializedReader) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return nil, nil
}
func (s *r8MaterializedReader) GetAllNodes() []*storage.Node       { return s.nodes }
func (s *r8MaterializedReader) AllEdges() ([]*storage.Edge, error) { return nil, nil }
func TestReviewR8GraphAndExportCancellation(t *testing.T) {
	// These adapters return already materialized data immediately. Timing below
	// measures the Cypher consumer, independently of storage read cancellation.
	edges := make([]*storage.Edge, 1<<20)
	for i := range edges {
		edges[i] = &storage.Edge{ID: storage.EdgeID(strconv.Itoa(i)), StartNode: "root", EndNode: "end", Type: "R"}
	}
	nodes := make([]*storage.Node, 1<<17)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(strconv.Itoa(i)), Labels: []string{"N"}, Properties: map[string]interface{}{"text": strings.Repeat("a", 128)}}
	}
	cases := []struct {
		name, query, target string
		edges               []*storage.Edge
		nodes               []*storage.Node
		params              map[string]interface{}
	}{
		{"dijkstra_adjacency", "CALL apoc.algo.dijkstra($start,$end,'R>','') YIELD weight RETURN weight", ".apocSteps", edges, nil, map[string]interface{}{"start": "root", "end": "end"}},
		{"csv_export", "CALL apoc.export.csv.all('',{}) YIELD rows RETURN rows", ".callApocExportCsvAll", nil, nodes, nil},
	}
	for _, test := range cases {
		for _, deadline := range []bool{false, true} {
			mode := "cancel"
			if deadline {
				mode = "deadline"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(&r8MaterializedReader{Engine: newTestMemoryEngine(t), edges: test.edges, nodes: test.nodes}, 0, 0)
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
						t.Fatal("target not reached")
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
