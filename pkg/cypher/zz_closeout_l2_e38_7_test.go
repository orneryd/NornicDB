//go:build closeout_l2_e38_7

package cypher

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E38.7: MATCH (h:Hub)<-[r:LINK]-(d:Doc) OPTIONAL MATCH (h)<-[:CORRECTS]-(c:Correction)
// RETURN count(c) runs one optional lookup per row (N+1 ReadAdjacentEdges
// calls), and every lookup walks the hub's whole adjacency prefix
// (pkg/storage/badger_queries.go collectEdgesByIndexPrefix) before rejecting
// the other types, so the statement's work grows with N x degree = N^2 even
// though at most N relationships cross the storage boundary.
//
// The counting wrapper of the entry is kept (calls and relationships
// returned, logged), but it sees only the storage boundary. The decisive
// measure is therefore in-process scaling: the statement's cost per input row
// at N = 2048 against N = 256 (8x the rows and 8x the hub degree). If every
// optional lookup walks the hub's degree, the cost per row grows about 8x;
// if a lookup's cost does not depend on the hub's other relationships, it
// stays about 1x. Threshold 3.0 on both time and bytes allocated per row: far
// from either. The Control runs the same statement without the OPTIONAL
// MATCH through the same harness and must stay under the threshold.
//
// The parser is the process's (NORNICDB_PARSER, read at init).

const (
	l2E38_7Small    = 256
	l2E38_7Large    = 2048
	l2E38_7MaxRatio = 3.0
	l2E38_7Runs     = 5
	l2E38_7Query    = "MATCH (h:Hub)<-[r:LINK]-(d:Doc) OPTIONAL MATCH (h)<-[:CORRECTS]-(c:Correction) RETURN count(c) AS c"
	l2E38_7Linear   = "MATCH (h:Hub)<-[r:LINK]-(d:Doc) RETURN count(d) AS c"
)

type l2E38_7Counting struct {
	storage.Engine
	calls atomic.Int64
	edges atomic.Int64
}

func (c *l2E38_7Counting) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	edges, err := c.Engine.ReadAdjacentEdges(ctx, request)
	c.calls.Add(1)
	c.edges.Add(int64(len(edges)))
	return edges, err
}

type l2E38_7Store struct {
	n    int
	ns   *storage.NamespacedEngine
	exec *StorageExecutor
	hub  storage.NodeID
}

func l2E38_7Open(t *testing.T, n int) *l2E38_7Store {
	t.Helper()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	ns := storage.NewNamespacedEngine(base, "nornic")
	ctx := context.Background()
	if _, err := ns.CreateNode(ctx, &storage.Node{ID: "h", Labels: []string{"Hub"}, Properties: map[string]interface{}{"id": "h"}}); err != nil {
		t.Fatalf("hub: %v", err)
	}
	for i := 0; i < n; i++ {
		id := storage.NodeID(fmt.Sprintf("d%d", i))
		if _, err := ns.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": string(id)}}); err != nil {
			t.Fatalf("doc %d: %v", i, err)
		}
		if err := ns.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: id, EndNode: "h", Type: "LINK", Properties: map[string]interface{}{}}); err != nil {
			t.Fatalf("link %d: %v", i, err)
		}
	}
	return &l2E38_7Store{n: n, ns: ns, exec: NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0, TTL: 0}), hub: "h"}
}

func l2E38_7Int(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return -1
}

func l2E38_7Run(t *testing.T, exec *StorageExecutor, query string, want int64) {
	t.Helper()
	res, err := exec.Execute(context.Background(), query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if len(res.Rows) != 1 || l2E38_7Int(res.Rows[0][0]) != want {
		t.Fatalf("%s returned %v, want %d", query, res.Rows, want)
	}
}

// cost is the median wall time and the median bytes allocated of one run of
// query, after one warm-up run.
func l2E38_7Cost(t *testing.T, s *l2E38_7Store, query string, want int64) (time.Duration, uint64) {
	t.Helper()
	l2E38_7Run(t, s.exec, query, want)
	var times []time.Duration
	var bytes []uint64
	for i := 0; i < l2E38_7Runs; i++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		l2E38_7Run(t, s.exec, query, want)
		times = append(times, time.Since(started))
		runtime.ReadMemStats(&after)
		bytes = append(bytes, after.TotalAlloc-before.TotalAlloc)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	sort.Slice(bytes, func(i, j int) bool { return bytes[i] < bytes[j] })
	return times[len(times)/2], bytes[len(bytes)/2]
}

type l2E38_7Scaling struct {
	smallTime, largeTime   time.Duration
	smallBytes, largeBytes uint64
	timeRatio, bytesRatio  float64
}

func l2E38_7Scale(t *testing.T, small, large *l2E38_7Store, query string, want func(n int) int64) l2E38_7Scaling {
	t.Helper()
	var s l2E38_7Scaling
	s.smallTime, s.smallBytes = l2E38_7Cost(t, small, query, want(small.n))
	s.largeTime, s.largeBytes = l2E38_7Cost(t, large, query, want(large.n))
	perRowSmall := float64(s.smallTime) / float64(small.n)
	perRowLarge := float64(s.largeTime) / float64(large.n)
	s.timeRatio = perRowLarge / perRowSmall
	s.bytesRatio = (float64(s.largeBytes) / float64(large.n)) / (float64(s.smallBytes) / float64(small.n))
	return s
}

func (s l2E38_7Scaling) String() string {
	return fmt.Sprintf("N=%d: %v %dB; N=%d: %v %dB; perRowTimeRatio=%.2f perRowBytesRatio=%.2f",
		l2E38_7Small, s.smallTime, s.smallBytes, l2E38_7Large, s.largeTime, s.largeBytes, s.timeRatio, s.bytesRatio)
}

// boundary runs the statement once through an executor over the counting
// wrapper and returns the ReadAdjacentEdges calls and relationships returned.
func l2E38_7Boundary(t *testing.T, s *l2E38_7Store) (int64, int64) {
	t.Helper()
	counting := &l2E38_7Counting{Engine: s.ns}
	exec := NewStorageExecutor(counting, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	l2E38_7Run(t, exec, l2E38_7Query, 0)
	return counting.calls.Load(), counting.edges.Load()
}

func TestL2_e38_7_Repro(t *testing.T) {
	small, large := l2E38_7Open(t, l2E38_7Small), l2E38_7Open(t, l2E38_7Large)
	var boundary []string
	for _, s := range []*l2E38_7Store{small, large} {
		calls, edges := l2E38_7Boundary(t, s)
		boundary = append(boundary, fmt.Sprintf("N=%d calls=%d edges=%d", s.n, calls, edges))
	}
	t.Logf("storage boundary (counting wrapper): %v", boundary)
	scaling := l2E38_7Scale(t, small, large, l2E38_7Query, func(int) int64 { return 0 })
	t.Logf("optional lookup statement: %s", scaling)
	defect := false
	if scaling.timeRatio > l2E38_7MaxRatio {
		defect = true
		t.Errorf("cost per row grew %.2fx in time from N=%d to N=%d (bound %.1f): each optional lookup walks the hub's degree", scaling.timeRatio, l2E38_7Small, l2E38_7Large, l2E38_7MaxRatio)
	}
	if scaling.bytesRatio > l2E38_7MaxRatio {
		defect = true
		t.Errorf("bytes allocated per row grew %.2fx from N=%d to N=%d (bound %.1f)", scaling.bytesRatio, l2E38_7Small, l2E38_7Large, l2E38_7MaxRatio)
	}
	verdict := "OK"
	if defect {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E38.7 %s %s boundary=%v", verdict, scaling, boundary)
}

// TestL2_e38_7_Control: the same traversal without the OPTIONAL MATCH is
// linear in N, so its cost per row stays under the threshold in the same
// harness on the same stores.
func TestL2_e38_7_Control(t *testing.T) {
	small, large := l2E38_7Open(t, l2E38_7Small), l2E38_7Open(t, l2E38_7Large)
	scaling := l2E38_7Scale(t, small, large, l2E38_7Linear, func(n int) int64 { return int64(n) })
	t.Logf("control (no optional lookup): %s", scaling)
	if scaling.timeRatio > l2E38_7MaxRatio || scaling.bytesRatio > l2E38_7MaxRatio {
		t.Errorf("control is not linear: %s", scaling)
	}
}

// TestL2_e38_7_Cause calls the storage read one optional lookup makes
// (ReadAdjacentEdges, incoming, type CORRECTS, on the hub) directly at both
// hub degrees. It returns nothing at either degree; if its time and bytes
// per call grow with the degree, the walk of the whole adjacency prefix
// (collectEdgesByIndexPrefix, pkg/storage/badger_queries.go:1566 in the
// entry; the iterator loop of that function at the pin) is the cost.
func TestL2_e38_7_Cause(t *testing.T) {
	for _, n := range []int{l2E38_7Small, l2E38_7Large} {
		s := l2E38_7Open(t, n)
		ctx := context.Background()
		request := storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: s.hub, Types: []string{"CORRECTS"}}
		for _, headers := range []bool{false, true} {
			request.Headers = headers
			if _, err := s.ns.ReadAdjacentEdges(ctx, request); err != nil {
				t.Fatalf("read: %v", err)
			}
			const calls = 200
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			started := time.Now()
			returned := 0
			for i := 0; i < calls; i++ {
				edges, err := s.ns.ReadAdjacentEdges(ctx, request)
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				returned += len(edges)
			}
			elapsed := time.Since(started)
			runtime.ReadMemStats(&after)
			t.Logf("L2CAUSE E38.7 degree=%d headers=%v: %v per call, %d B per call, %d mallocs per call, returned %d",
				n, headers, elapsed/calls, (after.TotalAlloc-before.TotalAlloc)/calls, (after.Mallocs-before.Mallocs)/calls, returned)
		}
		// The matching type for comparison: all N relationships.
		all := storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: s.hub, Types: []string{"LINK"}}
		edges, err := s.ns.ReadAdjacentEdges(ctx, all)
		t.Logf("L2CAUSE E38.7 degree=%d LINK read returned %d err=%v", n, len(edges), err)
	}
}
