//go:build closeout_l2_e15

package storage

// L2 close-out, report entry 15: StreamNodesWithOptions with a projection
// drops its row on repeated scans. The first scan decodes the stored body
// (the decoder hands the filter only the projected properties); later scans
// find the node resident in the read cache, and the resident route must give
// the filter the same projected selection. A filter that looks at which
// properties the map holds (len(p) == 1) tells the two apart.

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

const l2E15Scans = 4

// l2E15Engine builds the entry's engine and its 36 nodes: a:00 .. a:35, the
// first 12 labelled Doc, the rest Other, each {id: "<n>", payload: "more"}.
func l2E15Engine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, NodeCacheMaxEntries: 200, NodeCacheMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	ctx := context.Background()
	for i := 0; i < 36; i++ {
		label := "Other"
		if i < 12 {
			label = "Doc"
		}
		node := &Node{
			ID:         NodeID(fmt.Sprintf("a:%02d", i)),
			Labels:     []string{label},
			Properties: map[string]any{"id": fmt.Sprintf("%d", i), "payload": "more"},
		}
		if _, err := engine.CreateNode(ctx, node); err != nil {
			t.Fatalf("create %s: %v", node.ID, err)
		}
	}
	// Step 3 of the entry: empty the node read caches, so the first scan
	// decodes the stored bodies and fills the cache for the later scans.
	engine.nodeCacheMu.Lock()
	engine.resetNodeReadCachesLocked()
	engine.nodeCacheMu.Unlock()
	return engine
}

func l2E15Options(filter func(map[string]any) bool) StreamNodesOptions {
	return StreamNodesOptions{Prefix: "a:", Label: "Doc", QueryNodes: true, Projection: []string{"id"}, PropertyFilter: filter}
}

// l2E15Scan runs one StreamNodesWithOptions call and returns the IDs passed
// to the callback.
func l2E15Scan(t *testing.T, engine *BadgerEngine, opts StreamNodesOptions) []string {
	t.Helper()
	var ids []string
	if err := engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
		ids = append(ids, string(node.ID))
		return nil
	}); err != nil {
		t.Fatalf("StreamNodesWithOptions: %v", err)
	}
	return ids
}

func TestL2_e15_Repro(t *testing.T) {
	engine := l2E15Engine(t)
	// The entry's filter: true only for a map holding exactly the projected
	// property, with id "3".
	opts := l2E15Options(func(p map[string]any) bool { return len(p) == 1 && p["id"] == "3" })
	results := make([]string, 0, l2E15Scans)
	bad := 0
	for scan := 1; scan <= l2E15Scans; scan++ {
		ids := l2E15Scan(t, engine, opts)
		results = append(results, fmt.Sprintf("scan%d=[%s]", scan, strings.Join(ids, ",")))
		if len(ids) != 1 || ids[0] != "a:03" {
			bad++
		}
	}
	if bad > 0 {
		t.Logf("L2RESULT E15 DEFECT %s (expected a:03 on every scan; %d of %d scans differ)", strings.Join(results, " "), bad, l2E15Scans)
		t.Errorf("projected scan with a projection-sensitive filter did not return a:03 on every scan: %s", strings.Join(results, " "))
		return
	}
	t.Logf("L2RESULT E15 OK %s", strings.Join(results, " "))
}

// The control: the same four scans with a filter that tests only the named
// property (the kind every Cypher filter is). It must return a:03 every time
// whether or not the defect is present, proving the fixture and the cache
// path work.
func TestL2_e15_Control(t *testing.T) {
	engine := l2E15Engine(t)
	opts := l2E15Options(func(p map[string]any) bool { return p["id"] == "3" })
	for scan := 1; scan <= l2E15Scans; scan++ {
		ids := l2E15Scan(t, engine, opts)
		if len(ids) != 1 || ids[0] != "a:03" {
			t.Fatalf("control scan %d returned %v, want [a:03]", scan, ids)
		}
	}
	// Without a filter the label route returns the 12 Doc nodes.
	if ids := l2E15Scan(t, engine, StreamNodesOptions{Prefix: "a:", Label: "Doc", QueryNodes: true, Projection: []string{"id"}}); len(ids) != 12 {
		t.Fatalf("unfiltered control scan returned %d nodes, want 12: %v", len(ids), ids)
	}
	t.Logf("L2RESULT E15 CONTROL OK a:03 on %d scans with a name-only filter; 12 Doc nodes unfiltered", l2E15Scans)
}

// The cause the entry names: the resident-node route settles the filter on a
// property map other than the projected one (keepsResidentNode,
// query_node_scan.go:75 at the pin). This logs what that step hands the filter
// for a full resident image of a:03, and whether it keeps the node.
func TestL2_e15_Cause(t *testing.T) {
	var seen map[string]any
	opts := l2E15Options(func(p map[string]any) bool {
		seen = make(map[string]any, len(p))
		for k, v := range p {
			seen[k] = v
		}
		return len(p) == 1 && p["id"] == "3"
	})
	resident := &Node{ID: "a:03", Labels: []string{"Doc"}, Properties: map[string]any{"id": "3", "payload": "more"}}
	kept := opts.keepsResidentNode(resident)
	projected := (QueryResultProjection{}).NodeProperties(resident, opts.Projection)
	t.Logf("L2CAUSE E15 keepsResidentNode=%v filterSaw=%v projectedSelection=%v", kept, seen, projected)
	if !kept {
		t.Errorf("keepsResidentNode rejected a:03: the filter saw %v instead of the projection %v", seen, projected)
	}
	// The live engine: which scans lose the row, and what is resident then.
	engine := l2E15Engine(t)
	for scan := 1; scan <= l2E15Scans; scan++ {
		ids := l2E15Scan(t, engine, opts)
		engine.nodeCacheMu.RLock()
		_, inQueryCache := engine.queryNodeCache["a:03"]
		_, inNodeCache := engine.nodeCache["a:03"]
		engine.nodeCacheMu.RUnlock()
		t.Logf("L2CAUSE E15 scan%d ids=%v filterSawLast=%v residentQuery=%v residentFull=%v", scan, ids, seen, inQueryCache, inNodeCache)
	}
}
