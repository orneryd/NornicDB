package storage

// Verifier of hunt lead L-H6-001 (test-side only, not for merge): where the
// label read that CREATE INDEX and MATCH (n:G) pay grows faster than the data.
//
// The H6 graph shape (G nodes {id,k,s}, three GR relationships each, one Hub
// with 256*f Leaf nodes) is written through the storage API at scales f, then
// the same label population is read in six ways, three times each:
//   prod         BadgerEngine.GetStoredNodesByLabel (the read the DDL backfill
//                and the label scan take; point lookups after 2048 nodes,
//                label_scan_direct.go:14-18, badger_queries.go:707-725)
//   index        the label index walk alone (newLabelIndexReader)
//   point_raw    one txn.Get(nodeKey) per id, value bytes only
//   onepass_raw  readNodeRecordsInOnePass over the same ids, value bytes only
//   point_dec    point lookups decoded like the scan (readNodeItemInTxn)
//   onepass_dec  one pass decoded the same way
// VERIFY_H6_LABEL lines: per variant the milliseconds and microseconds per node.
//
// Run: VERIFY_H6=1 [VERIFY_H6_SCALES=1,16,64]
//      go test ./pkg/storage -run '^TestVerifyH6LabelRead$' -count=1 -timeout=60m -v

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func verifyH6LabelBuild(t *testing.T, ns *NamespacedEngine, f int) {
	ctx := context.Background()
	n := 5000 * f
	for from := 0; from < n; from += 5000 {
		nodes := make([]*Node, 0, 5000)
		for i := from; i < min(from+5000, n); i++ {
			nodes = append(nodes, &Node{ID: NodeID("g" + strconv.Itoa(i)), Labels: []string{"G"},
				Properties: map[string]any{"id": int64(i), "k": int64((i * 7919) % 100003), "s": "s" + strconv.Itoa(i)}})
		}
		if err := ns.BulkCreateNodes(ctx, nodes); err != nil {
			t.Fatalf("nodes: %v", err)
		}
	}
	for from := 0; from < n; from += 2000 {
		edges := make([]*Edge, 0, 6000)
		for i := from; i < min(from+2000, n); i++ {
			for j := 1; j <= 3; j++ {
				edges = append(edges, &Edge{ID: EdgeID(fmt.Sprintf("r%d_%d", i, j)), StartNode: NodeID("g" + strconv.Itoa(i)),
					EndNode: NodeID("g" + strconv.Itoa((i*31+j*7907)%n)), Type: "GR", Properties: map[string]any{"w": float64((i+j)%100) / 100}})
			}
		}
		if err := ns.BulkCreateEdges(ctx, edges); err != nil {
			t.Fatalf("edges: %v", err)
		}
	}
	hub := 256 * f
	nodes := []*Node{{ID: "hub", Labels: []string{"Hub"}, Properties: map[string]any{"id": int64(0)}}}
	for i := 0; i < hub; i++ {
		nodes = append(nodes, &Node{ID: NodeID("leaf" + strconv.Itoa(i)), Labels: []string{"Leaf"}, Properties: map[string]any{"i": int64(i)}})
	}
	if err := ns.BulkCreateNodes(ctx, nodes); err != nil {
		t.Fatalf("hub nodes: %v", err)
	}
	edges := make([]*Edge, 0, hub)
	for i := 0; i < hub; i++ {
		edges = append(edges, &Edge{ID: EdgeID("h" + strconv.Itoa(i)), StartNode: "hub", EndNode: NodeID("leaf" + strconv.Itoa(i)), Type: "HR"})
	}
	if err := ns.BulkCreateEdges(ctx, edges); err != nil {
		t.Fatalf("hub edges: %v", err)
	}
}

func TestVerifyH6LabelRead(t *testing.T) {
	if os.Getenv("VERIFY_H6") == "" {
		t.Skip("set VERIFY_H6=1")
	}
	scales := []int{1, 16, 64}
	if v := os.Getenv("VERIFY_H6_SCALES"); v != "" {
		scales = nil
		for _, s := range strings.Split(v, ",") {
			n, _ := strconv.Atoi(s)
			scales = append(scales, n)
		}
	}
	worker, _ := os.Hostname()
	ctx := context.Background()
	base, err := os.MkdirTemp(os.TempDir(), "verify-h6-label-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	for _, f := range scales {
		dir := filepath.Join(base, fmt.Sprintf("x%d", f))
		engine, err := NewBadgerEngine(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		ns := NewNamespacedEngine(engine, "nornic")
		start := time.Now()
		verifyH6LabelBuild(t, ns, f)
		fmt.Printf("VERIFY_H6_LABEL_BUILD scale=%d nodes=%d seconds=%.1f worker=%s\n", f, 5000*f, time.Since(start).Seconds(), worker)

		var ids []NodeID
		report := func(variant string, rep, count int, elapsed time.Duration, extra int) {
			data, _ := json.Marshal(map[string]any{"scale": f, "variant": variant, "rep": rep, "nodes": count, "ms": float64(elapsed.Microseconds()) / 1000,
				"us_per_node": float64(elapsed.Nanoseconds()) / 1000 / float64(max(count, 1)), "bytes": extra, "worker": worker})
			fmt.Printf("VERIFY_H6_LABEL %s\n", data)
		}
		for rep := 0; rep < 3; rep++ {
			t0 := time.Now()
			nodes, err := engine.GetStoredNodesByLabel(ctx, "G")
			if err != nil {
				t.Fatalf("prod: %v", err)
			}
			report("prod", rep, len(nodes), time.Since(t0), 0)
			if ids == nil {
				for _, node := range nodes {
					ids = append(ids, node.ID)
				}
			}
		}
		for rep := 0; rep < 3; rep++ {
			count := 0
			t0 := time.Now()
			err := engine.db.View(func(txn *badger.Txn) error {
				it, err := engine.newLabelIndexReader(ctx, txn, "", "G", "", storedRead, false)
				if err != nil {
					return err
				}
				defer it.Close()
				for it.Next() {
					count++
				}
				return it.Err()
			})
			if err != nil {
				t.Fatalf("index: %v", err)
			}
			report("index", rep, count, time.Since(t0), 0)
		}
		pointRead := func(decode bool) (int, int, error) {
			count, size := 0, 0
			err := engine.db.View(func(txn *badger.Txn) error {
				for _, id := range ids {
					item, err := txn.Get(nodeKey(id))
					if err != nil {
						return err
					}
					if decode {
						if _, err := engine.readNodeItemInTxn(txn, item, id, nodeItemFullEmbeddings, nodeBodyCacheNoAdmit, false, nil); err != nil {
							return err
						}
					} else if err := item.Value(func(v []byte) error { size += len(v); return nil }); err != nil {
						return err
					}
					count++
				}
				return nil
			})
			return count, size, err
		}
		onePass := func(decode bool) (int, int, error) {
			count, size := 0, 0
			err := engine.db.View(func(txn *badger.Txn) error {
				return readNodeRecordsInOnePass(ctx, txn, "", ids, func(id NodeID, item *badger.Item) error {
					count++
					if decode {
						_, err := engine.readNodeItemInTxn(txn, item, id, nodeItemFullEmbeddings, nodeBodyCacheNoAdmit, false, nil)
						return err
					}
					return item.Value(func(v []byte) error { size += len(v); return nil })
				})
			})
			return count, size, err
		}
		for _, variant := range []string{"point_raw", "onepass_raw", "point_dec", "onepass_dec"} {
			for rep := 0; rep < 3; rep++ {
				decode := strings.HasSuffix(variant, "_dec")
				read := pointRead
				if strings.HasPrefix(variant, "onepass") {
					read = onePass
				}
				t0 := time.Now()
				count, size, err := read(decode)
				elapsed := time.Since(t0)
				if err != nil {
					t.Fatalf("%s: %v", variant, err)
				}
				report(variant, rep, count, elapsed, size)
			}
		}
		if err := engine.Close(); err != nil {
			t.Logf("close: %v", err)
		}
		_ = os.RemoveAll(dir)
	}
}
