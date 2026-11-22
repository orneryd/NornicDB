package storage

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPR1826S1LongReadsDuringCompaction keeps old snapshots reading projected
// images on disk while writers commit, tables are flattened, the value log is
// collected and old versions are pruned.
func TestPR1826S1LongReadsDuringCompaction(t *testing.T) {
	b, _ := createTestBadgerEngineOnDisk(t)
	b.nodeCacheMaxEntries, b.nodeBodyCacheMaxBytes = 16, 64<<10
	ctx := t.Context()
	const nodes = 40
	id := func(i int) NodeID { return NodeID(fmt.Sprintf("k:n%d", i)) }
	image := func(i int, n int64) map[string]any {
		return map[string]any{"i": int64(i), "seq": n, "big": strings.Repeat(fmt.Sprintf("%d-%d|", i, n), 4000), "small": fmt.Sprintf("s%d-%d", i, n), "bytes": []byte{byte(i), byte(n)}}
	}
	for i := 0; i < nodes; i++ {
		_, err := b.CreateNode(ctx, &Node{ID: id(i), Labels: []string{"K"}, Properties: image(i, 0)})
		require.NoError(t, err)
	}
	sets := [][]string{{"seq"}, {"seq", "small"}, {"seq", "big"}, {"seq", "bytes", "absent"}, {"seq", "i", "big", "small", "bytes"}}
	var stop atomic.Bool
	errs := make(chan error, 32)
	report := func(err error) {
		select {
		case errs <- err:
		default:
		}
		stop.Store(true)
	}
	check := func(what string, node *Node, names []string, wantSeq int64) error {
		seq, ok := node.Properties["seq"].(int64)
		if !ok || (wantSeq >= 0 && seq != wantSeq) {
			return fmt.Errorf("%s %s: seq %v want %d", what, node.ID, node.Properties["seq"], wantSeq)
		}
		var i int
		_, _ = fmt.Sscanf(string(node.ID), "k:n%d", &i)
		if want, got := s1NormProps(s1Pick(image(i, seq), names)), s1NormProps(node.Properties); !reflect.DeepEqual(want, got) {
			return fmt.Errorf("%s %s: torn image for %v", what, node.ID, names)
		}
		return nil
	}
	var readers, workers sync.WaitGroup
	for w := 0; w < 3; w++ {
		readers.Go(func() {
			for !stop.Load() {
				tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
				if err != nil {
					report(err)
					return
				}
				pinned := make(map[NodeID]int64, nodes)
				for round := 0; round < 15 && !stop.Load(); round++ {
					for i := 0; i < nodes; i++ {
						names := sets[(round+i+w)%len(sets)]
						node, err := tx.GetNodeProjected(ctx, id(i), names)
						if err != nil {
							report(fmt.Errorf("snapshot: %w", err))
							_ = tx.Rollback()
							return
						}
						want, seen := pinned[id(i)]
						if !seen {
							want = -1
						}
						if err := check("snapshot", node, names, want); err != nil {
							report(err)
						}
						pinned[id(i)], _ = node.Properties["seq"].(int64)
					}
				}
				_ = tx.Rollback()
			}
		})
	}
	readers.Go(func() {
		for round := 0; !stop.Load(); round++ {
			names := sets[round%len(sets)]
			if err := b.StreamNodesByLabelProjected(ctx, "K", names, func(node *Node) error { return check("label scan", node, names, -1) }); err != nil {
				report(err)
				return
			}
		}
	})
	workers.Go(func() {
		for n := int64(1); n <= 40 && !stop.Load(); n++ {
			for i := 0; i < nodes; i++ {
				if err := b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: id(i), Labels: []string{"K"}, Properties: image(i, n)}}); err != nil {
					report(fmt.Errorf("update: %w", err))
					return
				}
				node, err := b.GetNodeProjected(ctx, id(i), []string{"seq", "small"})
				if err != nil {
					report(err)
					return
				}
				if err := check("own write", node, []string{"seq", "small"}, n); err != nil {
					report(err)
				}
			}
		}
	})
	flattened, collected, pruned := 0, 0, int64(0)
	readers.Go(func() {
		for !stop.Load() {
			if err := b.db.Flatten(2); err != nil {
				report(fmt.Errorf("flatten: %w", err))
				return
			}
			flattened++
			if b.db.RunValueLogGC(ctx, 0.01) == nil {
				collected++
			}
			removed, err := b.PruneMVCCVersions(ctx, MVCCPruneOptions{MaxVersionsPerKey: 1})
			if err != nil {
				report(fmt.Errorf("prune: %w", err))
				return
			}
			pruned += removed
		}
	})
	workers.Wait()
	stop.Store(true)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	t.Logf("S1COUNT compaction flattened=%d valueLogCollected=%d prunedVersions=%d", flattened, collected, pruned)
	for i := 0; i < nodes; i++ {
		node, err := b.GetNodeProjected(ctx, id(i), sets[4])
		require.NoError(t, err)
		require.NoError(t, check("final", node, sets[4], 40))
	}
}
