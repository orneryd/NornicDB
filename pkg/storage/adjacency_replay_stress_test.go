package storage

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestAdjacencyCacheLockOrderReplayStress(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.GapProbeStopWorker()
	for _, id := range []NodeID{"test:a", "test:b", "other:writer"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	// Exercise the scorer/access-metadata route as well as ordinary cache hits.
	b.SetDecayEnabled(true)
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "stress", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: false}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "stress", ProfileRef: "stress", TargetEdgeType: "STRESS", IsEdge: true}))
	var fills, replays, writes, holes atomic.Int64
	hook := func() { fills.Add(1) }
	b.adjCacheFillHook.Store(&hook)
	defer b.adjCacheFillHook.Store(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errors := make(chan error, 16)
	report := func(err error) {
		if err != nil {
			select {
			case errors <- err:
			default:
			}
			cancel()
		}
	}
	start := make(chan struct{})
	var readers, producers sync.WaitGroup
	readers.Add(5)
	// An earlier assigned ordinary commit may be delayed before its physical
	// write. Later replay/writer invalidations must survive this unpublished
	// interval while readers keep filling from the old published snapshot.
	go func() {
		defer readers.Done()
		<-start
		for ctx.Err() == nil {
			hole, err := b.db.oracle.assign()
			if err != nil {
				report(err)
				return
			}
			until := time.Now().Add(20 * time.Millisecond)
			for ctx.Err() == nil && b.nodeCacheCommitTs.Load() <= hole && time.Now().Before(until) {
				runtime.Gosched()
			}
			if b.nodeCacheCommitTs.Load() > hole {
				holes.Add(1)
			}
			b.db.oracle.finish(hole)
			runtime.Gosched()
		}
	}()
	go func() {
		defer readers.Done()
		<-start
		for ctx.Err() == nil {
			if err := b.FlushWriteBehind(); err != nil {
				report(err)
				return
			}
			replays.Add(1)
			runtime.Gosched()
		}
	}()
	for r := 0; r < 3; r++ {
		go func(direction int) {
			defer readers.Done()
			<-start
			for ctx.Err() == nil {
				b.adjCacheInvalidateAll() // force publication to overlap replay, not just hits
				var edges []*Edge
				var err error
				switch direction {
				case 0:
					edges, err = b.GetOutgoingEdges("test:a")
				case 1:
					edges, err = b.GetIncomingEdges("test:b")
				case 2:
					edges, err = b.GetEdgesByType("STRESS")
				}
				if err != nil {
					report(err)
					return
				}
				seen := map[EdgeID]bool{}
				for _, e := range edges {
					if e.StartNode != "test:a" || e.EndNode != "test:b" || e.Type != "STRESS" || seen[e.ID] {
						report(fmt.Errorf("invalid or duplicate edge: %+v", e))
						return
					}
					seen[e.ID] = true
				}
				runtime.Gosched()
			}
		}(r)
	}
	const count = 64
	producers.Add(2)
	go func() {
		defer producers.Done()
		<-start
		for i := 0; i < count && ctx.Err() == nil; i++ {
			tx, err := b.BeginTransaction()
			if err != nil {
				report(err)
				return
			}
			err = tx.SetNamespace("test")
			if err == nil {
				err = tx.SetImplicit(true)
			}
			if err == nil {
				err = tx.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:stress-%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "STRESS", Properties: map[string]any{"ordinal": int64(i)}})
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
			if err != nil {
				report(err)
				return
			}
			runtime.Gosched()
		}
	}()
	go func() {
		defer producers.Done()
		<-start
		for i := 0; i < count && ctx.Err() == nil; i++ {
			if err := b.UpdateNode(&Node{ID: "other:writer", Properties: map[string]any{"v": int64(i)}}); err != nil {
				report(err)
				return
			}
			writes.Add(1)
			runtime.Gosched()
		}
	}()
	close(start)
	produced := make(chan struct{})
	go func() { producers.Wait(); close(produced) }()
	select {
	case <-produced:
	case <-time.After(90 * time.Second):
		t.Fatal("replay/fill/write producers deadlocked")
	}
	cancel()
	stopped := make(chan struct{})
	go func() { readers.Wait(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("replay/fill readers deadlocked")
	}
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushWriteBehind())
	require.Equal(t, int64(count), writes.Load())
	require.Positive(t, replays.Load())
	require.Positive(t, fills.Load())
	require.Positive(t, holes.Load(), "stress must overlap an oracle hole with durable replay/writes")
	out, err := b.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, out, count)
	in, err := b.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.Len(t, in, count)
	typed, err := b.GetEdgesByType("STRESS")
	require.NoError(t, err)
	require.Len(t, typed, count)
	t.Logf("LOCK_ORDER_STRESS replay=%d fills=%d unrelatedWrites=%d oracleHoles=%d edges=%d", replays.Load(), fills.Load(), writes.Load(), holes.Load(), len(out))
}
