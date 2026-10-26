// Author: Codex implementation lane, 2026-10-27 (NOR-148).
package cypher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Count actual snapshot iterations as well as AllNodes. Opening/closing a
// capability probe is not a corpus scan, and cannot make NoRebuild pass.
func (s *fulltextCountingStore) OwnsSnapshotReader(issued storage.Engine) bool { return issued == s }

func (s *fulltextCountingStore) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := s.NamespacedEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &fulltextCountingSnapshot{QueryReadSnapshot: inner.(storage.QueryReadSnapshot), scans: &s.scans}, nil
}

type fulltextCountingSnapshot struct {
	storage.QueryReadSnapshot
	scans *atomic.Int64
}

func (s *fulltextCountingSnapshot) IterateNodes(fn func(*storage.Node) bool) error {
	s.scans.Add(1)
	return s.QueryReadSnapshot.IterateNodes(fn)
}

func (s *fulltextCountingSnapshot) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	s.scans.Add(1)
	return s.QueryReadSnapshot.StreamEdges(ctx, visit)
}

func (s *fulltextCountingSnapshot) IterateAllEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	s.scans.Add(1)
	return s.QueryReadSnapshot.IterateAllEdges(ctx, visit)
}

// The visible scan remains observable separately from snapshot acquisition.
type fulltextVisibleSnapshot interface {
	IterateVisibleNodes(context.Context, func(*storage.Node) bool) error
}

func (s *fulltextCountingSnapshot) IterateVisibleNodes(ctx context.Context, fn func(*storage.Node) bool) error {
	s.scans.Add(1)
	if inner, ok := s.QueryReadSnapshot.(fulltextVisibleSnapshot); ok {
		return inner.IterateVisibleNodes(ctx, fn)
	}
	return s.QueryReadSnapshot.IterateNodes(fn)
}

type fulltextPopulationProbe struct {
	*storage.NamespacedEngine
	mu   sync.Mutex
	rows []*storage.Node
	// crossing is armed only after fixture/warmup. It ensures a write occurs
	// during EVERY population attempt, without depending on scheduler luck.
	crossing func(context.Context) error
}

func (p *fulltextPopulationProbe) OwnsSnapshotReader(issued storage.Engine) bool { return issued == p }

func (p *fulltextPopulationProbe) cross(ctx context.Context) error {
	if p.crossing != nil {
		return p.crossing(ctx)
	}
	return nil
}

func (p *fulltextPopulationProbe) capture(rows []*storage.Node) {
	p.mu.Lock()
	p.rows = rows
	p.mu.Unlock()
}

func (p *fulltextPopulationProbe) AllNodes() ([]*storage.Node, error) {
	rows, err := p.NamespacedEngine.AllNodes()
	if err != nil {
		return nil, err
	}
	p.capture(rows)
	return rows, p.cross(context.Background())
}

func (p *fulltextPopulationProbe) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return p.scanEdges(ctx, p.NamespacedEngine.StreamEdges, visit)
}

func (p *fulltextPopulationProbe) scanEdges(ctx context.Context, scan func(context.Context, func(*storage.Edge) error) error, visit func(*storage.Edge) error) error {
	var rows []*storage.Node
	first := true
	err := scan(ctx, func(edge *storage.Edge) error {
		if first {
			first = false
			if err := p.cross(ctx); err != nil {
				return err
			}
		}
		// The frozen release oracle uses the same field kernel for nodes and
		// edges. Adapt captured edge bodies, without rereading the live store.
		rows = append(rows, &storage.Node{ID: storage.NodeID(edge.ID), Labels: []string{edge.Type}, Properties: edge.Properties})
		return visit(edge)
	})
	p.capture(rows)
	return err
}

func (p *fulltextPopulationProbe) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := p.NamespacedEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &fulltextPopulationSnapshot{QueryReadSnapshot: inner.(storage.QueryReadSnapshot), probe: p}, nil
}

type fulltextPopulationSnapshot struct {
	storage.QueryReadSnapshot
	probe *fulltextPopulationProbe
}

func (s *fulltextPopulationSnapshot) IterateNodes(visit func(*storage.Node) bool) error {
	return s.scanNodes(context.Background(), s.QueryReadSnapshot.IterateNodes, visit)
}

func (s *fulltextPopulationSnapshot) IterateVisibleNodes(ctx context.Context, visit func(*storage.Node) bool) error {
	scan := s.QueryReadSnapshot.IterateNodes
	if inner, ok := s.QueryReadSnapshot.(fulltextVisibleSnapshot); ok {
		scan = func(fn func(*storage.Node) bool) error { return inner.IterateVisibleNodes(ctx, fn) }
	}
	return s.scanNodes(ctx, scan, visit)
}

func (s *fulltextPopulationSnapshot) scanNodes(ctx context.Context, scan func(func(*storage.Node) bool) error, visit func(*storage.Node) bool) error {
	var rows []*storage.Node
	var crossErr error
	first := true
	err := scan(func(node *storage.Node) bool {
		if first {
			first = false
			if crossErr = s.probe.cross(ctx); crossErr != nil {
				return false
			}
		}
		rows = append(rows, node)
		return visit(node)
	})
	s.probe.capture(rows)
	if err != nil {
		return err
	}
	return crossErr
}

func (s *fulltextPopulationSnapshot) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return s.probe.scanEdges(ctx, s.QueryReadSnapshot.StreamEdges, visit)
}

func (s *fulltextPopulationSnapshot) IterateAllEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return s.probe.scanEdges(ctx, s.QueryReadSnapshot.IterateAllEdges, visit)
}

type fulltextCapturedEngine struct {
	storage.Engine
	rows []*storage.Node
}

func (e *fulltextCapturedEngine) AllNodes() ([]*storage.Node, error) { return e.rows, nil }

func TestFulltextMaintainedPublicPopulationWithWriters(t *testing.T) {
	for _, stage := range []string{"cold_new_index", "lost_journal", "shared_service_revision"} {
		for _, relationships := range []bool{false, true} {
			for _, outside := range []bool{false, true} {
				for _, text := range []string{"hotel", "*", "text:*"} {
					name := fmt.Sprintf("%s/relationships=%v/outside=%v/%s", stage, relationships, outside, text)
					t.Run(name, func(t *testing.T) { fulltextPopulationWithWriter(t, stage, relationships, outside, text) })
				}
			}
		}
	}
}

func fulltextPopulationWithWriter(t *testing.T, stage string, relationships, outside bool, text string) {
	_, counted := fulltextMaintainedFixture(t, 80)
	probe := &fulltextPopulationProbe{NamespacedEngine: counted.NamespacedEngine}
	exec := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
	ctx := context.Background()
	index, procedure, entity := "source_text", "queryNodes", "node"
	if relationships {
		_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX link_text FOR ()-[r:LINK]-() ON EACH [r.text, r.title]", nil)
		require.NoError(t, err)
		for i := 0; i < 4; i++ {
			typ := "LINK"
			if i == 3 {
				typ = "OTHER"
			}
			require.NoError(t, probe.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("edge%d", i)), StartNode: "n000000", EndNode: "n000040", Type: typ, Properties: map[string]any{"id": fmt.Sprintf("edge%d", i), "text": "hotel services", "title": "Correspondence"}}))
		}
		index, procedure, entity = "link_text", "queryRelationships", "relationship"
	}
	query := fmt.Sprintf("CALL db.index.fulltext.%s('%s', $query) YIELD %s, score RETURN %s.id, score", procedure, index, entity, entity)
	params := map[string]any{"query": text}
	if stage == "shared_service_revision" {
		svc := search.NewService(probe.NamespacedEngine, search.DefaultVectorDimensions, nil)
		t.Cleanup(func() { require.NoError(t, svc.Close()) })
		exec.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return svc })
	}
	if stage != "cold_new_index" {
		_, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		// Unique IDs overflow the resident-derived journal bound. This also
		// changes the revision for the constructor-wrapper mismatch path.
		for i := 0; i < 40; i++ {
			id := storage.NodeID(fmt.Sprintf("journal-%d", i))
			_, err = probe.CreateNode(&storage.Node{ID: id, Labels: []string{"Outside"}})
			require.NoError(t, err)
			require.NoError(t, probe.DeleteNode(id))
		}
	}
	stop, finished := make(chan struct{}), make(chan error, 1)
	requests := make(chan chan error)
	var writes atomic.Int64
	write := func() error {
		value := fmt.Sprintf("hotel services %d", writes.Add(1))
		if relationships {
			id := storage.EdgeID("edge0")
			if outside {
				id = "edge3"
			}
			edge, err := probe.GetEdge(id)
			if err != nil {
				return err
			}
			edge.Properties["text"] = value
			return probe.UpdateEdge(edge)
		}
		id := storage.NodeID("n000000")
		if outside {
			id = "n000001"
		}
		node, err := probe.GetNode(id)
		if err != nil {
			return err
		}
		node.Properties["text"] = value
		return probe.UpdateNode(node)
	}
	probe.crossing = func(ctx context.Context) error {
		done := make(chan error, 1)
		select {
		case requests <- done:
		case <-stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-done:
			return err
		case <-stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				finished <- nil
				return
			case done := <-requests:
				err := write()
				done <- err
				if err != nil {
					finished <- err
					return
				}
			case <-ticker.C:
				if err := write(); err != nil {
					finished <- err
					return
				}
			}
		}
	}()
	type outcome struct {
		result *ExecuteResult
		err    error
	}
	readDone := make(chan outcome, 1)
	go func() { r, err := exec.Execute(ctx, query, params); readDone <- outcome{r, err} }()
	var got outcome
	completed := false
	select {
	case got = <-readDone:
		completed = true
	case <-time.After(2 * time.Second):
	}
	// Only stop the writer AFTER establishing whether the reader finished.
	close(stop)
	require.NoError(t, <-finished)
	if !completed {
		got = <-readDone
	}
	require.True(t, completed, "population required the writer to stop")
	require.NoError(t, got.err)
	require.Positive(t, writes.Load())
	probe.mu.Lock()
	rows := probe.rows
	probe.mu.Unlock()
	var expected [][]any
	if relationships {
		adapted := make([]*storage.Node, 0, len(rows))
		for _, row := range rows {
			labels := []string{"Outside"}
			if matchesLabels(row, []string{"LINK"}) {
				labels = []string{"Source"}
			}
			adapted = append(adapted, &storage.Node{ID: row.ID, Labels: labels, Properties: row.Properties})
		}
		rows = adapted
	}
	if text == "hotel" {
		expected = fulltextReleaseOracle(t, &fulltextCapturedEngine{Engine: probe, rows: rows}, text)
	} else {
		for _, row := range rows {
			if matchesLabels(row, []string{"Source"}) {
				expected = append(expected, []any{row.Properties["id"], float64(1)})
			}
		}
	}
	require.Equal(t, expected, got.result.Rows, "rows/scores must equal the captured committed view")
}

func TestFulltextMaintainedPublicPopulationCancellation(t *testing.T) {
	for _, relationships := range []bool{false, true} {
		t.Run(fmt.Sprint(relationships), func(t *testing.T) {
			_, counted := fulltextMaintainedFixture(t, 80)
			probe := &fulltextPopulationProbe{NamespacedEngine: counted.NamespacedEngine}
			first := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
			_, err := first.Execute(context.Background(), "CREATE FULLTEXT INDEX other_text FOR (n:Unrelated) ON EACH [n.text]", nil)
			require.NoError(t, err)
			query := "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node, score RETURN node.id, score"
			if relationships {
				_, err = first.Execute(context.Background(), "CREATE FULLTEXT INDEX link_text FOR ()-[r:LINK]-() ON EACH [r.text]", nil)
				require.NoError(t, err)
				require.NoError(t, probe.CreateEdge(&storage.Edge{ID: "link", StartNode: "n000000", EndNode: "n000040", Type: "LINK", Properties: map[string]any{"text": "hotel"}}))
				query = "CALL db.index.fulltext.queryRelationships('link_text', 'hotel') YIELD relationship, score RETURN score"
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once atomic.Bool
			probe.crossing = func(ctx context.Context) error {
				if once.Swap(true) {
					return nil
				}
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			firstDone := make(chan error, 1)
			go func() { _, err := first.Execute(ctx, query, nil); firstDone <- err }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("population did not reach the read boundary")
			}
			waiter := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
			waiter.SetSearchService(first.search())
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer waitCancel()
			waitDone := make(chan error, 1)
			go func() { _, err := waiter.Execute(waitCtx, query, nil); waitDone <- err }()
			other := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
			other.SetSearchService(first.search())
			otherDone := make(chan error, 1)
			go func() {
				_, err := other.Execute(context.Background(), "CALL db.index.fulltext.queryNodes('other_text', 'hotel') YIELD node RETURN node.id", nil)
				otherDone <- err
			}()
			var waitErr, otherErr, firstErr error
			waitFinished, otherFinished, firstFinished := false, false, false
			select {
			case waitErr = <-waitDone:
				waitFinished = true
			case <-time.After(time.Second):
			}
			select {
			case otherErr = <-otherDone:
				otherFinished = true
			case <-time.After(time.Second):
			}
			cancel()
			select {
			case firstErr = <-firstDone:
				firstFinished = true
			case <-time.After(time.Second):
			}
			close(release)
			if !waitFinished {
				waitErr = <-waitDone
			}
			if !otherFinished {
				otherErr = <-otherDone
			}
			if !firstFinished {
				firstErr = <-firstDone
			}
			require.True(t, waitFinished, "cancelled same-projection waiter remained blocked")
			require.ErrorIs(t, waitErr, context.DeadlineExceeded)
			require.True(t, otherFinished, "another projection waited for a blocked population")
			require.NoError(t, otherErr)
			require.True(t, firstFinished, "cancellation did not terminate population")
			require.ErrorIs(t, firstErr, context.Canceled)
		})
	}
}
