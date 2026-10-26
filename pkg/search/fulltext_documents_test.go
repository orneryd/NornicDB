// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package search

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every representation of a view reaches the one cancellable visitation, and a
// body-frequency count agrees through it and through the postings.
func TestFulltextDocumentsEveryRepresentationSharesVisitation(t *testing.T) {
	first, second := &FulltextDocument{Text: "hotel"}, &FulltextDocument{Text: "motel"}
	retained := func() map[string]*FulltextDocument { return map[string]*FulltextDocument{"1": first, "2": second} }
	retainedView := func(order []string, dirty bool) *fulltextDocumentView {
		view := &fulltextDocumentView{}
		view.resetDocuments(0, true)
		view.replace("1", first)
		view.replace("2", second)
		view.order, view.orderDirty = order, dirty
		return view
	}
	for name, view := range map[string]*fulltextDocumentView{
		"temporary rows":     {rows: []*FulltextDocument{first, second}, order: []string{"1", "2"}},
		"original order":     retainedView([]string{"1", "removed", "2"}, false),
		"after an increment": retainedView(nil, true),
	} {
		t.Run(name, func(t *testing.T) {
			visited := map[string]*FulltextDocument{}
			FulltextDocuments{view: view, ctx: context.Background()}.Each(func(id string, doc *FulltextDocument) { visited[id] = doc })
			require.Equal(t, retained(), visited)

			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			FulltextDocuments{view: view, ctx: cancelled}.Each(func(string, *FulltextDocument) { t.Fatal("visited after cancellation") })

			docs := FulltextDocuments{view: view, ctx: context.Background()}
			require.Equal(t, 2, docs.BodyDocumentFrequency("o"), "a term shorter than a trigram has no postings")
			require.Equal(t, 1, docs.BodyDocumentFrequency("hot"))
		})
	}
	require.Zero(t, fulltextGramCount("ab"))
	require.Equal(t, 1, fulltextGramCount("abc"))
}

func TestFulltextDocumentsMaintainsWritesWithBoundedHistory(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(&storage.Node{ID: "history:kept", Properties: map[string]any{"text": "hotel services"}})
	require.NoError(t, err)
	scope := FulltextDocumentScope{Name: "test", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
	}
	read := func() error {
		return svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(d FulltextDocuments) error {
			require.Equal(t, 1, d.Count())
			require.Equal(t, 1, d.BodyDocumentFrequency("hotel"))
			return nil
		})
	}
	require.NoError(t, read())
	// A quiet reader sees a thousand different IDs arrive and leave. The
	// maintenance journal must be bounded by resident data, not lifetime writes.
	for i := 0; i < 1000; i++ {
		id := storage.NodeID(fmt.Sprintf("history:temporary-%d", i))
		_, err = store.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"text": "hotel temporary"}})
		require.NoError(t, err)
		require.NoError(t, store.DeleteNode(id))
	}
	owner := svc.fulltextDocuments
	owner.changes.Lock()
	retained, limit := len(owner.nodes)+len(owner.edges), owner.dirtyLimit
	owner.changes.Unlock()
	require.LessOrEqual(t, retained, limit)
	t.Logf("2000 changes: retained change IDs=%d; resident-derived limit=%d", retained, limit)
	require.NoError(t, read())
}

// Author of this test: #1683 lane (Claude), 2026-10-30 (NOR-148).
// The journal is judged by the documents a change can affect: churn on
// entities that can never be documents is not journaled, a change to a current
// document is, and journaled document churn stays bounded by resident data.
func TestFulltextDocumentsJournalsOnlyRelevantChanges(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(&storage.Node{ID: "relevance:doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel services"}})
	require.NoError(t, err)
	candidate := func(node *storage.Node, _ *storage.Edge) bool {
		return node != nil && slices.Contains(node.Labels, "Doc")
	}
	scope := FulltextDocumentScope{Name: "test", Definition: "text", Candidate: candidate}
	build := func(node *storage.Node, edge *storage.Edge) *FulltextDocument {
		if !candidate(node, edge) {
			return nil
		}
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
	}
	count := func() int {
		n := -1
		require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(d FulltextDocuments) error {
			n = d.Count()
			return nil
		}))
		return n
	}
	require.Equal(t, 1, count())
	owner := svc.fulltextDocuments
	journal := func() (int, int) {
		owner.changes.Lock()
		defer owner.changes.Unlock()
		return len(owner.nodes) + len(owner.edges), owner.dirtyLimit
	}
	for i := 0; i < 1000; i++ {
		id := storage.NodeID(fmt.Sprintf("relevance:other-%d", i))
		_, err = store.CreateNode(&storage.Node{ID: id, Labels: []string{"Other"}, Properties: map[string]any{"text": "hotel other"}})
		require.NoError(t, err)
		require.NoError(t, store.DeleteNode(id))
	}
	retained, _ := journal()
	require.Zero(t, retained, "changes to entities that can never be documents must not be journaled")
	node, err := store.GetNode("relevance:doc")
	require.NoError(t, err)
	node.Labels = []string{"Other"}
	require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: node}))
	retained, _ = journal()
	require.Equal(t, 1, retained, "a change to a current document must be journaled")
	require.Equal(t, 0, count(), "the relabelled document must be retired")
	for i := 0; i < 1000; i++ {
		id := storage.NodeID(fmt.Sprintf("relevance:doc-%d", i))
		_, err = store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel temporary"}})
		require.NoError(t, err)
		require.NoError(t, store.DeleteNode(id))
	}
	retained, limit := journal()
	require.LessOrEqual(t, retained, limit, "journaled document churn must stay bounded by resident data")
	require.Equal(t, 0, count())
}

// Author of this test: #1683 lane (Claude), 2026-10-30 (NOR-148, audit #33).
// A read in flight does not widen the journal. While a read populates its
// view, one transaction deletes a document that read's snapshot holds and
// creates far more unrelated entities than the index holds documents, in
// either phase of the read (populating, or serving after publication). The
// unrelated entities must stay out of the journal's bound, so the next read
// refreshes by ID instead of rereading the store, and the deleted document,
// which the read published, must still be retired.
func TestFulltextDocumentsInFlightReadJournalsOnlyRelevantChanges(t *testing.T) {
	for _, phase := range []string{"populating", "serving"} {
		t.Run(phase, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			svc := NewService(store, DefaultVectorDimensions, nil)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			const docs = 50
			for i := 0; i < docs; i++ {
				_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("inflight:doc-%02d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel services"}})
				require.NoError(t, err)
			}
			candidate := func(node *storage.Node, _ *storage.Edge) bool {
				return node != nil && slices.Contains(node.Labels, "Doc")
			}
			pending, built := true, 0
			write := func() {
				if !pending {
					return
				}
				pending = false
				tx, err := store.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.DeleteGraphNode("inflight:doc-07", storage.CascadeEdges))
				for i := 0; i < 1000; i++ {
					_, err := tx.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("inflight:other-%04d", i)), Labels: []string{"Other"}, Properties: map[string]any{"text": "hotel other"}})
					require.NoError(t, err)
				}
				require.NoError(t, tx.Commit())
			}
			scope := FulltextDocumentScope{Name: "test", Definition: "text", Candidate: candidate}
			build := func(node *storage.Node, edge *storage.Edge) *FulltextDocument {
				built++
				if phase == "populating" {
					write()
				}
				if !candidate(node, edge) {
					return nil
				}
				text, _ := node.Properties["text"].(string)
				return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
			}
			ids := func() []string {
				var got []string
				require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(d FulltextDocuments) error {
					d.Each(func(id string, _ *FulltextDocument) { got = append(got, id) })
					if phase == "serving" {
						write()
					}
					return nil
				}))
				slices.Sort(got)
				return got
			}
			require.Len(t, ids(), docs, "the first read serves its snapshot, which still holds the deleted document")
			require.False(t, pending, "the transaction must commit while the first read is in flight")
			owner := svc.fulltextDocuments
			owner.changes.Lock()
			retained, reset := len(owner.nodes)+len(owner.edges), owner.reset
			owner.changes.Unlock()
			require.Zero(t, reset, "unrelated entities written during a read must not exhaust the journal")
			require.Equal(t, 1, retained, "only the change to a published document is journaled")
			built = 0
			second := ids()
			require.Len(t, second, docs-1)
			require.NotContains(t, second, "inflight:doc-07", "the deleted document must be retired")
			require.Zero(t, built, "the next read refreshes by ID; it must not rebuild the corpus")
		})
	}
}

// Author of this test: #1683 lane (Claude), 2026-10-31 (NOR-148, audit #34).
// An in-flight read's deferred changes are bounded. One transaction committed
// during a population deletes a document the read's snapshot holds and creates
// more unrelated entities than the deferral limit. The read keeps at most the
// limit while it runs and releases them when it overflows; the journal does
// not grow; and the next read repopulates instead of refreshing by ID, so the
// deleted document is still retired.
func TestFulltextDocumentsInFlightDeferralIsBounded(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	const docs = 50
	for i := 0; i < docs; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("bounded:doc-%02d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel services"}})
		require.NoError(t, err)
	}
	candidate := func(node *storage.Node, _ *storage.Edge) bool {
		return node != nil && slices.Contains(node.Labels, "Doc")
	}
	pending, built, peak, overflowed := true, 0, 0, false
	scope := FulltextDocumentScope{Name: "test", Definition: "text", Candidate: candidate}
	build := func(node *storage.Node, edge *storage.Edge) *FulltextDocument {
		if pending {
			pending = false
			tx, err := store.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.DeleteGraphNode("bounded:doc-07", storage.CascadeEdges))
			for i := 0; i < fulltextDeferredFloor+100; i++ {
				_, err := tx.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("bounded:other-%05d", i)), Labels: []string{"Other"}, Properties: map[string]any{"text": "hotel other"}})
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit())
			owner := svc.fulltextDocuments // created by this first read
			owner.changes.Lock()
			projection := owner.views["test"]
			peak, overflowed = len(projection.deferred), projection.deferredOverflow
			require.LessOrEqual(t, len(projection.deferred), owner.deferredLimit())
			owner.changes.Unlock()
		}
		if !candidate(node, edge) {
			return nil
		}
		built++ // documents built, not entities read
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
	}
	ids := func() []string {
		var got []string
		require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(d FulltextDocuments) error {
			d.Each(func(id string, _ *FulltextDocument) { got = append(got, id) })
			return nil
		}))
		slices.Sort(got)
		return got
	}
	require.Len(t, ids(), docs, "the first read serves its snapshot, which still holds the deleted document")
	require.False(t, pending, "the transaction must commit while the first read is in flight")
	require.True(t, overflowed, "more unrelated changes than the limit must overflow the read's deferral")
	require.Zero(t, peak, "an overflowed read releases its deferred changes")
	owner := svc.fulltextDocuments
	owner.changes.Lock()
	projection := owner.views["test"]
	held, overflow, reset := len(projection.deferred), projection.deferredOverflow, owner.reset
	retained := len(owner.nodes) + len(owner.edges)
	owner.changes.Unlock()
	require.Zero(t, held, "a returned read holds no deferred changes")
	require.False(t, overflow)
	require.Zero(t, reset, "unrelated entities must not reset the journal")
	require.Zero(t, retained, "an overflowed read journals nothing")
	built = 0
	second := ids()
	require.Len(t, second, docs-1)
	require.NotContains(t, second, "bounded:doc-07", "the deleted document must be retired")
	require.Equal(t, docs-1, built, "the overflowed view is populated again, not refreshed by ID")
}

func TestFulltextDocumentsConcurrentWritesAndReaders(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(&storage.Node{ID: "concurrent:one", Properties: map[string]any{"text": "hotel"}})
	require.NoError(t, err)
	scope := FulltextDocumentScope{Name: "test", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: 1}
	}
	read := func() error {
		return svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(d FulltextDocuments) error {
			if d.Count() != 1 {
				return fmt.Errorf("lost document: %d", d.Count())
			}
			return nil
		})
	}
	require.NoError(t, read())
	var workers sync.WaitGroup
	failures := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(writer bool) {
			defer workers.Done()
			for i := 0; i < 60; i++ {
				var err error
				if writer {
					err = store.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "concurrent:one", Properties: map[string]any{"text": fmt.Sprintf("hotel %d", i)}}})
				} else {
					err = read()
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}(worker == 0)
	}
	workers.Wait()
	close(failures)
	for failure := range failures {
		require.NoError(t, failure)
	}
	require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(d FulltextDocuments) error {
		require.Equal(t, 1, d.BodyDocumentFrequency("hotel 59"))
		require.Zero(t, d.BodyDocumentFrequency("hotel 58"))
		return nil
	}))
}

// Fresh statistics cannot amortize construction of a retained posting index.
// They still consume the canonical builder and exact body-frequency predicate.
func TestFulltextDocumentsFreshConstruction(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for id, text := range map[storage.NodeID]string{"fresh:a": "hotel alpha", "fresh:b": "routine"} {
		_, err := store.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"text": text}})
		require.NoError(t, err)
	}
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	built := 0
	for call := 0; call < 2; call++ {
		err := svc.ReadFulltextDocuments(context.Background(), store, FulltextDocumentScope{Name: "fresh", Definition: "text", Fresh: true}, func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
			built++
			text := node.Properties["text"].(string)
			return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
		}, func(corpus FulltextDocuments) error {
			require.Nil(t, corpus.view.documents)
			require.Nil(t, corpus.view.postings)
			require.Len(t, corpus.view.rows, 2)
			require.Equal(t, 2, corpus.Count())
			require.Equal(t, 1, corpus.BodyDocumentFrequency("hotel"))
			require.Equal(t, 1, corpus.BodyDocumentFrequency("rou"))
			require.Equal(t, 2, corpus.BodyDocumentFrequency("o"))
			var ids []string
			corpus.Each(func(id string, _ *FulltextDocument) { ids = append(ids, id) })
			require.ElementsMatch(t, []string{"fresh:a", "fresh:b"}, ids)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2*(call+1), built)
	}
}
