// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package search

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every representation of a view reaches the one cancellable visitation, and a
// body-frequency count agrees through it.
func TestFulltextDocumentsEveryRepresentationSharesVisitation(t *testing.T) {
	first, second := &FulltextDocument{Text: "hotel"}, &FulltextDocument{Text: "motel"}
	retained := func() map[string]*FulltextDocument { return map[string]*FulltextDocument{"1": first, "2": second} }
	retainedView := func(order []string, dirty bool) *fulltextDocumentView {
		view := &fulltextDocumentView{}
		view.resetDocuments(0, true)
		require.NoError(t, view.replace(t.Context(), "1", first))
		require.NoError(t, view.replace(t.Context(), "2", second))
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
			require.NoError(t, FulltextDocuments{view: view}.Each(t.Context(), func(id string, doc *FulltextDocument) error {
				visited[id] = doc
				return nil
			}))
			require.Equal(t, retained(), visited)

			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, FulltextDocuments{view: view}.Each(cancelled, func(string, *FulltextDocument) error {
				t.Fatal("visited after cancellation")
				return nil
			}), context.Canceled)

			docs := FulltextDocuments{view: view}
			frequencies, err := docs.BodyDocumentFrequencies(t.Context(), []string{"o", "hot"})
			require.NoError(t, err)
			require.Equal(t, map[string]int{"o": 2, "hot": 1}, frequencies, "a term shorter than any index unit keeps the substring count")
		})
	}
}

// A body-frequency count is exact strings.Contains per document, and a
// document replacement changes it: the corpus the counts described is gone.
func TestFulltextBodyDocumentFrequenciesFollowDocumentChanges(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "postings:one", Properties: map[string]any{"text": "hotel services"}})
	require.NoError(t, err)
	scope := FulltextDocumentScope{Name: "test", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		text := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
	}
	read := func(check func(context.Context, FulltextDocuments)) {
		for deadline := time.Now().Add(10 * time.Second); !storage.EventNotificationsSettled(store); {
			require.True(t, time.Now().Before(deadline))
			time.Sleep(time.Millisecond)
		}
		require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(ctx context.Context, corpus FulltextDocuments) error {
			check(ctx, corpus)
			return nil
		}))
	}
	read(func(ctx context.Context, corpus FulltextDocuments) {
		frequencies, err := corpus.BodyDocumentFrequencies(ctx, []string{"hotel", "motel"})
		require.NoError(t, err)
		require.Equal(t, map[string]int{"hotel": 1, "motel": 0}, frequencies)
	})
	// The reuse of an unchanged corpus answers the same counts.
	read(func(ctx context.Context, corpus FulltextDocuments) {
		frequencies, err := corpus.BodyDocumentFrequencies(ctx, []string{"motel", "hotel"})
		require.NoError(t, err)
		require.Equal(t, map[string]int{"hotel": 1, "motel": 0}, frequencies, "an unchanged corpus keeps exact counts, in any term order")
	})
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "postings:two", Properties: map[string]any{"text": "motel services"}})
	require.NoError(t, err)
	read(func(ctx context.Context, corpus FulltextDocuments) {
		frequencies, err := corpus.BodyDocumentFrequencies(ctx, []string{"motel", "services"})
		require.NoError(t, err)
		require.Equal(t, map[string]int{"motel": 1, "services": 2}, frequencies)
	})
}

func TestFulltextDocumentsMaintainsWritesWithBoundedHistory(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "history:kept", Properties: map[string]any{"text": "hotel services"}})
	require.NoError(t, err)
	scope := FulltextDocumentScope{Name: "test", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
	}
	read := func() error {
		return svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(ctx context.Context, d FulltextDocuments) error {
			require.Equal(t, 1, d.Count())
			frequencies, err := d.BodyDocumentFrequencies(ctx, []string{"hotel"})
			require.NoError(t, err)
			require.Equal(t, 1, frequencies["hotel"])
			return nil
		})
	}
	require.NoError(t, read())
	// A quiet reader sees a thousand different IDs arrive and leave. The
	// maintenance journal must be bounded by resident data, not lifetime writes.
	for i := 0; i < 1000; i++ {
		id := storage.NodeID(fmt.Sprintf("history:temporary-%d", i))
		_, err = store.CreateNode(t.Context(), &storage.Node{ID: id, Properties: map[string]any{"text": "hotel temporary"}})
		require.NoError(t, err)
		require.NoError(t, store.DeleteNode(t.Context(), id))
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
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "relevance:doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel services"}})
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
		require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(_ context.Context, d FulltextDocuments) error {
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
		_, err = store.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Other"}, Properties: map[string]any{"text": "hotel other"}})
		require.NoError(t, err)
		require.NoError(t, store.DeleteNode(t.Context(), id))
	}
	retained, _ := journal()
	require.Zero(t, retained, "changes to entities that can never be documents must not be journaled")
	node, err := store.GetNode(t.Context(), "relevance:doc")
	require.NoError(t, err)
	node.Labels = []string{"Other"}
	require.NoError(t, store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
	retained, _ = journal()
	require.Equal(t, 1, retained, "a change to a current document must be journaled")
	require.Equal(t, 0, count(), "the relabelled document must be retired")
	for i := 0; i < 1000; i++ {
		id := storage.NodeID(fmt.Sprintf("relevance:doc-%d", i))
		_, err = store.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel temporary"}})
		require.NoError(t, err)
		require.NoError(t, store.DeleteNode(t.Context(), id))
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
			store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			svc := NewService(store, DefaultVectorDimensions, nil)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			const docs = 50
			for i := 0; i < docs; i++ {
				_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("inflight:doc-%02d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel services"}})
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
				tx, err := store.BeginTransaction(t.Context())
				require.NoError(t, err)
				require.NoError(t, tx.DeleteGraphNode(t.Context(), "inflight:doc-07", storage.CascadeEdges))
				for i := 0; i < 1000; i++ {
					_, err := tx.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("inflight:other-%04d", i)), Labels: []string{"Other"}, Properties: map[string]any{"text": "hotel other"}})
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
				require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(ctx context.Context, d FulltextDocuments) error {
					require.NoError(t, d.Each(ctx, func(id string, _ *FulltextDocument) error {
						got = append(got, id)
						return nil
					}))
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
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	const docs = 50
	for i := 0; i < docs; i++ {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("bounded:doc-%02d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel services"}})
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
			tx, err := store.BeginTransaction(t.Context())
			require.NoError(t, err)
			require.NoError(t, tx.DeleteGraphNode(t.Context(), "bounded:doc-07", storage.CascadeEdges))
			for i := 0; i < fulltextDeferredFloor+100; i++ {
				_, err := tx.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("bounded:other-%05d", i)), Labels: []string{"Other"}, Properties: map[string]any{"text": "hotel other"}})
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
		require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(ctx context.Context, d FulltextDocuments) error {
			return d.Each(ctx, func(id string, _ *FulltextDocument) error {
				got = append(got, id)
				return nil
			})
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
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "concurrent:one", Properties: map[string]any{"text": "hotel"}})
	require.NoError(t, err)
	scope := FulltextDocumentScope{Name: "test", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: 1}
	}
	read := func() error {
		return svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(_ context.Context, d FulltextDocuments) error {
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
					err = store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: "concurrent:one", Properties: map[string]any{"text": fmt.Sprintf("hotel %d", i)}}})
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
	require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(ctx context.Context, d FulltextDocuments) error {
		frequencies, err := d.BodyDocumentFrequencies(ctx, []string{"hotel 59", "hotel 58"})
		require.NoError(t, err)
		require.Equal(t, 1, frequencies["hotel 59"])
		require.Zero(t, frequencies["hotel 58"])
		return nil
	}))
}

// Fresh statistics are request-scoped: they consume the canonical builder and
// the exact body-frequency predicate, and retain no counts after the read.
func TestFulltextDocumentsFreshConstruction(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for id, text := range map[storage.NodeID]string{"fresh:a": "hotel alpha", "fresh:b": "routine"} {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Properties: map[string]any{"text": text}})
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
		}, func(ctx context.Context, corpus FulltextDocuments) error {
			require.Nil(t, corpus.view.documents)
			require.Len(t, corpus.view.rows, 2)
			require.Equal(t, 2, corpus.Count())
			frequencies, err := corpus.BodyDocumentFrequencies(ctx, []string{"hotel", "rou", "o"})
			require.NoError(t, err)
			require.Equal(t, map[string]int{"hotel": 1, "rou": 1, "o": 2}, frequencies)
			require.Nil(t, corpus.view.frequencies, "a request-scoped view retains no counts")
			var ids []string
			require.NoError(t, corpus.Each(ctx, func(id string, _ *FulltextDocument) error {
				ids = append(ids, id)
				return nil
			}))
			require.ElementsMatch(t, []string{"fresh:a", "fresh:b"}, ids)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2*(call+1), built)
	}
}
