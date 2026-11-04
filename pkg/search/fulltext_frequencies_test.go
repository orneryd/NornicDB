// Author: #1683 review-correction lane (DeepSeek), 2026-11-02 (NOR-148).
package search

import (
	"context"
	"strings"
	"testing"
	"time"
	"unsafe"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fulltextCountingContext counts context reads, so these tests can tell one
// body pass from a retained-count reuse without timing or private state.
type fulltextCountingContext struct {
	context.Context
	checks int
}

func (c *fulltextCountingContext) Err() error {
	c.checks++
	return c.Context.Err()
}

// BodyDocumentFrequencies keeps the release's strings.Contains meaning for
// every unique term: duplicate requests collapse, the empty term counts every
// document, and Unicode terms match by their exact bytes. One pass visits each
// document once, and an identical term set reuses the retained counts.
func TestFulltextBodyDocumentFrequenciesContract(t *testing.T) {
	view := newFulltextDocumentView("text", false, nil)
	view.resetDocuments(0, true)
	for id, text := range map[string]string{
		"uni:1": "Hôtel Zürich straße",
		"uni:2": "hotel motel 𝄞",
		"uni:3": "hotel",
	} {
		require.NoError(t, view.replace(t.Context(), id, &FulltextDocument{Text: text, Tokens: 1}))
	}
	view.orderDirty = true // enumerate the retained map, as a view after an increment does
	docs := FulltextDocuments{view: view}
	terms := []string{"hotel", "hotel", "Hôtel", "straße", "𝄞", "mot", ""}
	expected := map[string]int{"hotel": 2, "Hôtel": 1, "straße": 1, "𝄞": 1, "mot": 1, "": 3}

	counter := &fulltextCountingContext{Context: t.Context()}
	frequencies, err := docs.BodyDocumentFrequencies(counter, terms)
	require.NoError(t, err)
	require.Equal(t, expected, frequencies, "each unique term keeps its exact substring count")
	require.Equal(t, 1+view.documentCount(), counter.checks, "one pass checks cancellation once per document")

	// The same term set is answered from the retained counts: no body pass, and
	// no duplicate entry per repeated term.
	counter = &fulltextCountingContext{Context: t.Context()}
	reused, err := docs.BodyDocumentFrequencies(counter, []string{"", "mot", "𝄞", "straße", "Hôtel", "hotel"})
	require.NoError(t, err)
	require.Equal(t, expected, reused, "the retained counts are the term set's, in any order")
	require.Equal(t, 1, counter.checks, "a reused term set reads no document")
	require.Len(t, view.frequencies, len(expected), "the retained set holds one entry per unique term")

	// The result belongs to the caller: mutating it cannot reach the retained
	// counts or a later result.
	frequencies["hotel"] = 99
	delete(frequencies, "mot")
	frequencies["injected"] = 7
	again, err := docs.BodyDocumentFrequencies(t.Context(), terms)
	require.NoError(t, err)
	require.Equal(t, expected, again, "a caller's map mutation must not change the retained counts")
}

// A document replacement, update or deletion changes the counts the view
// answers, so the retained set never describes a corpus that is gone.
func TestFulltextBodyDocumentFrequenciesFollowUpdatesAndDeletes(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	for id, text := range map[storage.NodeID]string{"freq:a": "hotel alpha", "freq:b": "hotel beta"} {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Properties: map[string]any{"text": text}})
		require.NoError(t, err)
	}
	scope := FulltextDocumentScope{Name: "frequencies", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		text, _ := node.Properties["text"].(string)
		return &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}
	}
	count := func() map[string]int {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !storage.EventNotificationsSettled(store); {
			require.True(t, time.Now().Before(deadline))
			time.Sleep(time.Millisecond)
		}
		var frequencies map[string]int
		require.NoError(t, svc.ReadFulltextDocuments(context.Background(), store, scope, build, func(ctx context.Context, corpus FulltextDocuments) error {
			counts, err := corpus.BodyDocumentFrequencies(ctx, []string{"hotel", "motel", "alpha", "beta"})
			if err != nil {
				return err
			}
			frequencies = counts
			return nil
		}))
		return frequencies
	}
	require.Equal(t, map[string]int{"hotel": 2, "motel": 0, "alpha": 1, "beta": 1}, count())

	require.NoError(t, store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: "freq:b", Properties: map[string]any{"text": "motel beta"}}}))
	require.Equal(t, map[string]int{"hotel": 1, "motel": 1, "alpha": 1, "beta": 1}, count(), "an update must change the counts")

	require.NoError(t, store.DeleteNode(t.Context(), "freq:a"))
	require.Equal(t, map[string]int{"hotel": 0, "motel": 1, "alpha": 0, "beta": 1}, count(), "a deletion must change the counts")

	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "freq:c", Properties: map[string]any{"text": "hotel alpha beta"}})
	require.NoError(t, err)
	require.Equal(t, map[string]int{"hotel": 1, "motel": 1, "alpha": 1, "beta": 2}, count(), "a new document must change the counts")
}

// The view retains the most recent term set only, so this counting cache is
// bounded by one query's unique terms rather than by every term a session has
// asked for.
func TestFulltextBodyDocumentFrequenciesRetainsOneTermSet(t *testing.T) {
	view := newFulltextDocumentView("text", false, nil)
	view.resetDocuments(0, true)
	for id, text := range map[string]string{"set:1": "hotel alpha beta", "set:2": "hotel gamma"} {
		require.NoError(t, view.replace(t.Context(), id, &FulltextDocument{Text: text, Tokens: len(strings.Fields(text))}))
	}
	view.orderDirty = true // enumerate the retained map, as a view after an increment does
	docs := FulltextDocuments{view: view}
	wide := []string{"alpha", "beta", "gamma", "delta"}
	wideCounts := map[string]int{"alpha": 1, "beta": 1, "gamma": 1, "delta": 0}
	scan := func(terms []string, expected map[string]int) {
		t.Helper()
		counter := &fulltextCountingContext{Context: t.Context()}
		frequencies, err := docs.BodyDocumentFrequencies(counter, terms)
		require.NoError(t, err)
		require.Equal(t, expected, frequencies)
		require.Equal(t, 1+view.documentCount(), counter.checks, "a new term set counts the bodies again")
	}
	narrow := []string{"gamma", "alpha"}
	narrowCounts := map[string]int{"gamma": 1, "alpha": 1}
	scan(wide, wideCounts)
	require.Len(t, view.frequencies, len(wide))
	scan(narrow, narrowCounts)
	require.Len(t, view.frequencies, len(narrow), "only the most recent term set is retained")
	for _, entry := range view.frequencies {
		require.Contains(t, narrow, entry.term, "a replaced term set leaves no entry behind")
	}

	// The wider set is no longer retained, so it counts again and replaces the
	// narrow one, never accumulating both.
	scan(wide, wideCounts)
	require.Len(t, view.frequencies, len(wide))
	counter := &fulltextCountingContext{Context: t.Context()}
	reused, err := docs.BodyDocumentFrequencies(counter, wide)
	require.NoError(t, err)
	require.Equal(t, wideCounts, reused)
	require.Equal(t, 1, counter.checks, "the most recent set is reused without a body pass")
}

// Canceled work counts nothing and installs nothing: a partial body pass never
// becomes a successful result or a cached one.
func TestFulltextBodyDocumentFrequenciesCanceledReadCountsNothing(t *testing.T) {
	view := newFulltextDocumentView("text", false, nil)
	view.resetDocuments(0, true)
	require.NoError(t, view.replace(t.Context(), "one", &FulltextDocument{Text: "orchard apple"}))
	view.orderDirty = true
	docs := FulltextDocuments{view: view}
	retained, err := docs.BodyDocumentFrequencies(t.Context(), []string{"orchard"})
	require.NoError(t, err)
	require.Equal(t, map[string]int{"orchard": 1}, retained)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	frequencies, err := docs.BodyDocumentFrequencies(ctx, []string{"apple"})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, frequencies, "a canceled count returns no partial result")
	require.Equal(t, []fulltextFrequency{{term: "orchard", count: 1}}, view.frequencies,
		"a failed count neither replaces nor extends the retained set")
}

// Retained query terms must not pin a large request buffer. Request-local views
// keep no frequency cache at all, even though their returned counts are equal.
func TestFulltextBodyDocumentFrequenciesOwnsOnlyRetainedTerms(t *testing.T) {
	backing := strings.Repeat("query padding ", 8192) + "hotel"
	term := backing[len(backing)-len("hotel"):]
	for _, retained := range []bool{true, false} {
		t.Run(map[bool]string{true: "retained", false: "request-local"}[retained], func(t *testing.T) {
			view := newFulltextDocumentView("text", false, nil)
			view.resetDocuments(0, retained)
			require.NoError(t, view.replace(t.Context(), "one", &FulltextDocument{Text: "hotel"}))
			view.order = []string{"one"}
			counts, err := (FulltextDocuments{view: view}).BodyDocumentFrequencies(t.Context(), []string{term})
			require.NoError(t, err)
			require.Equal(t, map[string]int{"hotel": 1}, counts)
			if !retained {
				require.Empty(t, view.frequencies)
				return
			}
			require.Equal(t, []fulltextFrequency{{term: "hotel", count: 1}}, view.frequencies)
			require.False(t, unsafe.StringData(term) == unsafe.StringData(view.frequencies[0].term),
				"a cached substring must own its small string, not the large request backing buffer")
			require.Equal(t, int64(24+len(term)), fulltextFrequencyBytes(view.frequencies))
		})
	}
}
