// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package search

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

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
					err = store.UpdateNode(&storage.Node{ID: "concurrent:one", Properties: map[string]any{"text": fmt.Sprintf("hotel %d", i)}})
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
