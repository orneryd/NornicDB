// Author: Codex implementation lane, 2026-10-27 (NOR-148 S24 lifetime/retention).
package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fulltextLifetimeStore struct {
	*fulltextCountingStore
	subscriptions atomic.Int64
}

func (s *fulltextLifetimeStore) OwnsSnapshotReader(issued storage.Engine) bool { return issued == s }
func (s *fulltextLifetimeStore) SubscribeEvents(handlers storage.EventHandlers) error {
	s.subscriptions.Add(1)
	return s.NamespacedEngine.SubscribeEvents(handlers)
}

func TestFulltextMaintainedPublicTransientServiceRetention(t *testing.T) {
	for _, managed := range []bool{false, true} {
		t.Run(fmt.Sprintf("managed=%t", managed), func(t *testing.T) {
			_, counted := fulltextMaintainedFixture(t, 80)
			store := &fulltextLifetimeStore{fulltextCountingStore: counted}
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			var service *search.Service
			if managed {
				service = search.NewService(store, search.DefaultVectorDimensions, nil)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
			}
			exec.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return service })
			_, err := exec.Execute(context.Background(), "CREATE FULLTEXT INDEX link_text FOR ()-[r:LINK]-() ON EACH [r.text]", nil)
			require.NoError(t, err)
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: "link", StartNode: "n000000", EndNode: "n000040", Type: "LINK", Properties: map[string]any{"id": "link", "text": "hotel"}}))
			for _, entity := range []string{"nodes", "relationships"} {
				proc, index, column := "queryNodes", "source_text", "node"
				if entity == "relationships" {
					proc, index, column = "queryRelationships", "link_text", "relationship"
				}
				for _, text := range []string{"hotel", "*", "text:*"} {
					query := fmt.Sprintf("CALL db.index.fulltext.%s('%s',$q) YIELD %s,score RETURN %s.id,score", proc, index, column, column)
					var first [][]any
					for call := 0; call < 20; call++ {
						result, err := exec.Execute(context.Background(), query, map[string]any{"q": text})
						require.NoError(t, err)
						require.NotEmpty(t, result.Rows)
						if call == 0 {
							first = result.Rows
						} else {
							require.Equal(t, first, result.Rows)
						}
					}
				}
			}
			expected := int64(0)
			if managed {
				expected = 1
			}
			require.Equal(t, expected, store.subscriptions.Load(), "per-statement services must not accumulate engine callbacks; a retained owner subscribes once")
		})
	}
}
