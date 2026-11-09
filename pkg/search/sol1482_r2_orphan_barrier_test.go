package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
	"time"
)

func TestSol1482R2OrphanObservationCannotRemoveNewerIndex(t *testing.T) {
	runOrphanObservation(t, orphanObservationCase{
		old:     &storage.Node{ID: "source", ChunkEmbeddings: [][]float32{{1, 0}}},
		newer:   &storage.Node{ID: "source", Properties: map[string]any{"content": "currentword"}, ChunkEmbeddings: [][]float32{{0, 1}}},
		timeout: 3 * time.Second, timeoutMessage: "search did not read orphan",
		completionMessage: "an older not-found observation removed a newer live index",
		read: func(ctx context.Context, svc *Service) error {
			_, err := svc.Search(ctx, "", []float32{1, 0}, DefaultSearchOptions())
			return err
		},
	})
}
