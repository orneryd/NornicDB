package nornicdb

// Untracked probe adapters (batch 1b, item 2), final build API.

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func ei1bCreate(e storage.Engine, n *storage.Node) error {
	_, err := e.CreateNode(context.Background(), n)
	return err
}

func ei1bGet(e storage.Engine, id storage.NodeID) (*storage.Node, error) {
	return e.GetNode(context.Background(), id)
}

func ei1bUpdate(w *storage.WALEngine, n *storage.Node) error {
	return w.UpdateNode(context.Background(), storage.NodeUpdateRequest{After: n})
}

func ei1bSidecar(w *storage.WALEngine, n *storage.Node) error {
	return w.UpdateNodeEmbeddingSidecar(context.Background(), n)
}

func ei1bRecover(dst storage.Engine, dir, snap string) (int, int, error) {
	r, _, err := storage.RecoverIntoEngine(context.Background(), dst, dir, snap)
	return r.Applied, r.Failed, err
}

func ei1bSearchService(e storage.Engine, dims int) (*search.Service, error) {
	svc := search.NewService(e, dims, nil)
	return svc, svc.BuildIndexes(context.Background())
}

func ei1bVectorSearch(svc *search.Service, v []float32) ([]string, error) {
	resp, err := svc.Search(context.Background(), "", v, &search.SearchOptions{Limit: 10})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range resp.Results {
		out = append(out, fmt.Sprintf("%s sim=%.4f", r.ID, r.Similarity))
	}
	return out, nil
}

func ei1bFind(w *EmbedWorker) string {
	n, err := w.findNodeWithoutEmbedding(context.Background())
	if n == nil {
		return fmt.Sprintf("none (err=%v)", err)
	}
	return fmt.Sprintf("%s (err=%v)", n.ID, err)
}

func ei1bScan(e storage.Engine) string {
	n, err := storage.FindNodeNeedingEmbedding(context.Background(), e)
	if n == nil {
		return fmt.Sprintf("none (err=%v)", err)
	}
	return fmt.Sprintf("%s (err=%v)", n.ID, err)
}
