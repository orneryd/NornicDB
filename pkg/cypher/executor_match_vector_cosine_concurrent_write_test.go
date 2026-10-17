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

// A filtered vector query counts the property's vector entries to choose its
// candidate set. That count read the metadata live writes change without the
// lock they hold, so a query racing index writes could crash the process with
// a concurrent map read and write.
func TestMatchVectorCosineFastPath_CandidateCountDuringConcurrentWrites(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX evidence_emb_idx FOR (n:Evidence) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	// A production-sized property index. The count reads every node with the
	// property; the query scans only Evidence, so the count dominates it and
	// index writes land while it reads.
	for index := 0; index < 20000; index++ {
		_, err = ns.CreateNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("archive-%05d", index)),
			Labels:     []string{"Archive"},
			Properties: map[string]any{"id": fmt.Sprintf("archive-%05d", index), "asset_id": "other", "embedding": []float32{1, 0, 0}},
		})
		require.NoError(t, err)
	}
	for index := 0; index < 8; index++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Evidence {id:'wanted-%03d', asset_id:'wanted', embedding:[%.2f,1.0,0.0]})", index, 0.1+float64(index)/100), nil)
		require.NoError(t, err)
	}
	live := make([]*storage.Node, 40)
	for index := range live {
		live[index] = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("live-%02d", index)),
			Labels:     []string{"Evidence"},
			Properties: map[string]any{"id": fmt.Sprintf("live-%02d", index), "asset_id": "live", "embedding": []float32{0, 1, 0}},
		}
		_, err = ns.CreateNode(live[index])
		require.NoError(t, err)
	}

	searchService := search.NewServiceWithDimensions(ns, 3)
	t.Cleanup(func() { require.NoError(t, searchService.Close()) })
	exec.SetSearchService(searchService)
	require.NoError(t, searchService.BuildIndexes(ctx))
	require.True(t, searchService.IsReady())

	// Index writes add and remove the live nodes' property vectors while the
	// query runs.
	stop := make(chan struct{})
	var writes atomic.Int64
	writerDone := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				writerDone <- nil
				return
			default:
			}
			node := live[i%len(live)]
			if err := searchService.IndexNode(node); err != nil {
				writerDone <- err
				return
			}
			if err := searchService.RemoveNode(node.ID); err != nil {
				writerDone <- err
				return
			}
			writes.Add(1)
		}
	}()

	query := `
MATCH (n:Evidence) WHERE n.asset_id = $asset
WITH n, vector.similarity.cosine(n.embedding, $q) AS score
RETURN n.id AS id, score
ORDER BY score DESC LIMIT 10`
	for i := 0; i < 50; i++ {
		// A distinct query vector per run keeps each one off the result cache.
		params := map[string]interface{}{"asset": "wanted", "q": []float64{1.0, float64(i) * 1e-6, 0.0}}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "run %d left the vector fast path", i)
		require.Len(t, result.Rows, 8)
		require.Equal(t, "wanted-007", result.Rows[0][0])
	}
	close(stop)
	require.NoError(t, <-writerDone)
	require.Positive(t, writes.Load(), "no index write ran during the queries")
}
