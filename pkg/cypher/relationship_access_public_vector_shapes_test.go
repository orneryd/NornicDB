package cypher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The access owner must leave unsupported optimized shapes to the public row
// and projection owners. The single edge has an independent exact cosine of1.
func TestRelationshipAccessProgramPublicVectorScanShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_vector_shapes"))
	for _, query := range []string{
		"CREATE ()-[:VectorRead {key:'r1', vector:[1.0,0.0]}]->()",
		"CREATE VECTOR INDEX public_vector_shapes FOR ()-[r:VectorRead]-() ON (r.vector) OPTIONS {indexConfig:{`vector.dimensions`:2,`vector.similarity_function`:'cosine'}}",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	for _, test := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"typed", "MATCH ()-[r:VectorRead]->() RETURN r.key, vector.similarity.cosine(r.vector,$v) AS score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"untyped", "MATCH ()-[r]->() RETURN r.key, vector.similarity.cosine(r.vector,$v) AS score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"with_score_only", "MATCH ()-[r:VectorRead]->() WITH vector.similarity.cosine(r.vector,$v) AS score RETURN score ORDER BY score DESC LIMIT 1", [][]interface{}{{1.0}}},
		{"with_alias", "MATCH ()-[r:VectorRead]->() WITH r AS edge, vector.similarity.cosine(r.vector,$v) AS score RETURN edge.key, score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"with_extra_binding", "MATCH ()-[r:VectorRead]->() WITH r, r.key AS key, vector.similarity.cosine(r.vector,$v) AS score RETURN key, score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"with_score_expression", "MATCH ()-[r:VectorRead]->() WITH r, vector.similarity.cosine(r.vector,$v) AS score RETURN r.key, score+0.0 AS result ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"with_identity_alias", "MATCH ()-[r:VectorRead]->() WITH r AS r, vector.similarity.cosine(r.vector,$v) AS score RETURN r.key, score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"with_reordered_items", "MATCH ()-[r:VectorRead]->() WITH vector.similarity.cosine(r.vector,$v) AS score, r RETURN r.key, score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"with_endpoints", "MATCH (a)-[r:VectorRead]->(b) WITH DISTINCT r,a,b,vector.similarity.cosine(r.vector,$v) AS score RETURN r.key,score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"property_order", "MATCH ()-[r:VectorRead]->() RETURN r.key, vector.similarity.cosine(r.vector,$v) AS score ORDER BY r.key DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"compound_score", "MATCH ()-[r:VectorRead]->() WITH r, vector.similarity.cosine(r.vector,$v) AS score WHERE score>=0.0 AND score<=1.0 RETURN r.key, score ORDER BY score DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
		{"computed_order", "MATCH ()-[r:VectorRead]->() RETURN r.key, vector.similarity.cosine(r.vector,$v) AS score ORDER BY score+0.0 DESC LIMIT 1", [][]interface{}{{"r1", 1.0}}},
	} {
		for _, mode := range []string{"", "PROFILE ", "EXPLAIN "} {
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), mode+test.query, map[string]interface{}{"v": []interface{}{1.0, 0.0}})
				require.NoError(t, err)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, test.want, result.Rows)
				}
			})
		}
	}
}

type relationshipPublicVectorReadFailure struct {
	storage.Engine
	mode  string
	scans atomic.Int64
}

func (p *relationshipPublicVectorReadFailure) AllEdges() ([]*storage.Edge, error) {
	p.scans.Add(1)
	if p.mode == "all_edges" {
		return nil, errors.New("public vector relationship scan failure")
	}
	return p.Engine.AllEdges()
}

func (p *relationshipPublicVectorReadFailure) GetNode(id storage.NodeID) (*storage.Node, error) {
	if p.mode == "missing_endpoint" {
		return nil, storage.ErrNotFound
	}
	return p.Engine.GetNode(id)
}

func TestRelationshipAccessProgramPublicVectorReadFailures(t *testing.T) {
	for _, mode := range []string{"all_edges", "missing_endpoint"} {
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_vector_failures")
			setup := NewStorageExecutor(store)
			for _, query := range []string{
				"CREATE ()-[:VectorRead {key:'r1', vector:[1.0,0.0]}]->()",
				"CREATE VECTOR INDEX public_vector_failures FOR ()-[r:VectorRead]-() ON (r.vector) OPTIONS {indexConfig:{`vector.dimensions`:2,`vector.similarity_function`:'cosine'}}",
			} {
				_, err := setup.Execute(context.Background(), query, nil)
				require.NoError(t, err)
			}
			probe := &relationshipPublicVectorReadFailure{Engine: store, mode: mode}
			exec := NewStorageExecutor(probe)
			result, err := exec.Execute(context.Background(), "MATCH (a)-[r:VectorRead]->(b) RETURN r.key,vector.similarity.cosine(r.vector,$v) AS score ORDER BY score DESC LIMIT 1", map[string]interface{}{"v": []interface{}{1.0, 0.0}})
			require.NoError(t, err)
			require.Positive(t, probe.scans.Load())
			if mode == "all_edges" {
				// The optional vector route declines on a failed scan. Normal
				// traversal still has an independent, complete adjacency read.
				require.Equal(t, [][]interface{}{{"r1", 1.0}}, result.Rows)
			} else {
				require.Empty(t, result.Rows)
			}
		})
	}
}
