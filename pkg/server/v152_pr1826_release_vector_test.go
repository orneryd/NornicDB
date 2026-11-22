package server

// v1.5.2 verification (scratch, test-only): vector similarity reads in explicit
// transactions (R1 #6 of the PR #1826 review, on the release).

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestV152P26VectorInTransaction(t *testing.T) {
	e := p26Serve(t, nil)
	store, err := e.server.dbManager.GetStorage(e.database)
	if err != nil {
		t.Fatal(err)
	}
	for _, embedded := range []bool{false, true} {
		label := map[bool]string{false: "VecPlain", true: "VecEmbedded"}[embedded]
		e.must("CREATE VECTOR INDEX "+label+"_i FOR (n:"+label+") ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
		for i, emb := range [][]float64{{1, 0, 0}, {0.6, 0.8, 0}, {0, 0, 1}} {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("%s%d", label, i)), Labels: []string{label}, Properties: map[string]any{"key": fmt.Sprintf("v%d", i), "body": fmt.Sprintf("body %d", i), "emb": emb}}
			if embedded { // what the managed embedding worker stores beside the properties
				node.ChunkEmbeddings = [][]float32{{1, 2, 3}}
				node.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1}
			}
			if _, err := store.CreateNode(t.Context(), node); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(3 * time.Second)
		where := fmt.Sprintf("embedded=%t", embedded)
		const score = "vector.similarity.cosine(n.emb, [1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 1"
		for _, c := range []struct{ q, want string }{
			{"MATCH (n:L) RETURN n.key AS v, " + score, `["v0",1]`},
			{"MATCH (n:L) RETURN n.key AS v, n.emb AS e, " + score, `["v0",[1,0,0],1]`},
			{"MATCH (n:L) RETURN {node:n}.node.key AS v, " + score, `["v0",1]`},
			{"MATCH (n:L) RETURN head([n]).body AS v, " + score, `["body 0",1]`},
			{"MATCH (n:L) RETURN coalesce(n, null).body AS v, " + score, `["body 0",1]`},
			{"MATCH (n:L) RETURN size(keys({node:n}.node)) AS v, " + score, `[3,1]`},
			{"MATCH (n:L) WITH n, vector.similarity.cosine(n.emb, [1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 1 RETURN coalesce(n, null).body AS v, score", `["body 0",1]`},
		} {
			q := c.q
			for i := 0; i+2 <= len(q); i++ {
				if q[i:i+2] == ":L" {
					q = q[:i] + ":" + label + q[i+2:]
					break
				}
			}
			e.check("R6", where, q, c.want)
		}
	}
}
