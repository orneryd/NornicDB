package nornicdb

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// vectorWorkFallbackCounts snapshots how often each storage owner site had to
// admit vector work that arrived without the request's admission.
func vectorWorkFallbackCounts() map[string]int64 {
	counts := map[string]int64{}
	for _, fallback := range storage.VectorWorkFallbacks() {
		counts[fallback.Site] = fallback.Count
	}
	return counts
}

// requireNoFallbackAdmission runs drive and fails if any storage owner site
// admitted vector work on its own meanwhile; it logs every site that fired.
func requireNoFallbackAdmission(t *testing.T, root string, drive func(t *testing.T)) {
	t.Helper()
	before := vectorWorkFallbackCounts()
	drive(t)
	fired := map[string]int64{}
	for site, count := range vectorWorkFallbackCounts() {
		if delta := count - before[site]; delta != 0 {
			fired[site] = delta
		}
	}
	record, _ := json.Marshal(map[string]any{"root": root, "fired": fired})
	t.Logf("VECTOR_WORK_REQUEST_ROOT %s", record)
	require.Empty(t, fired, "request root %s reached a storage owner without its admission", root)
}

// Census (b) for the embedded DB API: each write entry is driven through its
// public method, and no storage owner may admit on its own meanwhile.
func TestVectorWorkRequestRootsDB(t *testing.T) {
	db, _ := openConsumerDB(t, false)
	db.bgWg.Wait()
	ctx := context.Background()
	const root = "(*github.com/orneryd/nornicdb/pkg/nornicdb.DB)."
	vector := []float64{1, 0, 0}
	var first, second *Node
	requireNoFallbackAdmission(t, root+"CreateNode", func(t *testing.T) {
		var err error
		first, err = db.CreateNode(ctx, []string{"Doc"}, map[string]interface{}{"content": "first", "embedding": vector})
		require.NoError(t, err)
	})
	requireNoFallbackAdmission(t, root+"CreateNodeWithID", func(t *testing.T) {
		var err error
		second, err = db.CreateNodeWithID(ctx, "second", []string{"Doc"}, map[string]interface{}{"content": "second"})
		require.NoError(t, err)
	})
	requireNoFallbackAdmission(t, root+"UpdateNode", func(t *testing.T) {
		_, err := db.UpdateNode(ctx, first.ID, map[string]interface{}{"content": "updated", "embedding": vector})
		require.NoError(t, err)
	})
	var edge *GraphEdge
	requireNoFallbackAdmission(t, root+"CreateEdge", func(t *testing.T) {
		var err error
		edge, err = db.CreateEdge(ctx, first.ID, second.ID, "LINKS", map[string]interface{}{"w": int64(1)})
		require.NoError(t, err)
	})
	requireNoFallbackAdmission(t, root+"DeleteEdge", func(t *testing.T) {
		require.NoError(t, db.DeleteEdge(ctx, edge.ID))
	})
	requireNoFallbackAdmission(t, root+"RecordConsent", func(t *testing.T) {
		require.NoError(t, db.RecordConsent(ctx, &Consent{UserID: "user-1", Purpose: "marketing", Given: true, Source: "api"}))
	})
	requireNoFallbackAdmission(t, root+"RevokeConsent", func(t *testing.T) {
		require.NoError(t, db.RevokeConsent(ctx, "user-1", "marketing"))
	})
	requireNoFallbackAdmission(t, root+"AnonymizeUserData", func(t *testing.T) {
		require.NoError(t, db.AnonymizeUserData(ctx, "user-1"))
	})
	// Every Cypher write shape, on the DB's own storage stack.
	requireNoFallbackAdmission(t, root+"ExecuteCypher", func(t *testing.T) {
		for _, query := range []string{
			"CREATE (a:Doc {content: 'cypher', embedding: $v})-[:R {w: 1}]->(b:Doc {content: 'b'})",
			"MATCH (n:Doc {content: 'cypher'}) SET n.title = 'x', n.embedding = $v",
			"MATCH (a:Doc {content: 'cypher'}) MERGE (c:Doc {content: 'c'}) MERGE (a)-[:M]->(c)",
			"MATCH (:Doc {content: 'cypher'})-[r:R]->() SET r.w = 2",
			"UNWIND range(1, 3) AS i CREATE (:Bulk {i: i})",
			"MATCH (n:Bulk) DELETE n",
			"CREATE (:Pipe {i: 1})-[:PR]->(:Pipe {i: 2})",
			"MATCH ()-[r:PR]->() WITH r DELETE r",
			"MATCH (n:Pipe) WITH n DELETE n",
			"MATCH (n:Doc {content: 'b'}) CALL kp.suppress(n)",
			"MATCH (:Doc {content: 'cypher'})-[r:R]->() CALL kp.suppress(r)",
			"MATCH (n:Doc {content: 'c'}) DETACH DELETE n",
		} {
			_, err := db.ExecuteCypher(ctx, query, map[string]interface{}{"v": vector})
			require.NoError(t, err, query)
		}
	})
	requireNoFallbackAdmission(t, root+"DeleteNode", func(t *testing.T) {
		require.NoError(t, db.DeleteNode(ctx, second.ID))
	})
	backup := filepath.Join(t.TempDir(), "backup.json")
	restore, err := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"id": "restored", "labels": []string{"Doc"}, "properties": map[string]any{"content": "restored"}}},
		"edges": []map[string]any{},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(backup, restore, 0o600))
	requireNoFallbackAdmission(t, root+"Restore", func(t *testing.T) {
		require.NoError(t, db.Restore(ctx, backup))
	})
}
