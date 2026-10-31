package cypher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type censusStorageResolver struct{ engine storage.Engine }

func (r censusStorageResolver) GetStorage(string) (storage.Engine, error) { return r.engine, nil }

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

// Census (b) for the Cypher request roots: every write path the static census
// cannot separate (the statement executor writes through its storage handle)
// is driven through the public entry, and no storage owner may admit on its own.
func TestVectorWorkRequestRootsCypher(t *testing.T) {
	// apoc.import.json reads a local file; the executor takes this setting
	// when it is constructed.
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "true")
	ctx := context.Background()
	// Cypher writes namespaced entity IDs, as on every database.
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "census")
	var exec *StorageExecutor
	requireNoFallbackAdmission(t, "github.com/orneryd/nornicdb/pkg/cypher.NewStorageExecutor", func(t *testing.T) {
		exec = NewStorageExecutor(engine)
	})
	requireNoFallbackAdmission(t, "github.com/orneryd/nornicdb/pkg/cypher.NewDatabaseScopedExecutor", func(t *testing.T) {
		_, err := NewDatabaseScopedExecutor(DatabaseScopedExecutorOptions{Name: "scoped", Storage: censusStorageResolver{storage.NewNamespacedEngine(newTestMemoryEngine(t), "scoped")}})
		require.NoError(t, err)
	})
	importFile := filepath.Join(t.TempDir(), "import.json")
	require.NoError(t, os.WriteFile(importFile, []byte(`{"nodes":[{"id":"imp1","labels":["Imported"],"properties":{"name":"i"}},{"id":"imp2","labels":["Imported"],"properties":{"name":"j"}}],"relationships":[{"id":"impr","type":"LINKS","startNode":"imp1","endNode":"imp2","properties":{}}]}`), 0o600))
	statements := []struct {
		query  string
		params map[string]interface{}
	}{
		{"CREATE (a:Doc {name: 'a', embedding: [1.0, 0.0]})-[:R {w: 1}]->(b:Doc {name: 'b'})", nil},
		{"MATCH (n:Doc {name: 'a'}) SET n.title = 'x', n.embedding = [0.0, 1.0]", nil},
		{"MATCH (a:Doc {name: 'a'}) MERGE (c:Doc {name: 'c'}) MERGE (a)-[:M]->(c)", nil},
		{"MATCH (a:Doc {name: 'a'})-[r:R]->() SET r.w = 2", nil},
		{"UNWIND range(1, 3) AS i CREATE (:Bulk {i: i})", nil},
		{"MATCH (n:Bulk) DELETE n", nil},
		{"MATCH (n:Doc {name: 'c'}) DETACH DELETE n", nil},
		{"MATCH (n:Doc {name: 'b'}) CALL kp.suppress(n)", nil},
		{"MATCH (:Doc {name: 'a'})-[r:R]->() CALL kp.suppress(r)", nil},
		{"CREATE (:Pipe {i: 1})-[:PR]->(:Pipe {i: 2})", nil},
		{"MATCH ()-[r:PR]->() WITH r DELETE r", nil},
		{"MATCH (n:Pipe) WITH n DELETE n", nil},
		{"CREATE CONSTRAINT census_req FOR ()-[r:REQ]-() REQUIRE r.p IS NOT NULL", nil},
		{"CREATE PROCEDURE census.one() MODE READ AS RETURN 1 AS n", nil},
		{"DROP PROCEDURE census.one", nil},
		{"CALL apoc.import.json($file) YIELD source, nodes, relationships", map[string]interface{}{"file": importFile}},
		{"BEGIN", nil},
		{"CREATE (:InTx {v: 1})", nil},
		{"COMMIT", nil},
	}
	// A relationship the constraint rejects makes CREATE remove the nodes it
	// already wrote; that cleanup is a write path too.
	rejected := []string{"CREATE (:ReqEnd)-[:REQ]->(:ReqEnd)"}
	requireNoFallbackAdmission(t, "(*github.com/orneryd/nornicdb/pkg/cypher.StorageExecutor).Execute", func(t *testing.T) {
		for _, statement := range statements {
			_, err := exec.Execute(ctx, statement.query, statement.params)
			require.NoError(t, err, statement.query)
		}
		for _, query := range rejected {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err, query)
		}
		count, err := exec.Execute(ctx, "MATCH (n:ReqEnd) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		require.EqualValues(t, 0, count.Rows[0][0])
	})
	requireNoFallbackAdmission(t, "(*github.com/orneryd/nornicdb/pkg/cypher.StorageExecutor).FunctionStorage", func(t *testing.T) {
		_, err := exec.FunctionStorage("")
		require.NoError(t, err)
	})
}
