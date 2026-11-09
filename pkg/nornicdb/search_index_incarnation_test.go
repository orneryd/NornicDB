package nornicdb

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-386 and NOR-404 through the database: a storage write's search refresh
// waits in the debounced queue (the path of replication apply, imports and
// APOC writes, which reach search only through storage events). DROP INDEX
// leaves the search service's node vectors, which are projections of storage:
// another vector index on the property keeps serving, an index created again
// serves the stored nodes, and a refresh queued before the drop installs the
// node's current stored vector, never an older one.

const incarnationCreateDocs = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
const incarnationCreateNotes = "CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"

func openIncarnationDB(t *testing.T) (*DB, *dbSearchService) {
	t.Helper()
	db, entry := openIncarnationDBAt(t, t.TempDir(), false)
	t.Cleanup(func() { _ = db.Close() })
	return db, entry
}

// openIncarnationDBAt opens the database in dir, with its search indexes
// persisted on close and restored on open when persist is set.
func openIncarnationDBAt(t *testing.T, dir string, persist bool) (*DB, *dbSearchService) {
	t.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingDimensions = 3
	config.Memory.SearchBM25Enabled = true
	config.Memory.SearchVectorEnabled = true
	config.Database.PersistSearchIndexes = persist
	db, err := Open(dir, config)
	require.NoError(t, err)
	_, err = db.EnsureSearchIndexes(t.Context(), db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	waitDBWorkIdle(t, db)
	db.searchServicesMu.RLock()
	entry := db.searchServices[db.defaultDatabaseName()]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, entry)
	// Each storage event re-arms the flush with this delay: refreshes stay
	// queued until the test applies them, as the flush would later.
	entry.pendingFlushDelay = time.Hour
	return db, entry
}

func incarnationRun(t *testing.T, db *DB, query string) [][]interface{} {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(), query, nil)
	require.NoError(t, err, query)
	return result.Rows
}

// incarnationServed is what index serves for [1, 0, 0], by node id, with
// scores rounded to 3 decimals. Against it v1 [1, 0, 0] scores 1.0, v2
// [0.6, 0.8, 0] 0.8, and [0, 1, 0] or [0, 0, 1] 0.5.
func incarnationServed(t *testing.T, db *DB, index string) map[interface{}]float64 {
	t.Helper()
	served := map[interface{}]float64{}
	for _, row := range incarnationRun(t, db, fmt.Sprintf("CALL db.index.vector.queryNodes('%s', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.id, score", index)) {
		score, ok := row[1].(float64)
		require.True(t, ok, "score %v", row[1])
		served[row[0]] = math.Round(score*1000) / 1000
	}
	return served
}

func requireNoIncarnationIndex(t *testing.T, db *DB, index string) {
	t.Helper()
	_, err := db.ExecuteCypher(t.Context(), fmt.Sprintf("CALL db.index.vector.queryNodes('%s', 10, [1.0, 0.0, 0.0]) YIELD node RETURN node", index), nil)
	require.ErrorContains(t, err, "There is no such vector schema index", "a query naming %s", index)
}

func incarnationQueued(t *testing.T, entry *dbSearchService) {
	t.Helper()
	require.Eventually(t, entry.hasPending, 5*time.Second, time.Millisecond, "the write's refresh was not queued")
}

// incarnationStore writes vector to the node's emb property in storage only,
// as replication apply does: its refresh waits in the queue.
func incarnationStore(t *testing.T, db *DB, entry *dbSearchService, id storage.NodeID, vector []float64) {
	t.Helper()
	stored, err := db.storage.GetNode(t.Context(), id)
	require.NoError(t, err)
	changed := storage.CopyNode(stored)
	changed.Properties["emb"] = vector
	require.NoError(t, db.storage.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: changed}))
	incarnationQueued(t, entry)
}

func TestNOR386RefreshQueuedBeforeDropIndex(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "plain drop"
		if recreate {
			name = "drop and recreate"
		}
		t.Run(name, func(t *testing.T) {
			db, entry := openIncarnationDB(t)
			incarnationRun(t, db, incarnationCreateDocs)
			incarnationRun(t, db, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]})")
			incarnationQueued(t, entry)
			applyQueued(t, entry)
			require.Equal(t, map[interface{}]float64{"a": 1}, incarnationServed(t, db, "docs"))

			// A storage write before the drop: its refresh waits in the queue.
			id := storage.NodeID(fmt.Sprint(incarnationRun(t, db, "MATCH (n:Doc {id: 'a'}) RETURN id(n)")[0][0]))
			incarnationStore(t, db, entry, id, []float64{0.6, 0.8, 0})
			incarnationRun(t, db, "DROP INDEX docs")
			if recreate {
				incarnationRun(t, db, incarnationCreateDocs)
			}
			require.True(t, entry.hasPending(), "the refresh must still be queued")
			applyQueued(t, entry)
			require.Equal(t, 1, entry.svc.CountPropertyVectorEntries("emb"), "the node's vector stays in the search service")
			if recreate {
				require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationServed(t, db, "docs"), "the recreated index serves the stored vector")
			} else {
				requireNoIncarnationIndex(t, db, "docs")
			}

			// A write after the drop is indexed.
			incarnationRun(t, db, "MATCH (n:Doc {id: 'a'}) SET n.emb = [0.0, 0.0, 1.0]")
			incarnationQueued(t, entry)
			applyQueued(t, entry)
			require.Equal(t, 1, entry.svc.CountPropertyVectorEntries("emb"))
			if recreate {
				require.Equal(t, map[interface{}]float64{"a": 0.5}, incarnationServed(t, db, "docs"), "the stored vector [0, 0, 1], not an earlier one")
			}
		})
	}
}

func TestNOR386RebuildAfterDropIndexRepopulates(t *testing.T) {
	db, entry := openIncarnationDB(t)
	incarnationRun(t, db, incarnationCreateDocs)
	incarnationRun(t, db, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]}), (:Doc {id: 'b', emb: [0.0, 1.0, 0.0]})")
	incarnationQueued(t, entry)
	applyQueued(t, entry)
	incarnationRun(t, db, "DROP INDEX docs")
	require.Equal(t, 2, entry.svc.CountPropertyVectorEntries("emb"), "the stored nodes' vectors stay")
	incarnationRun(t, db, incarnationCreateDocs)
	want := map[interface{}]float64{"a": 1, "b": 0.5}
	require.Equal(t, want, incarnationServed(t, db, "docs"), "the recreated index serves the stored nodes")
	require.NoError(t, db.BuildSearchIndexes(t.Context()))
	require.Equal(t, 2, entry.svc.CountPropertyVectorEntries("emb"), "a rebuild after the drop populates from storage")
	require.Equal(t, want, incarnationServed(t, db, "docs"))
}

// Through the real storage-event queue and real DROP INDEX and CREATE VECTOR
// INDEX: a write before the drop whose refresh is still queued. Once it lands,
// the recreated index serves the node's current stored vector, never the
// older one, in every representation (the property's vector array, the named
// embedding with its name, the node's managed chunks). A rebuild and a write
// after the drop serve the stored vector too.
func TestNOR386QueuedRefreshKeepsRecreatedIndexCurrent(t *testing.T) {
	for _, representation := range []string{"property", "named", "chunks"} {
		t.Run(representation, func(t *testing.T) {
			db, entry := openIncarnationDB(t)
			served := func() map[interface{}]float64 {
				t.Helper()
				return incarnationServed(t, db, "docs")
			}
			set := func(node *storage.Node, vector []float32) {
				switch representation {
				case "property":
					node.Properties["emb"] = vector
				case "named":
					node.NamedEmbeddings = map[string][]float32{"emb": vector}
				case "chunks":
					node.ChunkEmbeddings = [][]float32{vector}
				}
			}
			write := func(vector []float32) {
				t.Helper()
				stored, err := db.storage.GetNode(t.Context(), "current")
				require.NoError(t, err)
				changed := storage.CopyNode(stored)
				set(changed, vector)
				require.NoError(t, db.storage.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: changed}))
				incarnationQueued(t, entry)
			}

			incarnationRun(t, db, incarnationCreateDocs)
			node := &storage.Node{ID: "current", Labels: []string{"Doc"}, Properties: map[string]any{"id": "current"}}
			set(node, []float32{1, 0, 0})
			_, err := db.storage.CreateNode(t.Context(), node)
			require.NoError(t, err)
			incarnationQueued(t, entry)
			applyQueued(t, entry)
			require.Equal(t, map[interface{}]float64{"current": 1}, served())

			write([]float32{0.6, 0.8, 0}) // its refresh stays queued across the DDL
			incarnationRun(t, db, "DROP INDEX docs")
			incarnationRun(t, db, incarnationCreateDocs)
			require.True(t, entry.hasPending(), "the refresh must still be queued")
			applyQueued(t, entry)
			require.Equal(t, map[interface{}]float64{"current": 0.8}, served(), "the recreated index serves the stored %s vector", representation)

			require.NoError(t, db.BuildSearchIndexes(t.Context()))
			require.Equal(t, map[interface{}]float64{"current": 0.8}, served(), "a rebuild after the drop serves the stored vector")

			write([]float32{0, 1, 0})
			applyQueued(t, entry)
			require.Equal(t, map[interface{}]float64{"current": 0.5}, served(), "a write after the drop is served")
		})
	}
}

// NOR-404 through the database, its storage-event queue and a restart with
// the search indexes persisted on close and loaded on open: dropping one of
// two vector indexes on a property leaves the other serving its nodes, with
// a write queued across the drop; the dropped name stays unknown; an index
// created again serves the stored nodes; and all of it holds after a restart.
func TestNOR404SiblingIndexSurvivesDropAndRestart(t *testing.T) {
	dir := t.TempDir()
	db, entry := openIncarnationDBAt(t, dir, true)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})
	incarnationRun(t, db, incarnationCreateDocs)
	incarnationRun(t, db, incarnationCreateNotes)
	incarnationRun(t, db, "CREATE (:Doc {id: 'd', emb: [1.0, 0.0, 0.0]}), (:Note {id: 'n', emb: [0.6, 0.8, 0.0]}), (:Doc:Note {id: 'dn', emb: [0.0, 1.0, 0.0]})")
	incarnationQueued(t, entry)
	applyQueued(t, entry)
	docs := map[interface{}]float64{"d": 1, "dn": 0.5}
	notes := map[interface{}]float64{"dn": 0.5, "n": 0.8}
	require.Equal(t, docs, incarnationServed(t, db, "docs"))
	require.Equal(t, notes, incarnationServed(t, db, "notes"))

	// A write whose refresh is queued across the drop.
	id := storage.NodeID(fmt.Sprint(incarnationRun(t, db, "MATCH (n {id: 'dn'}) RETURN id(n)")[0][0]))
	incarnationStore(t, db, entry, id, []float64{0.6, 0.8, 0})
	incarnationRun(t, db, "DROP INDEX docs")
	require.Equal(t, notes, incarnationServed(t, db, "notes"), "the sibling serves its nodes right after the drop")
	applyQueued(t, entry)
	docs = map[interface{}]float64{"d": 1, "dn": 0.8}
	notes = map[interface{}]float64{"dn": 0.8, "n": 0.8}
	require.Equal(t, notes, incarnationServed(t, db, "notes"), "the sibling serves the write queued across the drop")
	requireNoIncarnationIndex(t, db, "docs")
	incarnationRun(t, db, "DROP INDEX notes")
	incarnationRun(t, db, incarnationCreateNotes)
	require.Equal(t, notes, incarnationServed(t, db, "notes"), "the index created again serves the stored nodes")

	require.NoError(t, db.Close())
	closed = true
	db, _ = openIncarnationDBAt(t, dir, true)
	t.Cleanup(func() { _ = db.Close() })
	require.Equal(t, notes, incarnationServed(t, db, "notes"), "after the restart")
	requireNoIncarnationIndex(t, db, "docs")
	incarnationRun(t, db, incarnationCreateDocs)
	require.Equal(t, docs, incarnationServed(t, db, "docs"), "an index created after the restart serves the stored nodes")
}

// Review round 4 of #1753 (reviewer probe, kept verbatim): the real storage
// event queued before DROP INDEX, drained after a database rebuild of the
// recreated index, must not erase the rebuild's current entry.
func TestReview1753R4EventDrainAfterRebuild(t *testing.T) {
	db, entry := openIncarnationDB(t)
	run := func(query string) [][]interface{} {
		t.Helper()
		result, err := db.ExecuteCypher(t.Context(), query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
	const query = "CALL db.index.vector.queryNodes('docs',5,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score"
	run(create)
	run("CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]})")
	require.Eventually(t, entry.hasPending, 5*time.Second, time.Millisecond)
	applyQueued(t, entry)
	require.Len(t, run(query), 1)
	id := storage.NodeID(fmt.Sprint(run("MATCH (n:Doc) RETURN id(n)")[0][0]))
	node, err := db.GetStorage().GetNode(t.Context(), id)
	require.NoError(t, err)
	node = storage.CopyNode(node)
	node.Properties["emb"] = []float64{0, 1, 0}
	require.NoError(t, db.GetStorage().UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
	require.Eventually(t, entry.hasPending, 5*time.Second, time.Millisecond)
	run("DROP INDEX docs")
	run(create)
	require.NoError(t, db.BuildSearchIndexes(t.Context()))
	require.True(t, entry.hasPending(), "the real storage event is still pending")
	before := run(query)
	require.Len(t, before, 1)
	require.InDelta(t, 0.5, before[0][1], 1e-6)
	applyQueued(t, entry)
	after := run(query)
	t.Logf("before=%v after=%v", before, after)
	require.Equal(t, before, after, "draining the old event erased the rebuild's current entry")
}

// Review round 6 of #1753 (reviewer probe, kept verbatim): through the real
// storage-event queue, an index recreated while a write's refresh is still
// queued serves the stored vector at once.
func TestReview1753R6QueuedWriteBeforeRecreateQuery(t *testing.T) {
	db, entry := openIncarnationDB(t)
	incarnationRun(t, db, incarnationCreateDocs)
	incarnationRun(t, db, "CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]})")
	incarnationQueued(t, entry)
	applyQueued(t, entry)
	id := storage.NodeID(fmt.Sprint(incarnationRun(t, db, "MATCH (n:Doc {id:'a'}) RETURN id(n)")[0][0]))
	incarnationStore(t, db, entry, id, []float64{0, 1, 0})
	incarnationRun(t, db, "DROP INDEX docs")
	incarnationRun(t, db, incarnationCreateDocs)
	require.True(t, entry.hasPending())
	before := incarnationServed(t, db, "docs")
	applyQueued(t, entry)
	after := incarnationServed(t, db, "docs")
	t.Logf("before drain=%v after drain=%v stored=%v", before, after,
		incarnationRun(t, db, "MATCH (n:Doc {id:'a'}) RETURN n.emb"))
	require.Equal(t, map[interface{}]float64{"a": 0.5}, before,
		"the newly created index must serve the already-stored vector")
}

// DB.CreateIndex publishes a vector index through the same Cypher owner as
// CREATE VECTOR INDEX, so it too serves a write whose refresh is still queued
// at once.
func TestNOR404AdminCreateIndexServesQueuedWrite(t *testing.T) {
	db, entry := openIncarnationDB(t)
	incarnationRun(t, db, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]})")
	incarnationQueued(t, entry)
	applyQueued(t, entry)
	id := storage.NodeID(fmt.Sprint(incarnationRun(t, db, "MATCH (n:Doc {id: 'a'}) RETURN id(n)")[0][0]))
	incarnationStore(t, db, entry, id, []float64{0.6, 0.8, 0})
	require.NoError(t, db.CreateIndex(t.Context(), "Doc", "emb", "vector"))
	require.True(t, entry.hasPending(), "the write's refresh is still queued")
	require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationServed(t, db, "idx_Doc_emb"))
	applyQueued(t, entry)
	require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationServed(t, db, "idx_Doc_emb"))
}

// A write whose refresh is still queued when the database closes is served by
// the index after a restart with persisted search indexes, and by an index
// created after the restart.
func TestNOR404RestartWithQueuedWriteServesStoredVector(t *testing.T) {
	dir := t.TempDir()
	db, entry := openIncarnationDBAt(t, dir, true)
	incarnationRun(t, db, incarnationCreateDocs)
	incarnationRun(t, db, "CREATE (:Doc:Note {id: 'a', emb: [1.0, 0.0, 0.0]})")
	incarnationQueued(t, entry)
	applyQueued(t, entry)
	id := storage.NodeID(fmt.Sprint(incarnationRun(t, db, "MATCH (n {id: 'a'}) RETURN id(n)")[0][0]))
	incarnationStore(t, db, entry, id, []float64{0.6, 0.8, 0})
	require.NoError(t, db.Close())
	db, _ = openIncarnationDBAt(t, dir, true)
	t.Cleanup(func() { _ = db.Close() })
	require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationServed(t, db, "docs"), "the index after the restart")
	incarnationRun(t, db, incarnationCreateNotes)
	require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationServed(t, db, "notes"), "an index created after the restart")
}
