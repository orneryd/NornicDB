package cypher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-386 and NOR-404, on the search-service path. The search service's node
// vector entries are projections of storage, read by every vector index on a
// property, hybrid search and the Qdrant API; DROP INDEX leaves them. So:
//   - another vector index on the same property keeps serving its nodes after
//     a drop, and an index created again serves the stored nodes at once, as
//     Neo4j fills a new index from existing data (NOR-404);
//   - nothing is retired, and every route that installs an entry installs the
//     node's current stored vector, so no work admitted before a drop brings
//     back a retired or older one (NOR-386).
//
// Every index answers as the storage fallback does (a query without a search
// service): fallbackScores.

const incarnationVectorOptions = " OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"

type incarnationFixture struct {
	t       *testing.T
	store   storage.Engine
	exec    *StorageExecutor
	service *search.Service
}

func newIncarnationFixture(t *testing.T, setup ...string) *incarnationFixture {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	f := &incarnationFixture{t: t, store: store, exec: NewStorageExecutor(store)}
	for _, query := range setup {
		f.run(query, nil)
	}
	f.service = search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = f.service.Close() })
	require.NoError(t, f.service.BuildIndexes(context.Background()))
	f.exec.SetSearchService(f.service)
	return f
}

// newBadgerIncarnationFixture is newIncarnationFixture on a Badger store, for
// explicit schema transactions.
func newBadgerIncarnationFixture(t *testing.T, setup ...string) *incarnationFixture {
	t.Helper()
	store := newTestBadgerNamespacedStore(t)
	f := &incarnationFixture{t: t, store: store, exec: NewStorageExecutor(store)}
	for _, query := range setup {
		f.run(query, nil)
	}
	f.service = search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = f.service.Close() })
	require.NoError(t, f.service.BuildIndexes(context.Background()))
	f.exec.SetSearchService(f.service)
	return f
}

func (f *incarnationFixture) run(query string, params map[string]interface{}) [][]interface{} {
	f.t.Helper()
	result, err := f.exec.Execute(context.Background(), query, params)
	require.NoError(f.t, err, query)
	return result.Rows
}

const incarnationQuery = "CALL db.index.vector.queryNodes('%s', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.id, score ORDER BY node.id"

func (f *incarnationFixture) vectorQuery(index string) [][]interface{} {
	f.t.Helper()
	return f.run(fmt.Sprintf(incarnationQuery, index), nil)
}

func (f *incarnationFixture) nodeID(id string) storage.NodeID {
	f.t.Helper()
	return storage.NodeID(fmt.Sprint(f.run("MATCH (n {id: $id}) RETURN id(n)", map[string]interface{}{"id": id})[0][0]))
}

func incarnationScores(t *testing.T, rows [][]interface{}) map[interface{}]float64 {
	t.Helper()
	scores := map[interface{}]float64{}
	for _, row := range rows {
		score, ok := row[1].(float64)
		require.True(t, ok, "score %v", row[1])
		scores[row[0]] = math.Round(score*1000) / 1000
	}
	return scores
}

// vectorQueryScores is what the index serves through the search service, by
// node id.
func (f *incarnationFixture) vectorQueryScores(index string) map[interface{}]float64 {
	f.t.Helper()
	return incarnationScores(f.t, f.vectorQuery(index))
}

// fallbackScores is what the index serves through the storage fallback: the
// same query on an executor without a search service.
func (f *incarnationFixture) fallbackScores(index string) map[interface{}]float64 {
	f.t.Helper()
	result, err := NewStorageExecutor(f.store).Execute(context.Background(), fmt.Sprintf(incarnationQuery, index), nil)
	require.NoError(f.t, err)
	return incarnationScores(f.t, result.Rows)
}

// requireServes asserts that index serves want through the search service and
// through the storage fallback alike.
func (f *incarnationFixture) requireServes(index string, want map[interface{}]float64, msg string) {
	f.t.Helper()
	require.Equal(f.t, want, f.vectorQueryScores(index), "search service: %s", msg)
	require.Equal(f.t, want, f.fallbackScores(index), "storage fallback: %s", msg)
}

// requireNoIndex asserts that a query naming index fails as Neo4j's does for a
// name with no vector index, through the search service and the storage
// fallback alike.
func (f *incarnationFixture) requireNoIndex(index, msg string) {
	f.t.Helper()
	query := fmt.Sprintf(incarnationQuery, index)
	_, err := f.exec.Execute(context.Background(), query, nil)
	require.ErrorContains(f.t, err, "There is no such vector schema index", "search service: %s", msg)
	_, err = NewStorageExecutor(f.store).Execute(context.Background(), query, nil)
	require.ErrorContains(f.t, err, "There is no such vector schema index", "storage fallback: %s", msg)
}

// currentScores is ids served at score.
func currentScores(ids []interface{}, score float64) map[interface{}]float64 {
	scores := map[interface{}]float64{}
	for _, id := range ids {
		scores[id] = score
	}
	return scores
}

func (f *incarnationFixture) vectorQueryIDs(index string) []interface{} {
	f.t.Helper()
	ids := []interface{}{}
	for _, row := range f.vectorQuery(index) {
		ids = append(ids, row[0])
	}
	return ids
}

// NOR-404: dropping one vector index on a property leaves every other vector
// index on it serving its nodes, after one drop, after repeated drops and
// recreations, and when both are dropped and recreated in one transaction.
func TestNOR404DropKeepsSiblingVectorIndexesServing(t *testing.T) {
	f := newBadgerIncarnationFixture(t,
		"CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions,
		"CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)"+incarnationVectorOptions,
		"CREATE (:Doc {id: 'd', emb: [1.0, 0.0, 0.0]}), (:Note {id: 'n', emb: [0.6, 0.8, 0.0]}), (:Doc:Note {id: 'dn', emb: [0.0, 1.0, 0.0]})",
	)
	docs := map[interface{}]float64{"d": 1, "dn": 0.5}
	notes := map[interface{}]float64{"dn": 0.5, "n": 0.8}
	f.requireServes("docs", docs, "before any drop")
	f.requireServes("notes", notes, "before any drop")

	f.run("DROP INDEX docs", nil)
	f.requireServes("notes", notes, "after DROP INDEX docs")
	f.requireNoIndex("docs", "after DROP INDEX docs")
	f.run("CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.requireServes("docs", docs, "recreated")
	f.run("DROP INDEX docs", nil)
	f.requireServes("notes", notes, "after a second DROP INDEX docs")
	f.run("CREATE VECTOR INDEX docs_v2 FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.requireServes("docs_v2", docs, "recreated under another name")

	f.run("BEGIN", nil)
	f.run("DROP INDEX docs_v2", nil)
	f.run("DROP INDEX notes", nil)
	f.run("CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.run("CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)"+incarnationVectorOptions, nil)
	f.run("COMMIT", nil)
	f.requireServes("docs", docs, "both dropped and recreated in one transaction")
	f.requireServes("notes", notes, "both dropped and recreated in one transaction")
	require.Equal(t, 3, f.service.CountPropertyVectorEntries("emb"))
}

// NOR-404: an index created after the nodes were stored, created again after a
// drop, or created on another label after the property's last index was
// dropped serves the stored nodes at once.
func TestNOR404CreatedIndexServesStoredNodes(t *testing.T) {
	f := newIncarnationFixture(t,
		"CREATE (:Doc {id: 'd', emb: [1.0, 0.0, 0.0]}), (:Note {id: 'n', emb: [0.6, 0.8, 0.0]})",
	)
	f.run("CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.requireServes("docs", map[interface{}]float64{"d": 1}, "created after the nodes")
	f.run("DROP INDEX docs", nil)
	f.requireNoIndex("docs", "after the property's only index was dropped")
	f.run("CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.requireServes("docs", map[interface{}]float64{"d": 1}, "created again after the property's only index was dropped")
	f.run("DROP INDEX docs", nil)
	f.run("CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)"+incarnationVectorOptions, nil)
	f.requireServes("notes", map[interface{}]float64{"n": 0.8}, "created on another label after the drop")
}

// The bot reviews of #1753 (Codex discussion r4504397966, Copilot r4504462195):
// docs is dropped while it is the property's only index, created again
// alongside notes, a :Doc:Note node is indexed, and docs is dropped again.
// notes keeps serving the node.
func TestReview1753RepeatedDropKeepsLaterSibling(t *testing.T) {
	const docs = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.p)" + incarnationVectorOptions
	const notes = "CREATE VECTOR INDEX notes FOR (n:Note) ON (n.p)" + incarnationVectorOptions
	f := newIncarnationFixture(t, docs, "CREATE (:Doc {id: 'd', p: [1.0, 0.0, 0.0]})")
	f.run("DROP INDEX docs", nil)
	f.run(docs, nil)
	f.run(notes, nil)
	f.run("CREATE (:Doc:Note {id: 'dn', p: [0.6, 0.8, 0.0]})", nil)
	require.NoError(t, f.service.RefreshNode(context.Background(), f.nodeID("dn")))
	f.requireServes("docs", map[interface{}]float64{"d": 1, "dn": 0.8}, "before the second drop")
	f.requireServes("notes", map[interface{}]float64{"dn": 0.8}, "before the second drop")
	f.run("DROP INDEX docs", nil)
	f.requireServes("notes", map[interface{}]float64{"dn": 0.8}, "after the second drop")
	f.requireNoIndex("docs", "after the second drop")
}

// The issue's reproduction: a write before the drop, whose refresh is applied
// after the index was dropped and recreated with the same name. The recreated
// index serves the node's current stored vector, never the one before it.
func TestNOR386RefreshReservedBeforeDropServesTheCurrentVector(t *testing.T) {
	const create = "CREATE VECTOR INDEX v386 FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	f := newIncarnationFixture(t, create, "CREATE (:Doc {id: 'old', emb: [1.0, 0.0, 0.0]})")
	f.requireServes("v386", map[interface{}]float64{"old": 1}, "before the drop")
	id := f.nodeID("old")

	stored, err := f.store.GetNode(context.Background(), id)
	require.NoError(t, err)
	stored.Properties["emb"] = []float64{0.6, 0.8, 0}
	require.NoError(t, f.store.UpdateNode(context.Background(), storage.NodeUpdateRequest{After: stored}))
	mutation := f.service.ReserveNodeMutation(id) // the write's queued refresh
	f.run("DROP INDEX v386", nil)
	f.run(create, nil)
	require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
	f.requireServes("v386", map[interface{}]float64{"old": 0.8}, "the recreated index after the refresh admitted before the drop")

	// A write after the recreate is indexed, with its own vector.
	f.run("MATCH (n:Doc {id: 'old'}) SET n.emb = [0.0, 1.0, 0.0]", nil)
	require.NoError(t, f.service.RefreshNode(context.Background(), id))
	f.requireServes("v386", map[interface{}]float64{"old": 0.5}, "after a write after the recreate")
}

func TestNOR386RelationshipVectorDropKeepsNodeVectors(t *testing.T) {
	f := newIncarnationFixture(t,
		"CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions,
		"CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb)"+incarnationVectorOptions,
		"CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]})-[:R {emb: [1.0, 0.0, 0.0]}]->(:Doc {id: 'b', emb: [0.0, 1.0, 0.0]})",
	)
	require.Len(t, f.vectorQuery("docs"), 2)
	f.run("DROP INDEX rels", nil)
	rows := f.vectorQuery("docs")
	require.Len(t, rows, 2, "dropping a relationship vector index emptied a node vector index: %v", rows)
}

// A full-text index's documents are derived from storage under its own
// definition, so a recreated index serves its new definition and current data.
func TestNOR386FulltextDropAndRecreateServesItsOwnDefinition(t *testing.T) {
	f := newIncarnationFixture(t,
		"CREATE FULLTEXT INDEX docs FOR (n:Doc) ON EACH [n.title]",
		"CREATE (:Doc {id: 'a', title: 'alpha', body: 'beta'})",
	)
	const query = "CALL db.index.fulltext.queryNodes('docs', $q) YIELD node RETURN node.id ORDER BY node.id"
	lookup := func(term string) [][]interface{} {
		t.Helper()
		return f.run(query, map[string]interface{}{"q": term})
	}
	require.Equal(t, [][]interface{}{{"a"}}, lookup("alpha"))
	require.Empty(t, lookup("beta"))

	f.run("DROP INDEX docs", nil)
	_, err := f.exec.Execute(context.Background(), query, map[string]interface{}{"q": "alpha"})
	require.Error(t, err, "a dropped full-text index cannot be queried")

	f.run("CREATE (:Doc {id: 'b', title: 'alpha', body: 'alpha'})", nil)
	f.run("CREATE FULLTEXT INDEX docs FOR (n:Doc) ON EACH [n.body]", nil)
	require.Equal(t, [][]interface{}{{"b"}}, lookup("alpha"), "the recreated index serves its own definition")
	require.Equal(t, [][]interface{}{{"a"}}, lookup("beta"))

	f.run("DROP INDEX docs", nil)
	f.run("CREATE FULLTEXT INDEX docs FOR (n:Doc) ON EACH [n.title]", nil)
	require.Equal(t, [][]interface{}{{"a"}, {"b"}}, lookup("alpha"))
}

// Review round 1 of #1753 (reviewer test; expectation updated for NOR-404): an
// index renamed in one schema transaction, with a refresh admitted before the
// drop, serves the node's current stored vector.
func TestReview1753RenameInTransactionRetiresOldRefresh(t *testing.T) {
	f := newBadgerIncarnationFixture(t,
		"CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions,
		"CREATE (:Doc {id: 'old', emb: [1.0, 0.0, 0.0]})",
	)
	id := f.nodeID("old")
	node, err := f.store.GetNode(context.Background(), id)
	require.NoError(t, err)
	node.Properties["emb"] = []float64{0, 1, 0}
	require.NoError(t, f.store.UpdateNode(context.Background(), storage.NodeUpdateRequest{After: node}))
	mutation := f.service.ReserveNodeMutation(id)
	f.run("BEGIN", nil)
	f.run("DROP INDEX docs", nil)
	f.run("CREATE VECTOR INDEX replacement FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.run("COMMIT", nil)
	require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
	f.requireServes("replacement", map[interface{}]float64{"old": 0.5}, "the replacement serves the current stored vector")
}

// Several indexes dropped and recreated in one transaction, under the same name
// and under new ones, with refreshes admitted before the transaction: the
// indexes created there serve every stored node of their labels, with its
// current vector.
func TestNOR386DropAndRecreateUnderSeveralNamesInOneTransaction(t *testing.T) {
	f := newBadgerIncarnationFixture(t,
		"CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions,
		"CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)"+incarnationVectorOptions,
		"CREATE (:Doc {id: 'd', emb: [1.0, 0.0, 0.0]}), (:Note {id: 'n', emb: [1.0, 0.0, 0.0]}), (:Doc:Note {id: 'dn', emb: [1.0, 0.0, 0.0]})",
	)
	require.Equal(t, []interface{}{"d", "dn"}, f.vectorQueryIDs("docs"))

	// Writes before the transaction whose refreshes are still queued.
	var pending []search.NodeMutation
	for _, name := range []string{"d", "n", "dn"} {
		id := f.nodeID(name)
		node, err := f.store.GetNode(context.Background(), id)
		require.NoError(t, err)
		node.Properties["emb"] = []float64{0.6, 0.8, 0}
		require.NoError(t, f.store.UpdateNode(context.Background(), storage.NodeUpdateRequest{After: node}))
		pending = append(pending, f.service.ReserveNodeMutation(id))
	}
	f.run("BEGIN", nil)
	f.run("DROP INDEX docs", nil)
	f.run("DROP INDEX notes", nil)
	f.run("CREATE VECTOR INDEX docs_v2 FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.run("CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)"+incarnationVectorOptions, nil)
	f.run("COMMIT", nil)
	for _, mutation := range pending {
		require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
	}
	f.requireServes("docs_v2", map[interface{}]float64{"d": 0.8, "dn": 0.8}, "the renamed replacement")
	f.requireServes("notes", map[interface{}]float64{"dn": 0.8, "n": 0.8}, "the same-name replacement")

	// A write after the transaction reaches both replacements on its labels.
	f.run("MATCH (n {id: 'dn'}) SET n.emb = [0.0, 0.0, 1.0]", nil)
	require.NoError(t, f.service.RefreshNode(context.Background(), f.nodeID("dn")))
	f.requireServes("docs_v2", map[interface{}]float64{"d": 0.8, "dn": 0.5}, "after a write after the transaction")
	f.requireServes("notes", map[interface{}]float64{"dn": 0.5, "n": 0.8}, "after a write after the transaction")
}

// Three incarnations of docs, each dropped with a write's refresh still queued:
// each incarnation serves the node's current stored vector, never an earlier
// one.
func TestNOR386RepeatedIncarnationsServeTheCurrentVector(t *testing.T) {
	const docs = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	f := newIncarnationFixture(t, docs, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]})")
	ctx := context.Background()
	a := f.nodeID("a")
	f.requireServes("docs", map[interface{}]float64{"a": 1}, "the first incarnation")
	vectors := [][]float64{{0.6, 0.8, 0}, {0, 1, 0}, {1, 0, 0}}
	scores := []float64{0.8, 0.5, 1}
	for i := range vectors {
		stored, err := f.store.GetNode(ctx, a)
		require.NoError(t, err)
		stored.Properties["emb"] = vectors[i]
		require.NoError(t, f.store.UpdateNode(ctx, storage.NodeUpdateRequest{After: stored}))
		late := f.service.ReserveNodeMutation(a)
		f.run("DROP INDEX docs", nil)
		f.run(docs, nil)
		require.NoError(t, f.service.RefreshNodeWithMutation(ctx, late))
		f.requireServes("docs", map[interface{}]float64{"a": scores[i]}, fmt.Sprintf("incarnation %d", i+2))
	}
}

func newReview1753R2BadgerFixture(t *testing.T) *incarnationFixture {
	return newBadgerIncarnationFixture(t,
		"CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions,
		"CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)"+incarnationVectorOptions,
		"CREATE (:Doc:Note {id: 'both', emb: [1.0, 0.0, 0.0]})",
	)
}

// Review round 2 of #1753 (reviewer probe; expectation updated for NOR-404): a
// schema migration replaces both indexes under their existing names. Both
// replacements serve the stored node with its current vector.
func TestReview1753R2ReplaceBothSameNames(t *testing.T) {
	f := newReview1753R2BadgerFixture(t)
	mutation := f.service.ReserveNodeMutation(f.nodeID("both"))
	f.run("BEGIN", nil)
	f.run("DROP INDEX docs", nil)
	f.run("DROP INDEX notes", nil)
	f.run("CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	f.run("CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)"+incarnationVectorOptions, nil)
	f.run("COMMIT", nil)
	require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
	f.requireServes("docs", map[interface{}]float64{"both": 1}, "the same-name replacement of docs")
	f.requireServes("notes", map[interface{}]float64{"both": 1}, "the same-name replacement of notes")
}

// Review round 2 of #1753 (reviewer probe, kept verbatim).
func TestReview1753R2RollbackLeavesNoRetirement(t *testing.T) {
	f := newReview1753R2BadgerFixture(t)
	mutation := f.service.ReserveNodeMutation(f.nodeID("both"))
	f.run("BEGIN", nil)
	f.run("DROP INDEX docs", nil)
	f.run("DROP INDEX notes", nil)
	f.run("ROLLBACK", nil)
	require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
	require.Len(t, f.vectorQuery("docs"), 1)
	require.Len(t, f.vectorQuery("notes"), 1)
}

// Review round 2 of #1753 (reviewer probe, as adapted in round 2): a drop
// whose transaction is rejected at commit leaves both indexes serving.
func TestReview1753R2ConcurrentSchemaConflictLeavesNoRetirement(t *testing.T) {
	f := newReview1753R2BadgerFixture(t)
	other := NewStorageExecutor(f.store)
	other.SetSearchService(f.service)
	mutation := f.service.ReserveNodeMutation(f.nodeID("both"))
	f.run("BEGIN", nil)
	f.run("DROP INDEX docs", nil)
	for _, query := range []string{
		"CREATE VECTOR INDEX others FOR (n:Other) ON (n.vec)" + incarnationVectorOptions,
		"DROP INDEX others",
	} {
		_, err := other.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	_, err := f.exec.Execute(context.Background(), "COMMIT", nil)
	require.Error(t, err, "the earlier schema snapshot must conflict with the committed change")
	require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
	require.Len(t, f.vectorQuery("docs"), 1, "a rejected drop must leave its entries and admissions alive")
	require.Len(t, f.vectorQuery("notes"), 1)
}

// Every way a schema change drops and creates node vector indexes on a
// property, in one transaction and in separate ones, for a node of one label,
// a node of both indexed labels, and nodes of each label, and for every
// representation an index serves (the property's vector array, the named
// embedding with its name, the node's managed chunks). Every node has a
// refresh admitted before the change, carrying a newer vector than the one
// indexed. After the change every index on a node's labels serves its current
// stored vector, as the storage fallback does; writes after the change reach
// every index on their labels.
func TestNOR386DropMatrixServesTheCurrentVectors(t *testing.T) {
	create := func(name, label string) string {
		return fmt.Sprintf("CREATE VECTOR INDEX %s FOR (n:%s) ON (n.emb)", name, label) + incarnationVectorOptions
	}
	type index struct{ name, label string }
	operations := []struct {
		name       string
		statements []string
		after      []index
	}{
		{"drop one", []string{"DROP INDEX docs"}, []index{{"notes", "Note"}}},
		{"drop all", []string{"DROP INDEX docs", "DROP INDEX notes"}, nil},
		{"recreate same name", []string{"DROP INDEX docs", create("docs", "Doc")}, []index{{"docs", "Doc"}, {"notes", "Note"}}},
		{"recreate other name", []string{"DROP INDEX docs", create("docs_v2", "Doc")}, []index{{"docs_v2", "Doc"}, {"notes", "Note"}}},
		{"swap names", []string{"DROP INDEX docs", "DROP INDEX notes", create("docs", "Note"), create("notes", "Doc")}, []index{{"docs", "Note"}, {"notes", "Doc"}}},
	}
	type node struct {
		id     string
		labels []string
	}
	configs := []struct {
		name  string
		nodes []node
	}{
		{"single label", []node{{"d", []string{"Doc"}}}},
		{"multi-label node", []node{{"dn", []string{"Doc", "Note"}}}},
		{"sibling on another label", []node{{"d", []string{"Doc"}}, {"n", []string{"Note"}}}},
	}
	readers := func(nodes []node, label string) []interface{} {
		ids := []interface{}{}
		for _, n := range nodes {
			if slices.Contains(n.labels, label) {
				ids = append(ids, n.id)
			}
		}
		return ids
	}
	for _, representation := range []string{"property", "named", "chunks"} {
		for _, operation := range operations {
			for _, oneTransaction := range []bool{true, false} {
				for _, config := range configs {
					mode := "separate transactions"
					if oneTransaction {
						mode = "one transaction"
					}
					t.Run(representation+"/"+operation.name+"/"+mode+"/"+config.name, func(t *testing.T) {
						ctx := context.Background()
						store := newTestBadgerNamespacedStore(t)
						f := &incarnationFixture{t: t, store: store, exec: NewStorageExecutor(store)}
						f.run(create("docs", "Doc"), nil)
						f.run(create("notes", "Note"), nil)
						for _, n := range config.nodes {
							stored := &storage.Node{ID: storage.NodeID(n.id), Labels: n.labels, Properties: map[string]any{"id": n.id}}
							setIncarnationVector(stored, representation, []float32{1, 0, 0})
							_, err := store.CreateNode(ctx, stored)
							require.NoError(t, err)
						}
						f.service = search.NewService(store, 3, nil)
						t.Cleanup(func() { _ = f.service.Close() })
						require.NoError(t, f.service.BuildIndexes(ctx))
						f.exec.SetSearchService(f.service)
						f.requireServes("docs", currentScores(readers(config.nodes, "Doc"), 1), "before the change")
						f.requireServes("notes", currentScores(readers(config.nodes, "Note"), 1), "before the change")
						rewrite := func(vector []float32) []search.NodeMutation {
							var mutations []search.NodeMutation
							for _, n := range config.nodes {
								id := f.nodeID(n.id)
								stored, err := store.GetNode(ctx, id)
								require.NoError(t, err)
								setIncarnationVector(stored, representation, vector)
								require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: stored}))
								mutations = append(mutations, f.service.ReserveNodeMutation(id))
							}
							return mutations
						}

						// A write before the change to every node, whose refresh is
						// still queued when the change commits: storage holds v2
						// (score 0.8), the index still the v1 it was built with
						// (score 1.0).
						pending := rewrite([]float32{0.6, 0.8, 0})
						if oneTransaction {
							f.run("BEGIN", nil)
						}
						for _, statement := range operation.statements {
							f.run(statement, nil)
						}
						if oneTransaction {
							f.run("COMMIT", nil)
						}
						for _, mutation := range pending {
							require.NoError(t, f.service.RefreshNodeWithMutation(ctx, mutation))
						}
						for _, idx := range operation.after {
							f.requireServes(idx.name, currentScores(readers(config.nodes, idx.label), 0.8), "after the change")
						}
						require.Equal(t, len(config.nodes), f.service.EmbeddingCount(), "the node vectors stay installed")

						// Writes after the change reach every index on their labels,
						// with the vector they wrote (v3, score 0.5).
						for _, mutation := range rewrite([]float32{0, 0, 1}) {
							require.NoError(t, f.service.RefreshNodeWithMutation(ctx, mutation))
						}
						for _, idx := range operation.after {
							f.requireServes(idx.name, currentScores(readers(config.nodes, idx.label), 0.5), "after a write after the change")
						}
						require.Equal(t, len(config.nodes), f.service.EmbeddingCount())
					})
				}
			}
		}
	}
}

// setIncarnationVector sets the node's emb vector in the representation's
// field.
func setIncarnationVector(node *storage.Node, representation string, vector []float32) {
	switch representation {
	case "property":
		node.Properties["emb"] = vector
	case "named":
		node.NamedEmbeddings = map[string][]float32{"emb": vector}
	case "chunks":
		node.ChunkEmbeddings = [][]float32{vector}
	}
}

// incarnationOutageEngine fails node reads while down, as a storage outage
// does, so the search service hands its refresh to the retry owner.
type incarnationOutageEngine struct {
	*storage.NamespacedEngine
	down atomic.Bool
}

func (e *incarnationOutageEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if e.down.Load() {
		return nil, errors.New("node read outage")
	}
	return e.NamespacedEngine.GetNode(ctx, id)
}

// With real DROP INDEX and CREATE VECTOR INDEX, a refresh admitted before the
// drop, two refreshes applied out of order, and a refresh retried after a
// storage outage all leave the recreated index on the node's stored vector,
// never an older one, in every representation.
func TestNOR386RecreatedIndexServesTheCurrentVector(t *testing.T) {
	for _, representation := range []string{"property", "named", "chunks"} {
		for _, route := range []string{"queued refresh", "refreshes out of order", "retried refresh"} {
			t.Run(representation+"/"+route, func(t *testing.T) {
				const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
				ctx := context.Background()
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
				reads := &incarnationOutageEngine{NamespacedEngine: store}
				f := &incarnationFixture{t: t, store: store, exec: NewStorageExecutor(store)}
				f.run(create, nil)
				f.service = search.NewService(reads, 3, nil)
				t.Cleanup(func() { _ = f.service.Close() })
				require.NoError(t, f.service.BuildIndexes(ctx))
				f.exec.SetSearchService(f.service)
				var retried []search.RefreshRetry
				f.service.SetRefreshRetryOwner(func(work search.RefreshRetry, err error) { retried = append(retried, work) })

				node := &storage.Node{ID: "current", Labels: []string{"Doc"}, Properties: map[string]any{"id": "current"}}
				setIncarnationVector(node, representation, []float32{1, 0, 0})
				id, err := store.CreateNode(ctx, node)
				require.NoError(t, err)
				require.NoError(t, f.service.RefreshNode(ctx, id))
				f.requireServes("docs", map[interface{}]float64{"current": 1}, "before the drop")
				write := func(vector []float32) search.NodeMutation {
					stored, err := store.GetNode(ctx, id)
					require.NoError(t, err)
					setIncarnationVector(stored, representation, vector)
					require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: stored}))
					return f.service.ReserveNodeMutation(id)
				}

				want := 0.8 // v2 [0.6, 0.8, 0]
				mutation := write([]float32{0.6, 0.8, 0})
				var newer search.NodeMutation
				if route == "refreshes out of order" {
					newer = write([]float32{0, 1, 0})
					want = 0.5 // v3 [0, 1, 0]
				}
				if route == "retried refresh" {
					reads.down.Store(true)
					require.Error(t, f.service.RefreshNodeWithMutation(ctx, mutation))
					reads.down.Store(false)
					require.Len(t, retried, 1)
					mutation = retried[0].Node
				}
				f.run("DROP INDEX docs", nil)
				f.run(create, nil)
				if route == "refreshes out of order" {
					require.NoError(t, f.service.RefreshNodeWithMutation(ctx, newer))
				}
				require.NoError(t, f.service.RefreshNodeWithMutation(ctx, mutation))
				f.requireServes("docs", map[interface{}]float64{"current": want}, "the recreated index")
			})
		}
	}
}

// Review round 3 of #1753 (reviewer probe; expectation updated for NOR-404):
// the compatibility drop procedure drops like DROP INDEX, and the recreated
// index serves the stored node.
func TestReview1753R3CompatibilityDropRetiresRefresh(t *testing.T) {
	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	f := newIncarnationFixture(t, create, "CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]})")
	mutation := f.service.ReserveNodeMutation(f.nodeID("a"))
	f.run("CALL db.index.vector.drop('docs')", nil)
	f.requireNoIndex("docs", "after the compatibility drop")
	f.run(create, nil)
	require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
	f.requireServes("docs", map[interface{}]float64{"a": 1}, "the index recreated after the compatibility drop")
}

// Review round 4 of #1753 (reviewer probe, kept verbatim): an event admitted
// before DROP may still be waiting when an operator rebuilds the recreated
// index. Applying that event must not erase the newer population.
func TestReview1753R4OldRefreshAfterRebuild(t *testing.T) {
	ctx := context.Background()
	for _, drop := range []bool{false, true} {
		name := "without_drop"
		if drop {
			name = "with_drop"
		}
		t.Run(name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			executor := NewStorageExecutor(store)
			run := func(query string) *ExecuteResult {
				t.Helper()
				result, err := executor.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				return result
			}
			const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
			run(create)
			run("CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]})")
			svc := search.NewService(store, 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			require.NoError(t, svc.BuildIndexes(ctx))
			executor.SetSearchService(svc)
			nodes, err := store.GetNodesByLabel(ctx, "Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			run("MATCH (n:Doc) SET n.emb = [0.0,1.0,0.0]")
			pending := svc.ReserveNodeMutation(nodes[0].ID)
			if drop {
				run("DROP INDEX docs")
				run(create)
			}
			require.NoError(t, svc.BuildIndexes(ctx))
			const query = "CALL db.index.vector.queryNodes('docs',5,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score"
			before := run(query).Rows
			require.Len(t, before, 1, "the rebuild must populate from current storage")
			require.InDelta(t, 0.5, before[0][1], 1e-6)
			require.NoError(t, svc.RefreshNodeWithMutation(ctx, pending))
			after := run(query).Rows
			t.Logf("drop=%t before=%v after=%v", drop, before, after)
			require.Equal(t, before, after, "the old refresh erased the current rebuild's entry")
		})
	}
}

// incarnationOp is one step of an ordering-differential order: a storage
// write whose refresh stays pending (oversize: with text beyond the search
// service's index memory budget), the late drain of one pending refresh, one
// schema change in its own statement, an explicit transaction of several, a
// rebuild of the search indexes, arming a failure of the search service's next
// node read, or applying the refreshes the retry owner holds.
type incarnationOp struct {
	kind     string // "write", "drain", "ddl", "tx", "rebuild", "failread", "retry"
	node     int    // write: the node; drain: which pending refresh, modulo their number
	vector   int    // write: the vector
	oversize bool   // write: text beyond the index memory budget
	ddls     []int  // ddl and tx: the indexes changed, each dropped if live, else created
}

// incarnationOrderFixture runs ordering-differential orders over three nodes
// on two labels and three vector indexes on one property.
var (
	incarnationOrderIndexes = []struct{ name, label string }{{"docs", "Doc"}, {"notes", "Note"}, {"docs_v2", "Doc"}}
	incarnationOrderVectors = [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {0.6, 0.8, 0}}
	incarnationOrderNodes   = []struct {
		id     string
		labels []string
	}{{"d", []string{"Doc"}}, {"n", []string{"Note"}}, {"dn", []string{"Doc", "Note"}}}
)

// incarnationFlakyReads fails the search service's next node reads while
// armed, as a transient storage failure does.
type incarnationFlakyReads struct {
	storage.Engine
	failures atomic.Int32
}

func (e *incarnationFlakyReads) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if e.failures.Add(-1) >= 0 {
		return nil, errors.New("transient node read failure")
	}
	e.failures.Store(0)
	return e.Engine.GetNode(ctx, id)
}

// runIncarnationOrder applies ops and checks the search service against the
// storage fallback. Without failures, every live index answers exactly as the
// fallback right after each statement or transaction that creates one, while
// refreshes admitted before it are still pending, and once every refresh has
// landed. With failures injected (a failed read, a text beyond the BM25
// budget), every live index answers as the fallback for every node with no
// refresh pending or held for retry, after every drain and retry and right
// after each creation: a text the budget refuses never takes a node's vector
// out of an index (round 9), and a stale value is never served. A publication
// whose population failed reports IndexCreationFailed and leaves its index
// FAILED. Once every refresh and retry has landed, every live index answers
// exactly as the fallback.
func runIncarnationOrder(t *testing.T, representation string, failures bool, ops []incarnationOp) {
	t.Helper()
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	f := &incarnationFixture{t: t, store: store, exec: NewStorageExecutor(store)}
	live := map[string]bool{}
	var calls []string
	createIndex := func(statement func(string) error, i int) error {
		err := statement(fmt.Sprintf("CREATE VECTOR INDEX %s FOR (n:%s) ON (n.emb)", incarnationOrderIndexes[i].name, incarnationOrderIndexes[i].label) + incarnationVectorOptions)
		live[incarnationOrderIndexes[i].name] = true
		return err
	}
	mustRun := func(query string) error {
		f.run(query, nil)
		return nil
	}
	_ = createIndex(mustRun, 0)
	_ = createIndex(mustRun, 1)
	for i, n := range incarnationOrderNodes {
		stored := &storage.Node{ID: storage.NodeID(n.id), Labels: n.labels, Properties: map[string]any{"id": n.id, "text": "short"}}
		setIncarnationVector(stored, representation, incarnationOrderVectors[i])
		_, err := store.CreateNode(ctx, stored)
		require.NoError(t, err)
	}
	reads := &incarnationFlakyReads{Engine: store}
	var options *search.ServiceOptions
	if failures {
		options = &search.ServiceOptions{BM25MemoryMaxBytes: 4096}
	}
	f.service = search.NewService(reads, 3, options)
	t.Cleanup(func() { _ = f.service.Close() })
	require.NoError(t, f.service.BuildIndexes(ctx))
	f.exec.SetSearchService(f.service)
	var retries []search.NodeMutation
	f.service.SetRefreshRetryOwner(func(work search.RefreshRetry, err error) { retries = append(retries, work.Node) })

	type pendingRefresh struct {
		id       string
		mutation search.NodeMutation
	}
	var pending []pendingRefresh
	// lagging holds the nodes whose refresh is pending or held for retry: a
	// live index may serve their earlier vector until it lands.
	lagging := func() map[interface{}]bool {
		ids := map[interface{}]bool{}
		for _, p := range pending {
			ids[p.id] = true
		}
		if len(retries) > 0 {
			for _, n := range incarnationOrderNodes {
				ids[n.id] = true // a retry may hold any node's refresh
			}
		}
		return ids
	}
	// liveNames are the live indexes that answer. A FAILED one (its
	// population failed) must refuse queries, as in Neo4j, until it is
	// dropped and created again.
	liveNames := func() []string {
		names := make([]string, 0, len(live))
		for name := range live {
			names = append(names, name)
		}
		sort.Strings(names)
		online := names[:0]
		for _, name := range names {
			if state, _ := f.service.VectorIndexState(name); state == search.VectorIndexFailed {
				_, err := f.exec.Execute(ctx, fmt.Sprintf(incarnationQuery, name), nil)
				require.ErrorContains(t, err, "entered a FAILED state", "a FAILED index must refuse queries; calls: %v", calls)
				continue
			}
			online = append(online, name)
		}
		return online
	}
	requireLiveMatchStorage := func(when string) {
		t.Helper()
		for _, name := range liveNames() {
			require.Equal(t, f.fallbackScores(name), f.vectorQueryScores(name), "%s differs from the storage fallback %s; calls: %v", name, when, calls)
		}
	}
	// requireMatchesStorage: every node with no refresh pending or held for
	// retry is served exactly as storage holds it, neither stale nor missing.
	requireMatchesStorage := func(when string) {
		t.Helper()
		skip := lagging()
		for _, name := range liveNames() {
			want := f.fallbackScores(name)
			served := f.vectorQueryScores(name)
			for id, score := range served {
				if skip[id] {
					continue
				}
				expected, stored := want[id]
				require.True(t, stored, "%s serves %v, which storage does not hold, %s; calls: %v", name, id, when, calls)
				require.Equal(t, expected, score, "%s serves a stale value for %v %s; calls: %v", name, id, when, calls)
			}
			for id := range want {
				_, ok := served[id]
				require.True(t, ok || skip[id], "%s does not serve %v %s; calls: %v", name, id, when, calls)
			}
		}
	}
	// publicationResult checks a statement or COMMIT that published an index:
	// a population that could not complete is reported as IndexCreationFailed.
	publicationResult := func(err error) bool {
		t.Helper()
		if err == nil {
			return true
		}
		require.True(t, failures, "a publication failed without an injected failure: %v; calls: %v", err, calls)
		var classified interface{ BoltErrorCode() string }
		require.ErrorAs(t, err, &classified, "calls: %v", calls)
		require.Equal(t, "Neo.DatabaseError.Schema.IndexCreationFailed", classified.BoltErrorCode(), "calls: %v", calls)
		return false
	}
	execute := func(query string) error {
		_, err := f.exec.Execute(ctx, query, nil)
		return err
	}
	// ddl changes one index: drop it when live, else create it. A label holds
	// one vector index on emb at a time, as Neo4j allows, so an index whose
	// label already has one drops that one instead (docs and docs_v2 rename
	// each other). It reports the index it created ("" for a drop), and the
	// creating statement's error.
	ddl := func(i int) (string, error) {
		for j := range incarnationOrderIndexes {
			if j != i && incarnationOrderIndexes[j].label == incarnationOrderIndexes[i].label && live[incarnationOrderIndexes[j].name] {
				i = j
			}
		}
		if live[incarnationOrderIndexes[i].name] {
			f.run("DROP INDEX "+incarnationOrderIndexes[i].name, nil)
			delete(live, incarnationOrderIndexes[i].name)
			calls = append(calls, "drop "+incarnationOrderIndexes[i].name)
			return "", nil
		}
		err := createIndex(execute, i)
		calls = append(calls, "create "+incarnationOrderIndexes[i].name)
		return incarnationOrderIndexes[i].name, err
	}
	// A publication catches up every refresh reserved before it: the pending
	// writes are superseded, and only what the retry owner then holds lags.
	afterPublication := func(created bool, err error, when string) {
		t.Helper()
		if !created {
			require.NoError(t, err)
			return
		}
		published := publicationResult(err)
		pending = nil
		if published {
			retries = nil
		}
		if failures {
			requireMatchesStorage(when)
		} else {
			requireLiveMatchStorage(when)
		}
	}
	for _, op := range ops {
		switch op.kind {
		case "write":
			n := incarnationOrderNodes[op.node]
			id := f.nodeID(n.id)
			stored, err := store.GetNode(ctx, id)
			require.NoError(t, err)
			setIncarnationVector(stored, representation, incarnationOrderVectors[op.vector])
			stored.Properties["text"] = "short"
			if op.oversize {
				stored.Properties["text"] = strings.Repeat("payload ", 1024)
			}
			require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: stored}))
			pending = append(pending, pendingRefresh{id: n.id, mutation: f.service.ReserveNodeMutation(id)})
			calls = append(calls, fmt.Sprintf("write %s oversize=%t", n.id, op.oversize))
		case "drain":
			if len(pending) > 0 {
				j := op.node % len(pending)
				err := f.service.RefreshNodeWithMutation(ctx, pending[j].mutation)
				if !failures {
					require.NoError(t, err)
				}
				pending = append(pending[:j], pending[j+1:]...)
				calls = append(calls, "drain")
				if failures {
					requireMatchesStorage("after a drain")
				}
			}
		case "failread":
			reads.failures.Store(1)
			calls = append(calls, "failread")
		case "retry":
			held := retries
			retries = nil
			for _, mutation := range held {
				_ = f.service.RefreshNodeWithMutation(ctx, mutation)
			}
			calls = append(calls, "retry")
			if failures {
				requireMatchesStorage("after the retries")
			}
		case "ddl":
			created, err := ddl(op.ddls[0])
			afterPublication(created != "", err, "right after the create")
		case "tx":
			f.run("BEGIN", nil)
			calls = append(calls, "begin")
			// The transaction publishes the indexes it creates and doesn't
			// drop again; one it creates and drops is not published, so
			// its COMMIT populates nothing.
			var created []string
			for _, i := range op.ddls {
				name, err := ddl(i)
				require.NoError(t, err, "a statement in the transaction; calls: %v", calls)
				if name != "" {
					created = append(created, name)
				}
			}
			published := false
			for _, name := range created {
				published = published || live[name]
			}
			_, err := f.exec.Execute(ctx, "COMMIT", nil)
			calls = append(calls, "commit")
			afterPublication(published, err, "right after the commit")
		case "rebuild":
			require.NoError(t, f.service.BuildIndexes(ctx))
			calls = append(calls, "rebuild")
		}
	}
	reads.failures.Store(0)
	for _, p := range pending {
		_ = f.service.RefreshNodeWithMutation(ctx, p.mutation)
	}
	pending = nil
	for len(retries) > 0 {
		held := retries
		retries = nil
		for _, mutation := range held {
			if err := f.service.RefreshNodeWithMutation(ctx, mutation); err != nil {
				require.ErrorIs(t, err, search.ErrIndexMemoryBudgetExceeded, "calls: %v", calls)
			}
		}
	}
	requireLiveMatchStorage("once every refresh and retry has landed")
}

// The ordering differential for NOR-386 and NOR-404. Random orders interleave
// storage writes whose refreshes stay pending (drained late, in any order),
// creating and dropping three vector indexes on one property and two labels
// (in their own statements and in explicit transactions, renamed from one to
// another), and a rebuild of the search indexes. Each live index must answer
// exactly as the storage fallback does right after every create, with
// refreshes still pending, and at the end, in every representation. An order
// holds at most one rebuild: a refresh reserved before a rebuild and drained
// after a second one is a drop-free defect on the release head, filed
// separately. Pinned: the round-4 review order (a write's refresh pending
// across DROP, CREATE and a rebuild, drained last) and the round-6 one (an
// index recreated with a write's refresh pending, queried at once).
//
// A second set injects refresh failures (runIncarnationOrder): a failed node
// read and texts beyond the BM25 budget, at drains, retries and the population
// of every create. Pinned: the round-7 review orders (a recreate after a failed
// read, and after a text beyond the budget) and a live index whose node's text
// outgrows the budget, which keeps serving the node's vector (round 9).
func TestNOR386NOR404VectorIndexOrdersMatchStorage(t *testing.T) {
	for _, representation := range []string{"property", "named", "chunks"} {
		t.Run("round4/"+representation, func(t *testing.T) {
			runIncarnationOrder(t, representation, false, []incarnationOp{
				{kind: "write", node: 0, vector: 1},
				{kind: "ddl", ddls: []int{0}},
				{kind: "ddl", ddls: []int{0}},
				{kind: "rebuild"},
				{kind: "drain"},
			})
		})
		t.Run("round6/"+representation, func(t *testing.T) {
			runIncarnationOrder(t, representation, false, []incarnationOp{
				{kind: "write", node: 2, vector: 3},
				{kind: "ddl", ddls: []int{0}},
				{kind: "tx", ddls: []int{1, 2, 1}},
			})
		})
		t.Run("round7-read/"+representation, func(t *testing.T) {
			runIncarnationOrder(t, representation, true, []incarnationOp{
				{kind: "write", node: 0, vector: 3},
				{kind: "ddl", ddls: []int{0}},
				{kind: "failread"},
				{kind: "ddl", ddls: []int{0}},
				{kind: "retry"},
			})
		})
		t.Run("round7-capacity/"+representation, func(t *testing.T) {
			runIncarnationOrder(t, representation, true, []incarnationOp{
				{kind: "write", node: 0, vector: 3, oversize: true},
				{kind: "ddl", ddls: []int{0}},
				{kind: "tx", ddls: []int{0}},
			})
		})
		t.Run("live-capacity/"+representation, func(t *testing.T) {
			runIncarnationOrder(t, representation, true, []incarnationOp{
				{kind: "write", node: 2, vector: 3, oversize: true},
				{kind: "drain"},
				{kind: "write", node: 2, vector: 1},
				{kind: "drain"},
			})
		})
	}
	for _, failures := range []bool{false, true} {
		for seed := int64(1); seed <= 80; seed++ {
			representation := []string{"property", "named", "chunks"}[seed%3]
			t.Run(fmt.Sprintf("failures=%t/%d/%s", failures, seed, representation), func(t *testing.T) {
				r := rand.New(rand.NewSource(seed))
				var ops []incarnationOp
				rebuilt := false
				for step := 0; step < 14; step++ {
					k := r.Intn(10)
					if failures {
						k = r.Intn(12)
					}
					switch {
					case k < 3:
						ops = append(ops, incarnationOp{kind: "write", node: r.Intn(len(incarnationOrderNodes)), vector: r.Intn(len(incarnationOrderVectors)), oversize: failures && r.Intn(3) == 0})
					case k < 5:
						ops = append(ops, incarnationOp{kind: "drain", node: r.Intn(64)})
					case k < 7:
						ops = append(ops, incarnationOp{kind: "ddl", ddls: []int{r.Intn(len(incarnationOrderIndexes))}})
					case k < 9:
						var ddls []int
						for range 1 + r.Intn(3) {
							ddls = append(ddls, r.Intn(len(incarnationOrderIndexes)))
						}
						ops = append(ops, incarnationOp{kind: "tx", ddls: ddls})
					case k == 9:
						if !rebuilt && !failures {
							rebuilt = true
							ops = append(ops, incarnationOp{kind: "rebuild"})
						}
					case k == 10:
						ops = append(ops, incarnationOp{kind: "failread"})
					default:
						ops = append(ops, incarnationOp{kind: "retry"})
					}
				}
				runIncarnationOrder(t, representation, failures, ops)
			})
		}
	}
}

// Review round 6 of #1753 (reviewer probe, kept verbatim): an index recreated
// while a write's refresh is still pending serves the current stored vector at
// once, in every representation.
func TestReview1753R6RecreateBeforeQueuedRefresh(t *testing.T) {
	for _, representation := range []string{"property", "named", "chunks"} {
		t.Run(representation, func(t *testing.T) {
			const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
			f := newIncarnationFixture(t, create, "CREATE (:Doc {id:'a'})")
			id := f.nodeID("a")
			write := func(v []float32) {
				n, err := f.store.GetNode(context.Background(), id)
				require.NoError(t, err)
				setIncarnationVector(n, representation, v)
				require.NoError(t, f.store.UpdateNode(context.Background(), storage.NodeUpdateRequest{After: n}))
			}
			write([]float32{1, 0, 0})
			require.NoError(t, f.service.RefreshNode(context.Background(), id))
			write([]float32{0, 1, 0})
			mutation := f.service.ReserveNodeMutation(id)
			f.run("DROP INDEX docs", nil)
			f.run(create, nil)
			before := f.vectorQueryScores("docs")
			want := f.fallbackScores("docs")
			require.NoError(t, f.service.RefreshNodeWithMutation(context.Background(), mutation))
			after := f.vectorQueryScores("docs")
			t.Logf("before drain=%v storage=%v after drain=%v", before, want, after)
			require.Equal(t, want, before, "a recreated index must serve the current stored vector")
		})
	}
}

// Review round 6 of #1753 (reviewer probe, kept verbatim): a name with no
// vector index reads managed embeddings before the search service's lazy first
// build, as the storage fallback does.
func TestReview1753R6ManagedLazyWarm(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := store.CreateNode(ctx, &storage.Node{ID: "p", Labels: []string{"Doc"}, Properties: map[string]any{"id": "property", "emb": []float32{1, 0, 0}}})
	require.NoError(t, err)
	_, err = store.CreateNode(ctx, &storage.Node{ID: "m", Labels: []string{"Doc"}, Properties: map[string]any{"id": "managed"}, NamedEmbeddings: map[string][]float32{"default": {1, 0, 0}}})
	require.NoError(t, err)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetLazyWarming(true, func() { _ = svc.BuildIndexes(svc.LifetimeContext()) })
	// A live property write reaches search before its initial lazy build.
	require.NoError(t, svc.RefreshNode(ctx, "p"))
	require.False(t, svc.IsReady())
	require.Equal(t, 1, svc.EmbeddingCount())
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	query := "CALL db.index.vector.queryNodes('managed',10,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score"
	want, err := NewStorageExecutor(store).Execute(ctx, query, nil)
	require.NoError(t, err)
	got, err := exec.Execute(ctx, query, nil)
	t.Logf("storage=%v service=%v error=%v ready=%v", want.Rows, got, err, svc.IsReady())
	require.NoError(t, err)
	require.Equal(t, want.Rows, got.Rows)
}

// Review round 6 of #1753 (reviewer probe, kept verbatim): managed embeddings
// of another dimension than the search service's are still read by a name
// with no vector index, through the storage fallback.
func TestReview1753R6ManagedOtherDimensions(t *testing.T) {
	f := newIncarnationFixture(t, "CREATE (:Doc {id:'property', emb:[1.0,0.0,0.0]})")
	_, err := f.store.CreateNode(context.Background(), &storage.Node{
		ID: "managed", Labels: []string{"Doc"}, Properties: map[string]any{"id": "managed"},
		NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}},
	})
	require.NoError(t, err)
	require.NoError(t, f.service.BuildIndexes(context.Background()))
	query := "CALL db.index.vector.queryNodes('managed',10,[1.0,0.0,0.0,0.0]) YIELD node,score RETURN node.id,score"
	fallback, err := NewStorageExecutor(f.store).Execute(context.Background(), query, nil)
	require.NoError(t, err)
	actual, err := f.exec.Execute(context.Background(), query, nil)
	t.Logf("storage=%v service=%v error=%v", fallback.Rows, actual, err)
	require.NoError(t, err, fmt.Sprint(fallback.Rows))
	require.Equal(t, fallback.Rows, actual.Rows)
}

// A newly published vector index serves the stored vectors of writes whose
// refreshes are still pending, through every Cypher route that publishes one:
// CREATE VECTOR INDEX in its own statement and in an explicit transaction, and
// the compatibility procedure; as a first creation over stored nodes and as a
// recreate after a drop; in every representation. Draining the pending
// refresh afterwards changes nothing.
func TestNOR404NewIndexServesQueuedWritesOnEveryRoute(t *testing.T) {
	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	routes := map[string][]string{
		"statement":   {create},
		"transaction": {"BEGIN", create, "COMMIT"},
		"procedure":   {"CALL db.index.vector.createNodeIndex('docs', 'Doc', 'emb', 3, 'cosine')"},
	}
	for _, representation := range []string{"property", "named", "chunks"} {
		for _, recreate := range []bool{false, true} {
			for route, statements := range routes {
				name := fmt.Sprintf("%s/%s/recreate=%t", representation, route, recreate)
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					store := newTestBadgerNamespacedStore(t)
					f := &incarnationFixture{t: t, store: store, exec: NewStorageExecutor(store)}
					if recreate {
						f.run(create, nil)
					}
					node := &storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "a"}}
					setIncarnationVector(node, representation, []float32{1, 0, 0})
					_, err := store.CreateNode(ctx, node)
					require.NoError(t, err)
					f.service = search.NewService(store, 3, nil)
					t.Cleanup(func() { _ = f.service.Close() })
					require.NoError(t, f.service.BuildIndexes(ctx))
					f.exec.SetSearchService(f.service)

					// A write whose refresh is still queued: storage holds v2
					// (score 0.8), search the v1 it indexed (score 1.0).
					stored, err := store.GetNode(ctx, "a")
					require.NoError(t, err)
					setIncarnationVector(stored, representation, []float32{0.6, 0.8, 0})
					require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: stored}))
					queued := f.service.ReserveNodeMutation("a")
					if recreate {
						f.run("DROP INDEX docs", nil)
					}
					for _, statement := range statements {
						f.run(statement, nil)
					}
					f.requireServes("docs", map[interface{}]float64{"a": 0.8}, "the new index at once")
					require.NoError(t, f.service.RefreshNodeWithMutation(ctx, queued))
					f.requireServes("docs", map[interface{}]float64{"a": 0.8}, "after the queued refresh lands")
				})
			}
		}
	}
}

// The same for a relationship vector index recreated while a relationship
// write's refresh is still pending.
func TestNOR404RecreatedRelationshipIndexServesQueuedWrite(t *testing.T) {
	ctx := context.Background()
	const create = "CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb)" + incarnationVectorOptions
	f := newIncarnationFixture(t, create, "CREATE (:Doc {id: 'a'})-[:R {id: 'r', emb: [1.0, 0.0, 0.0]}]->(:Doc {id: 'b'})")
	const query = "CALL db.index.vector.queryRelationships('rels', 10, [1.0, 0.0, 0.0]) YIELD relationship, score RETURN relationship.id, score"
	served := func(exec *StorageExecutor) map[interface{}]float64 {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		return incarnationScores(t, result.Rows)
	}
	require.Equal(t, map[interface{}]float64{"r": 1}, served(f.exec))
	id := storage.EdgeID(fmt.Sprint(f.run("MATCH ()-[r:R]->() RETURN id(r)", nil)[0][0]))
	edge, err := f.store.GetEdge(ctx, id)
	require.NoError(t, err)
	changed := storage.CopyEdge(edge)
	changed.Properties["emb"] = []float64{0.6, 0.8, 0}
	require.NoError(t, f.store.UpdateEdge(ctx, storage.EdgeUpdateRequest{After: changed}))
	queued := f.service.ReserveEdgeMutation(id)
	f.run("DROP INDEX rels", nil)
	f.run(create, nil)
	want := map[interface{}]float64{"r": 0.8}
	require.Equal(t, want, served(NewStorageExecutor(f.store)), "storage fallback")
	require.Equal(t, want, served(f.exec), "the recreated relationship index at once")
	require.NoError(t, f.service.RefreshEdgeWithMutation(ctx, queued))
	require.Equal(t, want, served(f.exec), "after the queued refresh lands")
}

type reviewR7FailOneRead struct {
	storage.Engine
	fail atomic.Bool
}

func (e *reviewR7FailOneRead) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if e.fail.Swap(false) {
		return nil, errors.New("transient node read failure")
	}
	return e.Engine.GetNode(ctx, id)
}

// Review round 7 of #1753 (reviewer probe, kept verbatim): a successful
// publication must not serve the old population when catch-up fails. Either
// report the failure or establish the current population.
func TestReview1753R7PublishAfterFailedCatchUp(t *testing.T) {
	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	for _, failure := range []string{"read", "capacity"} {
		for _, route := range []string{"statement", "transaction", "procedure"} {
			t.Run(failure+"/"+route, func(t *testing.T) {
				ctx := context.Background()
				store := newTestBadgerNamespacedStore(t)
				exec := NewStorageExecutor(store)
				run := func(q string) *ExecuteResult {
					t.Helper()
					r, err := exec.Execute(ctx, q, nil)
					require.NoError(t, err, q)
					return r
				}
				run(create)
				run("CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0], text:'initial'})")
				reads := &reviewR7FailOneRead{Engine: store}
				svc := search.NewService(reads, 3, &search.ServiceOptions{BM25MemoryMaxBytes: 4096})
				t.Cleanup(func() { _ = svc.Close() })
				require.NoError(t, svc.BuildIndexes(ctx))
				exec.SetSearchService(svc)
				var retries []search.RefreshRetry
				svc.SetRefreshRetryOwner(func(work search.RefreshRetry, err error) { retries = append(retries, work) })
				nodes, err := store.GetNodesByLabel(ctx, "Doc")
				require.NoError(t, err)
				require.Len(t, nodes, 1)
				node := storage.CopyNode(nodes[0])
				node.Properties["emb"] = []float64{0.6, 0.8, 0}
				if failure == "capacity" {
					node.Properties["text"] = strings.Repeat("payload ", 2048)
				}
				require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}))
				queued := svc.ReserveNodeMutation(node.ID)
				run("DROP INDEX docs")
				if route == "transaction" {
					run("BEGIN")
					run(create)
				}
				reads.fail.Store(failure == "read")
				publish := create
				if route == "transaction" {
					publish = "COMMIT"
				} else if route == "procedure" {
					publish = "CALL db.index.vector.createNodeIndex('docs','Doc','emb',3,'cosine')"
				}
				_, publishErr := exec.Execute(ctx, publish, nil)
				if publishErr != nil {
					t.Logf("publication reports failure: %v", publishErr)
					return
				}
				actual := incarnationScores(t, run("CALL db.index.vector.queryNodes('docs',10,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score").Rows)
				fallback, err := NewStorageExecutor(store).Execute(ctx, "CALL db.index.vector.queryNodes('docs',10,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score", nil)
				require.NoError(t, err)
				t.Logf("publication succeeded: indexed=%v storage=%v retries=%d", actual, incarnationScores(t, fallback.Rows), len(retries))
				// The old queued reservation cannot repair the capacity case:
				// catch-up already consumed its replacement on apply failure.
				require.NoError(t, svc.RefreshNodeWithMutation(ctx, queued))
				require.Equal(t, incarnationScores(t, fallback.Rows), actual,
					"a successful CREATE must not publish a since-changed pre-drop vector")
			})
		}
	}
}

// incarnationFlakyEdgeReads fails the search service's next relationship read
// while armed.
type incarnationFlakyEdgeReads struct {
	storage.Engine
	fail atomic.Bool
}

func (e *incarnationFlakyEdgeReads) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if e.fail.Swap(false) {
		return nil, errors.New("transient relationship read failure")
	}
	return e.Engine.GetEdge(ctx, id)
}

// A live index serves a node as stored whatever its refresh refuses: a text
// beyond the BM25 budget is refused on its own, and the node's vector, changed
// or not, stays served as stored (round 9). A failed read keeps the refresh
// for the retry owner: the node lags until it lands. The same for a
// relationship index and a failed relationship read.
func TestNOR386LiveRefreshRefusalKeepsStoredEntries(t *testing.T) {
	ctx := context.Background()
	const docs = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	const rels = "CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb)" + incarnationVectorOptions
	store := newTestBadgerNamespacedStore(t)
	f := &incarnationFixture{t: t, store: store, exec: NewStorageExecutor(store)}
	f.run(docs, nil)
	f.run(rels, nil)
	f.run("CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0], text: 'short'})-[:R {id: 'r', emb: [1.0, 0.0, 0.0]}]->(:Doc {id: 'b', emb: [0.0, 1.0, 0.0], text: 'short'})", nil)
	reads := &incarnationFlakyReads{Engine: store}
	edgeReads := &incarnationFlakyEdgeReads{Engine: reads}
	f.service = search.NewService(edgeReads, 3, &search.ServiceOptions{BM25MemoryMaxBytes: 4096})
	t.Cleanup(func() { _ = f.service.Close() })
	require.NoError(t, f.service.BuildIndexes(ctx))
	f.exec.SetSearchService(f.service)
	var retries []search.RefreshRetry
	f.service.SetRefreshRetryOwner(func(work search.RefreshRetry, err error) { retries = append(retries, work) })
	f.requireServes("docs", map[interface{}]float64{"a": 1, "b": 0.5}, "before any failure")
	a := f.nodeID("a")
	write := func(vector []float64, text string) {
		t.Helper()
		stored, err := store.GetNode(ctx, a)
		require.NoError(t, err)
		changed := storage.CopyNode(stored)
		changed.Properties["emb"] = vector
		changed.Properties["text"] = text
		require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: changed}))
	}

	// A text beyond the budget, the vector unchanged: the vector stays.
	write([]float64{1, 0, 0}, strings.Repeat("payload ", 1024))
	require.ErrorIs(t, f.service.RefreshNode(ctx, a), search.ErrIndexMemoryBudgetExceeded)
	f.requireServes("docs", map[interface{}]float64{"a": 1, "b": 0.5}, "after a text beyond the budget")
	// And with a changed vector: the stored vector is installed.
	write([]float64{0.6, 0.8, 0}, strings.Repeat("payload ", 1024))
	require.ErrorIs(t, f.service.RefreshNode(ctx, a), search.ErrIndexMemoryBudgetExceeded)
	f.requireServes("docs", map[interface{}]float64{"a": 0.8, "b": 0.5}, "after a changed vector with a text beyond the budget")
	write([]float64{0.6, 0.8, 0}, "short again")
	require.NoError(t, f.service.RefreshNode(ctx, a))
	f.requireServes("docs", map[interface{}]float64{"a": 0.8, "b": 0.5}, "after a write that fits")

	// A failed read: the node lags until the retry lands.
	write([]float64{0, 0, 1}, "short")
	reads.failures.Store(1)
	require.Error(t, f.service.RefreshNode(ctx, a))
	require.Equal(t, map[interface{}]float64{"a": 0.8, "b": 0.5}, f.vectorQueryScores("docs"), "the refresh is held for retry; the node lags")
	require.Len(t, retries, 1)
	require.NoError(t, f.service.RefreshNodeWithMutation(ctx, retries[0].Node))
	f.requireServes("docs", map[interface{}]float64{"a": 0.5, "b": 0.5}, "after the retry")

	// A relationship whose read fails.
	const query = "CALL db.index.vector.queryRelationships('rels', 10, [1.0, 0.0, 0.0]) YIELD relationship, score RETURN relationship.id, score"
	served := func(exec *StorageExecutor) map[interface{}]float64 {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		return incarnationScores(t, result.Rows)
	}
	require.Equal(t, map[interface{}]float64{"r": 1}, served(f.exec))
	r := storage.EdgeID(fmt.Sprint(f.run("MATCH ()-[r:R]->() RETURN id(r)", nil)[0][0]))
	edge, err := store.GetEdge(ctx, r)
	require.NoError(t, err)
	changed := storage.CopyEdge(edge)
	changed.Properties["emb"] = []float64{0.6, 0.8, 0}
	require.NoError(t, store.UpdateEdge(ctx, storage.EdgeUpdateRequest{After: changed}))
	edgeReads.fail.Store(true)
	require.Error(t, f.service.RefreshEdge(ctx, r))
	require.Equal(t, map[interface{}]float64{"r": 1}, served(f.exec), "the refresh is held for retry; the relationship lags")
	require.Len(t, retries, 2)
	require.NoError(t, f.service.RefreshEdgeWithMutation(ctx, retries[1].Edge))
	require.Equal(t, map[interface{}]float64{"r": 0.8}, served(f.exec))
	require.Equal(t, map[interface{}]float64{"r": 0.8}, served(NewStorageExecutor(store)))
}

// Review round 8 of #1753 (reviewer probe): an index whose population failed
// is FAILED and refuses queries, as Neo4j's do. Round 9 moved its trigger from
// the BM25 budget to the vector budget: a text the BM25 budget refuses no
// longer takes the node's vector out of search, so it no longer fails a vector
// index's population; a stored vector the vector budget refuses does. Its
// assertions are unchanged.
func TestReview1753R8PopulationFailureIsNotQueryable(t *testing.T) {
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	exec := NewStorageExecutor(store)
	run := func(q string) *ExecuteResult {
		t.Helper()
		r, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
		return r
	}
	run("CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0], text:'initial'})")
	run("CREATE (:Doc {id:'b', emb:[0.0,1.0,0.0], text:'initial'})")
	// Two 3-dimensional vectors fit the vector budget; a third does not.
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 60})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)
	created := &storage.Node{ID: "c", Labels: []string{"Doc"}, Properties: map[string]any{"id": "c", "emb": []float64{0.6, 0.8, 0}, "text": "initial"}}
	_, err := store.CreateNode(ctx, created)
	require.NoError(t, err)
	svc.ReserveNodeMutation(created.ID)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	require.Error(t, err)
	t.Logf("CREATE error: %v", err)
	shown := run("SHOW VECTOR INDEXES YIELD name, state RETURN name, state")
	t.Logf("SHOW VECTOR INDEXES: %v", shown.Rows)
	rows, queryErr := exec.Execute(ctx, "CALL db.index.vector.queryNodes('docs',10,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score", nil)
	if rows != nil {
		t.Logf("query rows after failed population: %v", rows.Rows)
	}
	require.Error(t, queryErr, "Neo4j refuses queries against an index whose population failed")
}

type reviewR8PublicationGate struct {
	storage.Engine
	armed            atomic.Bool
	entered, release chan struct{}
}

func (e *reviewR8PublicationGate) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if e.armed.Swap(false) {
		close(e.entered)
		<-e.release
	}
	return e.Engine.GetNode(ctx, id)
}

// Review round 8 of #1753 (reviewer probe, kept verbatim): a query of an index
// still populating waits for it, and refuses rather than serve the pre-drop
// vector when the wait ends first.
func TestReview1753R8QueryDuringRecreationWaitsForPopulation(t *testing.T) {
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	exec := NewStorageExecutor(store)
	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	_, err := exec.Execute(ctx, create, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	gate := &reviewR8PublicationGate{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
	svc := search.NewService(gate, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)
	nodes, err := store.GetNodesByLabel(ctx, "Doc")
	require.NoError(t, err)
	changed := storage.CopyNode(nodes[0])
	changed.Properties["emb"] = []float64{0.6, 0.8, 0}
	require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: changed}))
	svc.ReserveNodeMutation(changed.ID)
	_, err = exec.Execute(ctx, "DROP INDEX docs", nil)
	require.NoError(t, err)
	gate.armed.Store(true)
	done := make(chan error, 1)
	go func() { _, err := exec.Execute(ctx, create, nil); done <- err }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		close(gate.release)
		<-done
		t.Fatal("creation did not catch up")
	}
	other := NewStorageExecutor(store)
	other.SetSearchService(svc)
	result, queryErr := other.Execute(ctx, "CALL db.index.vector.queryNodes('docs',10,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score", nil)
	close(gate.release)
	require.NoError(t, <-done)
	if queryErr != nil {
		t.Logf("query refused while populating: %v", queryErr)
		return
	}
	t.Logf("query during population: %v", result.Rows)
	require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationScores(t, result.Rows), "a concurrent reader must not see a pre-drop vector while CREATE populates")
}

// A vector index has Neo4j's lifecycle. While its population runs it is
// POPULATING: SHOW and db.indexes() say so, and a query from another session
// waits for it, then answers the current vector, or fails as Neo4j does when
// the wait ends first. A population that fails leaves it FAILED, with the
// cause, refusing queries until it is dropped and created again, which then
// populates it. A query in the transaction that creates an index does not
// wait for it.
func TestNOR404VectorIndexLifecycle(t *testing.T) {
	ctx := context.Background()
	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	const query = "CALL db.index.vector.queryNodes('docs', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.id, score"
	state := func(exec *StorageExecutor) []interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name, state, populationPercent, failureMessage RETURN name, state, populationPercent, failureMessage", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		return result.Rows[0]
	}
	setup := func(t *testing.T, options *search.ServiceOptions) (*StorageExecutor, *reviewR8PublicationGate, *search.Service, storage.Engine) {
		t.Helper()
		store := newTestBadgerNamespacedStore(t)
		exec := NewStorageExecutor(store)
		_, err := exec.Execute(ctx, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0], text: 'short'})", nil)
		require.NoError(t, err)
		gate := &reviewR8PublicationGate{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
		svc := search.NewService(gate, 3, options)
		t.Cleanup(func() { _ = svc.Close() })
		require.NoError(t, svc.BuildIndexes(ctx))
		exec.SetSearchService(svc)
		nodes, err := store.GetNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		changed := storage.CopyNode(nodes[0])
		changed.Properties["emb"] = []float64{0.6, 0.8, 0}
		require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: changed}))
		svc.ReserveNodeMutation(changed.ID)
		return exec, gate, svc, store
	}
	// populating starts CREATE with its population parked, and returns the
	// statement's outcome.
	populating := func(t *testing.T, exec *StorageExecutor, gate *reviewR8PublicationGate) chan error {
		t.Helper()
		gate.armed.Store(true)
		done := make(chan error, 1)
		go func() { _, err := exec.Execute(ctx, create, nil); done <- err }()
		select {
		case <-gate.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the population did not start")
		}
		return done
	}

	t.Run("a query waits for the population", func(t *testing.T) {
		exec, gate, svc, store := setup(t, nil)
		done := populating(t, exec, gate)
		other := NewStorageExecutor(store)
		other.SetSearchService(svc)
		require.Equal(t, []interface{}{"docs", "POPULATING", 0.0, ""}, state(other))
		indexes, err := other.Execute(ctx, "CALL db.indexes()", nil)
		require.NoError(t, err)
		listed := map[interface{}]interface{}{}
		for _, row := range indexes.Rows {
			listed[row[0]] = row[len(row)-1]
		}
		require.Equal(t, "POPULATING", listed["docs"])
		answered := make(chan *ExecuteResult, 1)
		failed := make(chan error, 1)
		go func() {
			result, err := other.Execute(ctx, query, nil)
			if err != nil {
				failed <- err
				return
			}
			answered <- result
		}()
		select {
		case <-answered:
			t.Fatal("a query answered while the index was populating")
		case err := <-failed:
			t.Fatalf("a query failed while waiting for the population: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
		close(gate.release)
		require.NoError(t, <-done)
		select {
		case result := <-answered:
			require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationScores(t, result.Rows))
		case err := <-failed:
			t.Fatalf("the waiting query failed: %v", err)
		}
		require.Equal(t, []interface{}{"docs", "ONLINE", 100.0, ""}, state(other))
	})

	t.Run("a query that waits too long fails", func(t *testing.T) {
		previous := vectorIndexOnlineQueryTimeout
		vectorIndexOnlineQueryTimeout = 100 * time.Millisecond
		t.Cleanup(func() { vectorIndexOnlineQueryTimeout = previous })
		exec, gate, svc, store := setup(t, nil)
		done := populating(t, exec, gate)
		other := NewStorageExecutor(store)
		other.SetSearchService(svc)
		_, err := other.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "Expected index to come online within a reasonable time.")
		close(gate.release)
		require.NoError(t, <-done)
		result, err := other.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationScores(t, result.Rows))
	})

	t.Run("a failed population leaves the index FAILED until it is created again", func(t *testing.T) {
		// One 3-dimensional vector fits the vector budget; a second does not.
		exec, _, svc, store := setup(t, &search.ServiceOptions{VectorMemoryMaxBytes: 40})
		beyond := &storage.Node{ID: "z", Labels: []string{"Doc"}, Properties: map[string]any{"id": "z", "emb": []float64{0, 0, 1}}}
		_, err := store.CreateNode(ctx, beyond)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, create, nil)
		require.ErrorContains(t, err, "population failed")
		shown := state(exec)
		require.Equal(t, []interface{}{"docs", "FAILED"}, shown[:2])
		require.Contains(t, shown[3], "index memory budget exceeded")
		_, err = exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "Index `docs` entered a FAILED state. Please see database logs.")

		require.NoError(t, store.DeleteNode(ctx, beyond.ID))
		svc.ReserveNodeMutation(beyond.ID) // the delete's refresh, still queued
		_, err = exec.Execute(ctx, query, nil)
		require.Error(t, err, "a FAILED index stays FAILED until it is dropped and created again")
		_, err = exec.Execute(ctx, "DROP INDEX docs", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, create, nil)
		require.NoError(t, err)
		require.Equal(t, []interface{}{"docs", "ONLINE", 100.0, ""}, state(exec))
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationScores(t, result.Rows))
	})

	t.Run("the creating transaction does not wait for its own index", func(t *testing.T) {
		exec, _, _, _ := setup(t, nil)
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, create, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		require.Equal(t, []interface{}{"docs", "ONLINE", 100.0, ""}, state(exec))
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, map[interface{}]float64{"a": 0.8}, incarnationScores(t, result.Rows))
	})
}

// Population records end with their indexes (round 9 of #1753): vector
// indexes of 128 distinct names created and dropped leave none, the drop of a
// FAILED index takes its record with it, and the committed recreation of a
// dropped name keeps its own population's record.
func TestNOR404PopulationRecordsEndWithTheirIndex(t *testing.T) {
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]})", nil)
	require.NoError(t, err)
	// One 3-dimensional vector fits the vector budget; a second does not.
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 40})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)
	run := func(q string) error {
		_, err := exec.Execute(ctx, q, nil)
		return err
	}
	records := func() int {
		return reflect.ValueOf(svc).Elem().FieldByName("vectorIndexPopulations").FieldByName("records").Len()
	}
	for i := range 128 {
		name := fmt.Sprintf("migration_%d", i)
		require.NoError(t, run("CREATE VECTOR INDEX "+name+" FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions))
		require.NoError(t, run("DROP INDEX "+name))
	}
	require.Zero(t, records(), "created and dropped indexes leave no population record")

	const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
	beyond := &storage.Node{ID: "z", Labels: []string{"Doc"}, Properties: map[string]any{"id": "z", "emb": []float64{0, 0, 1}}}
	_, err = store.CreateNode(ctx, beyond)
	require.NoError(t, err)
	require.ErrorContains(t, run(create), "population failed")
	require.Equal(t, 1, records(), "a FAILED index keeps its record")
	require.NoError(t, run("DROP INDEX docs"))
	require.Zero(t, records(), "its drop removes it")

	require.ErrorContains(t, run(create), "population failed")
	require.NoError(t, store.DeleteNode(ctx, beyond.ID))
	require.NoError(t, run("BEGIN"))
	require.NoError(t, run("DROP INDEX docs"))
	require.NoError(t, run(create))
	require.NoError(t, run("COMMIT"))
	state, _ := svc.VectorIndexState("docs")
	require.Equal(t, search.VectorIndexOnline, state)
	require.Equal(t, 1, records(), "the created index keeps the record of the population that served it")
	require.NotNil(t, svc.VectorIndexPopulationOf("docs"), "the dropped index's record is not restored")
}

// Review round 10 of #1753 (reviewer probe): an index created and dropped in
// one transaction, alone or replaced by one of the same name on another
// label, is not populated by the COMMIT; only the definition the transaction
// publishes is.
func TestReview1753R10DroppedIntermediatePopulation(t *testing.T) {
	for _, last := range []string{"drop_all", "replace_with_other_label"} {
		t.Run(last, func(t *testing.T) {
			ctx := context.Background()
			store := newTestBadgerNamespacedStore(t)
			_, err := store.CreateNode(ctx, &storage.Node{ID: "a", Labels: []string{"Doc", "Note"}, Properties: map[string]any{"emb": []float64{1, 0, 0}}})
			require.NoError(t, err)
			svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 40})
			t.Cleanup(func() { _ = svc.Close() })
			require.NoError(t, svc.BuildIndexes(ctx))
			_, err = store.CreateNode(ctx, &storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"emb": []float64{0, 1, 0}}})
			require.NoError(t, err)
			require.ErrorIs(t, svc.RefreshNode(ctx, "b"), search.ErrIndexMemoryBudgetExceeded)
			exec := NewStorageExecutor(store)
			exec.SetSearchService(svc)
			run := func(q string) {
				t.Helper()
				_, err := exec.Execute(ctx, q, nil)
				require.NoError(t, err, q)
			}
			run("BEGIN")
			run("CREATE VECTOR INDEX temporary FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:3,`vector.similarity_function`:'cosine'}}")
			run("DROP INDEX temporary")
			if last == "replace_with_other_label" {
				run("CREATE VECTOR INDEX temporary FOR (n:Note) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:3,`vector.similarity_function`:'cosine'}}")
			}
			_, commitErr := exec.Execute(ctx, "COMMIT", nil)
			shown, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
			require.NoError(t, err)
			t.Logf("commit=%v finalIndexes=%v", commitErr, shown.Rows)
			require.NoError(t, commitErr, "a transaction must populate only indexes it actually publishes")
			if last == "drop_all" {
				require.Empty(t, shown.Rows)
				require.Nil(t, svc.VectorIndexPopulationOf("temporary"), "no population record outlives its index")
			} else {
				require.Equal(t, [][]interface{}{{"temporary", "ONLINE"}}, shown.Rows)
			}
		})
	}
}

// Review round 10 of #1753 (reviewer probe): an index whose declared
// dimensions differ from the search service's is read from storage by its
// queries, so its population uses the same source decision and does not fail
// on a refused managed chunk the index never reads.
func TestReview1753R10OtherDimensionPopulationUsesQuerySource(t *testing.T) {
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	_, err := store.CreateNode(ctx, &storage.Node{ID: "a", Labels: []string{"Other"}, Properties: map[string]any{"vec": []float64{1, 0, 0}}})
	require.NoError(t, err)
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 40})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	// The managed embedding has the service's dimensions; this ordinary property
	// vector has another supported dimension and is served by storage fallback.
	_, err = store.CreateNode(ctx, &storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"emb": []float64{1, 0}}, ChunkEmbeddings: [][]float32{{0, 1, 0}}})
	require.NoError(t, err)
	require.ErrorIs(t, svc.RefreshNode(ctx, "b"), search.ErrIndexMemoryBudgetExceeded)
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	_, createErr := exec.Execute(ctx, "CREATE VECTOR INDEX two_dims FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:2,`vector.similarity_function`:'cosine'}}", nil)
	fallback := NewStorageExecutor(store)
	const query = "CALL db.index.vector.queryNodes('two_dims',10,[1.0,0.0]) YIELD node,score RETURN score"
	stored, err := fallback.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{float64(1)}}, stored.Rows)
	shown, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
	require.NoError(t, err)
	rows, queryErr := exec.Execute(ctx, query, nil)
	t.Logf("creation=%v state=%v indexed=%v queryError=%v storage=%v", createErr, shown.Rows, rows, queryErr, stored.Rows)
	require.NoError(t, createErr, "the index's property is served by the existing other-dimension fallback; unrelated refused managed chunks must not fail this index")
	require.NoError(t, queryErr)
	require.Equal(t, stored.Rows, rows.Rows)
}

// Review round 10 of #1753 (reviewer probe, passing): refreshes pending across
// two sequential builds serve the stored vectors, in every representation.
func TestReview1753R10MultipleBuildsPendingRefresh(t *testing.T) {
	for _, representation := range []string{"property", "named", "chunks"} {
		t.Run(representation, func(t *testing.T) {
			runIncarnationOrder(t, representation, false, []incarnationOp{
				{kind: "write", node: 0, vector: 1},
				{kind: "rebuild"},
				{kind: "write", node: 1, vector: 2},
				{kind: "rebuild"},
				{kind: "drain", node: 0},
			})
		})
	}
}

// awaitPopulationWaiter returns once a goroutine is blocked waiting for a
// vector index population (search.(*Service).AwaitVectorIndexOnline).
func awaitPopulationWaiter(t *testing.T) {
	t.Helper()
	stacks := make([]byte, 1<<22)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		n := runtime.Stack(stacks, true)
		for _, goroutine := range strings.Split(string(stacks[:n]), "\n\n") {
			if strings.Contains(goroutine, "[select") && strings.Contains(goroutine, "search.(*Service).AwaitVectorIndexOnline") {
				return
			}
		}
	}
	t.Fatal("no query started waiting for the population")
}

// Review of #1753 after round 11 (r4508547200): a query waiting for a
// population wakes when the committed drop of its index retires that
// population, and reads the index as it is when its wait ends. A migration
// drops the index and creates the name again on another label (type) and
// property while the old population is parked: the query neither waits out
// the retired population nor reads the old definition.
func TestReview1753DropDuringPopulationWaitReadsTheNewDefinition(t *testing.T) {
	for _, tc := range []struct {
		name, data, create, recreate, query string
		want                                map[interface{}]float64
	}{{
		name:     "nodes",
		data:     "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0]}), (:Note {id: 'n', vec: [0.0, 1.0, 0.0]})",
		create:   "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions,
		recreate: "CREATE VECTOR INDEX docs FOR (n:Note) ON (n.vec)" + incarnationVectorOptions,
		query:    "CALL db.index.vector.queryNodes('docs', 10, [0.0, 1.0, 0.0]) YIELD node, score RETURN node.id, score",
		want:     map[interface{}]float64{"n": 1},
	}, {
		name:     "relationships",
		data:     "CREATE (:Doc {id: 'a'})-[:R {id: 'r', emb: [1.0, 0.0, 0.0]}]->(:Doc {id: 'b'})-[:S {id: 's', vec: [0.0, 1.0, 0.0]}]->(:Doc {id: 'c'})",
		create:   "CREATE VECTOR INDEX docs FOR ()-[r:R]-() ON (r.emb)" + incarnationVectorOptions,
		recreate: "CREATE VECTOR INDEX docs FOR ()-[r:S]-() ON (r.vec)" + incarnationVectorOptions,
		query:    "CALL db.index.vector.queryRelationships('docs', 10, [0.0, 1.0, 0.0]) YIELD relationship, score RETURN relationship.id, score",
		want:     map[interface{}]float64{"s": 1},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestBadgerNamespacedStore(t)
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(ctx, tc.data, nil)
			require.NoError(t, err)
			gate := &reviewR8PublicationGate{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
			svc := search.NewService(gate, 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			require.NoError(t, svc.BuildIndexes(ctx))
			exec.SetSearchService(svc)
			// A refresh still reserved makes the population read a node,
			// where the gate parks it.
			nodes, err := store.GetNodesByLabel(ctx, "Doc")
			require.NoError(t, err)
			svc.ReserveNodeMutation(nodes[0].ID)
			gate.armed.Store(true)
			created := make(chan error, 1)
			go func() { _, err := exec.Execute(ctx, tc.create, nil); created <- err }()
			released := false
			release := func() {
				if !released {
					released = true
					close(gate.release)
				}
			}
			t.Cleanup(release)
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the population did not start")
			}

			reader := NewStorageExecutor(store)
			reader.SetSearchService(svc)
			answered := make(chan *ExecuteResult, 1)
			failed := make(chan error, 1)
			go func() {
				result, err := reader.Execute(ctx, tc.query, nil)
				if err != nil {
					failed <- err
					return
				}
				answered <- result
			}()
			awaitPopulationWaiter(t)

			migrator := NewStorageExecutor(store)
			migrator.SetSearchService(svc)
			migrated := make(chan error, 1)
			go func() {
				for _, statement := range []string{"BEGIN", "DROP INDEX docs", tc.recreate, "COMMIT"} {
					if _, err := migrator.Execute(ctx, statement, nil); err != nil {
						migrated <- fmt.Errorf("%s: %w", statement, err)
						return
					}
				}
				migrated <- nil
			}()
			select {
			case err := <-migrated:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("the migration did not commit while the old population was parked")
			}
			select {
			case result := <-answered:
				require.Equal(t, tc.want, incarnationScores(t, result.Rows), "the query reads the index as it is when its wait ends")
			case err := <-failed:
				t.Fatalf("the waiting query failed: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("the query still waits for the population its index's drop retired")
			}
			release()
			require.NoError(t, <-created)
		})
	}
}

// Review round 12 of #1753 (reviewer probe of r4508564823): a node whose
// update a vector budget refuses is not served from another of its
// representations: an index on the property never answers the unchanged
// chunk in place of the refused property.
func TestReview1753R12RefusedPropertyMustNotFallBackToChunk(t *testing.T) {
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	_, err := store.CreateNode(ctx, &storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "a", "emb": []float64{1, 0, 0}}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
	require.NoError(t, err)
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 60})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)"+incarnationVectorOptions, nil)
	require.NoError(t, err)
	old, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	updated := storage.CopyNode(old)
	updated.Properties["emb"] = []float64{0.6, 0.8, 0}
	updated.Properties["third"] = []float64{0, 0, 1}
	require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: updated}))
	require.ErrorIs(t, svc.RefreshNode(ctx, "a"), search.ErrIndexMemoryBudgetExceeded)
	const query = "CALL db.index.vector.queryNodes('docs',10,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score"
	rows, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	fallback, err := NewStorageExecutor(store).Execute(ctx, query, nil)
	require.NoError(t, err)
	got, want := incarnationScores(t, rows.Rows), incarnationScores(t, fallback.Rows)
	t.Logf("served=%v storage=%v", got, want)
	for id, score := range got {
		require.Equal(t, want[id], score, "a refused preferred property must not substitute the unchanged lower-precedence chunk")
	}
}

// Review round 14 of #1753 (reviewer probe): a relationship vector index whose
// population failed (FAILED) is not read by a plain MATCH: it answers from
// storage, every stored relationship included, as with no index.
func TestReview1753R14FailedRelationshipMatch(t *testing.T) {
	ctx := context.Background()
	store := newTestBadgerNamespacedStore(t)
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := store.CreateNode(ctx, &storage.Node{ID: id})
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "r1", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"id": "best", "emb": []float64{1, 0, 0}}},
		{ID: "r2", StartNode: "b", EndNode: "c", Type: "R", Properties: map[string]any{"id": "other", "emb": []float64{0, 1, 0}}},
	} {
		require.NoError(t, store.CreateEdge(ctx, edge))
	}
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 30})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	changed, err := store.GetEdge(ctx, "r1")
	require.NoError(t, err)
	changed = storage.CopyEdge(changed)
	changed.Properties["extra"] = []float64{0, 0, 1}
	require.NoError(t, store.UpdateEdge(ctx, storage.EdgeUpdateRequest{After: changed}))
	require.ErrorIs(t, svc.RefreshEdge(ctx, changed.ID), search.ErrIndexMemoryBudgetExceeded)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	t.Logf("CREATE error=%v", err)
	state, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
	require.NoError(t, err)
	t.Logf("state=%v", state.Rows)
	const q = "MATCH ()-[r:R]->() RETURN r.id AS id, vector.similarity.cosine(r.emb, [1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 10"
	got, err := exec.Execute(ctx, q, nil)
	require.NoError(t, err)
	want, err := NewStorageExecutor(store).Execute(ctx, q, nil)
	require.NoError(t, err)
	t.Logf("wired=%v storage=%v", got.Rows, want.Rows)
	require.Equal(t, want.Rows, got.Rows, "a FAILED index must not make a plain MATCH omit stored relationships")
}

// Review round 15 of #1753 (Rule 73 audit #55): one decision answers, for
// every query bound to a schema vector index, whether the wired search
// service or storage answers (vectorIndexServiceAnswers). The index must be
// ONLINE. For nodes, a warm read (the query procedures, the publication)
// warms a lazy service and needs it ready, while a passive read (scalar
// MATCH) never warms it and also reads the vectors the service already holds
// for the property. For relationships, the service must hold vectors of the
// type and property.
func TestReview1753R15VectorIndexServiceAnswers(t *testing.T) {
	ctx := context.Background()
	nodes := search.VectorIndexTarget{Label: "Doc", Property: "emb"}
	others := search.VectorIndexTarget{Label: "Doc", Property: "other"}
	rels := search.VectorIndexTarget{Label: "R", Property: "emb", Relationship: true}
	// setup wires svc and writes a node and a relationship vector through
	// Cypher, which installs them live.
	setup := func(t *testing.T, svc func(storage.Engine) *search.Service) (*StorageExecutor, *search.Service) {
		t.Helper()
		store := newTestBadgerNamespacedStore(t)
		exec := NewStorageExecutor(store)
		service := svc(store)
		t.Cleanup(func() { _ = service.Close() })
		exec.SetSearchService(service)
		_, err := exec.Execute(ctx, "CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]})-[:R {id:'r', emb:[1.0,0.0,0.0]}]->(:Doc {id:'b', emb:[0.0,1.0,0.0]})", nil)
		require.NoError(t, err)
		require.Equal(t, 2, service.CountPropertyVectorEntries("emb"))
		require.True(t, service.HasRelationshipVectorEntries("R", "emb"))
		return exec, service
	}
	answers := func(t *testing.T, exec *StorageExecutor, index string, target search.VectorIndexTarget, dims int, read vectorQueryRead) bool {
		t.Helper()
		served, err := exec.vectorIndexServiceAnswers(ctx, exec.search(), index, target, dims, read)
		require.NoError(t, err)
		return served
	}

	t.Run("no service", func(t *testing.T) {
		exec := NewStorageExecutor(newTestBadgerNamespacedStore(t))
		for _, read := range []vectorQueryRead{vectorReadWarm, vectorReadPassive} {
			require.False(t, answers(t, exec, "", nodes, 3, read))
			require.False(t, answers(t, exec, "", rels, 3, read))
		}
	})

	t.Run("unready lazy service", func(t *testing.T) {
		var warmed atomic.Int32
		exec, svc := setup(t, func(store storage.Engine) *search.Service {
			svc := search.NewService(store, 3, nil)
			svc.SetLazyWarming(true, search.WarmFunc(func() {
				warmed.Add(1)
				_ = svc.BuildIndexes(context.Background())
			}))
			return svc
		})
		require.False(t, svc.IsReady())
		require.True(t, answers(t, exec, "", nodes, 3, vectorReadPassive), "a passive read uses the vectors already held")
		require.False(t, answers(t, exec, "", others, 3, vectorReadPassive), "a property the service holds nothing of is read from storage")
		require.False(t, answers(t, exec, "", nodes, 2, vectorReadPassive), "other dimensions are read from storage")
		require.True(t, answers(t, exec, "", rels, 3, vectorReadPassive))
		require.Zero(t, warmed.Load(), "a passive read never warms the service")
		require.True(t, answers(t, exec, "", nodes, 3, vectorReadWarm), "a warm read warms the service, then reads it")
		require.Equal(t, int32(1), warmed.Load())
		require.True(t, svc.IsReady())
	})

	t.Run("unready service without lazy warming", func(t *testing.T) {
		exec, svc := setup(t, func(store storage.Engine) *search.Service { return search.NewService(store, 3, nil) })
		require.False(t, svc.IsReady())
		require.False(t, answers(t, exec, "", nodes, 3, vectorReadWarm), "a warm read needs the service ready")
		require.True(t, answers(t, exec, "", nodes, 3, vectorReadPassive))
		require.True(t, answers(t, exec, "", rels, 3, vectorReadWarm))
	})

	t.Run("an index that isn't ONLINE", func(t *testing.T) {
		exec, svc := setup(t, func(store storage.Engine) *search.Service { return search.NewService(store, 3, nil) })
		require.NoError(t, svc.BuildIndexes(ctx))
		docs := svc.BeginVectorIndexPopulation("docs", nodes)
		edges := svc.BeginVectorIndexPopulation("rels", rels)
		for _, read := range []vectorQueryRead{vectorReadWarm, vectorReadPassive} {
			require.False(t, answers(t, exec, "docs", nodes, 3, read), "a POPULATING node index is read from storage")
			require.False(t, answers(t, exec, "rels", rels, 3, read), "a POPULATING relationship index is read from storage")
		}
		require.True(t, answers(t, exec, "", nodes, 3, vectorReadWarm), "the publication, which establishes the state, is not gated by it")
		require.NoError(t, docs.Populate(ctx, true))
		require.NoError(t, edges.Populate(ctx, true))
		for _, read := range []vectorQueryRead{vectorReadWarm, vectorReadPassive} {
			require.True(t, answers(t, exec, "docs", nodes, 3, read))
			require.True(t, answers(t, exec, "rels", rels, 3, read))
		}
	})
}

// Review round 15 of #1753 (reviewer probes): the publication populates the
// service for every reader of the index, not only for the vector query
// procedures. An unready service that already holds one of two stored
// vectors is read by MATCH without warming, so CREATE must populate it
// before the index is ONLINE, implicitly and in an explicit transaction.
func TestReview1753R15CreateWithPassiveService(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			ctx := context.Background()
			store := newTestBadgerNamespacedStore(t)
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(ctx, "CREATE (:Doc {id:'a',emb:[1.0,0.0,0.0]}),(:Doc {id:'b',emb:[0.0,1.0,0.0]})", nil)
			require.NoError(t, err)
			svc := search.NewService(store, 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			exec.SetSearchService(svc)
			nodes, err := store.GetNodesByLabel(ctx, "Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			require.NoError(t, svc.IndexNode(nodes[0]))
			require.False(t, svc.IsReady())
			statements := []string{"CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions}
			if explicit {
				statements = append([]string{"BEGIN"}, append(statements, "COMMIT")...)
			}
			for _, q := range statements {
				_, err = exec.Execute(ctx, q, nil)
				require.NoError(t, err, q)
			}
			shown, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
			require.NoError(t, err)
			t.Logf("after CREATE: %v ready=%t held=%d", shown.Rows, svc.IsReady(), svc.CountPropertyVectorEntries("emb"))
			require.Equal(t, [][]interface{}{{"docs", "ONLINE"}}, shown.Rows)
			require.Equal(t, 2, svc.CountPropertyVectorEntries("emb"), "CREATE actually populates the service independently of a query's storage fallback")
			procedure, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('docs',10,[1.0,0.0,0.0]) YIELD node,score RETURN node.id,score", nil)
			require.NoError(t, err)
			match, err := exec.Execute(ctx, "MATCH (n:Doc) RETURN n.id,vector.similarity.cosine(n.emb,[1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 10", nil)
			require.NoError(t, err)
			t.Logf("procedure=%v MATCH=%v", procedure.Rows, match.Rows)
			require.Equal(t, incarnationScores(t, procedure.Rows), incarnationScores(t, match.Rows), "a newly ONLINE index must serve all its stored nodes to passive MATCH too")
		})
	}
}
