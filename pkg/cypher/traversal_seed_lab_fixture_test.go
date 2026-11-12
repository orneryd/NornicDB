package cypher

// NOR-414 seed-shape A and B fixtures.
//
// Shape A is the reported collection/document control population: 172,000
// nodes, including 171,999 PDGraphNode nodes with 5,280 PDDocument members,
// and one PDCollection at n5280,
// 351 MEMBER_OF relationships (n0..n350 -> n5280) and 289,649 PD_RELATION
// relationships, 290,000 relationships in total. Its reported statements are
// the inverse traversal, which starts at the one collection, and the original
// forward control, which starts at the matched documents.
//
// Shape B is the reported keyed population: the same 172,000 nodes and 5,280
// documents with 5,280 MEMBER_OF relationships, 284,720 PD_RELATION
// relationships and one n6001 -[:HAS_ORIGINAL]-> n6000 relationship, 290,001
// relationships in total, plus the PDDocument/PDCollection/PDGraphNode/PDVersion
// unique keys, the PDOriginal content_hash key and the PD_RELATION key. Its
// reported statements are COLLECTION_DOCUMENTS and CONTENT_DOCUMENT, both
// selected by inline unique keys without a WHERE clause.
//
// The population is the original native Go harness of the NOR-414 laboratory
// reference in the NOR-414 comments, which is the authority where its text
// Cypher translation differs. Followed, with the differences recorded here:
//
//   - Shape B constraint names. The text Cypher names the node constraints
//     ck_doc, ck_collection, ck_graph and ck_version; the harness names them
//     ck_<label>, so this fixture declares ck_PDDocument, ck_PDCollection,
//     ck_PDGraphNode and ck_PDVersion. ck_hash and pd_relation_id keep the
//     reported names, and shape A's pd_document_id, pd_graph_node_id and
//     pd_relation_id names match the text Cypher.
//   - Relationship identity. The harness gives every loaded relationship an
//     ID: r{i} for both UNWIND populations and has-original for HAS_ORIGINAL.
//     The text Cypher leaves those relationships without an identity or id
//     property, so only PD_RELATION carries an id property here, and the
//     MEMBER_OF and HAS_ORIGINAL property maps stay as empty as the report's.
//   - Shape A collection construction. The text Cypher converts n5280 by
//     REMOVE n:PDGraphNode SET n:PDCollection from a node created with
//     archived:false; the harness creates it directly labelled PDCollection
//     with the same id and archived:false properties, which is the same stored
//     label and property state the statements read.
//   - Shape B relationship endpoints. The target MATCH in the text Cypher
//     omits a label restriction; it does not create unlabelled nodes. The
//     native generator retains the labels established during node creation.
//     The text targets are otherwise identical: shape A and B
//     both use 5281 + i % 166719, and the harness's 5281 + i%(172000-5281) is
//     the same span.
//   - Shape counts agree between the two forms: 351 MEMBER_OF + 289,649
//     PD_RELATION for shape A, and 5,280 MEMBER_OF + 284,720 PD_RELATION + 1
//     HAS_ORIGINAL for shape B.
//
// The reported history and configuration are preserved: in-memory native
// Badger behind the personal_documents namespace, populated by sequential
// CreateNode/CreateEdge calls, with every unique constraint installed through
// the public Execute path only after the whole population exists, and read
// through the public Execute path. The laboratory harness needed reflection to
// adapt a context argument that this checkout already has, so the statements
// and the population are reproduced directly through the current public API.
//
// No probe wraps the store. The reported statements must reach the engine's
// native namespace routing, count-store statistics and relationship-property
// equality lookup, and the shared adjacency meter embeds the base
// storage.Engine interface, so a wrapper around it would hide exactly those
// optional capabilities and change the observed cost. Shapes A and B qualify
// time and the complete result population, and the benchmarks assert every
// returned row on every measured read.
//
// These fixtures serve manual, optional benchmarks only; nothing here runs
// under the unit-test suite.

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// traversalLabSeedNamespace is the reported database namespace.
const traversalLabSeedNamespace = "personal_documents"

// traversalLabSeedNodeCount is the reported node population of both shapes.
const traversalLabSeedNodeCount = 172000

// traversalLabSeedDocumentCount is the reported PDDocument population of both shapes.
const traversalLabSeedDocumentCount = 5280

// traversalLabSeedIDCount is the reported $ids parameter population, n0 through n350.
const traversalLabSeedIDCount = 351

// traversalLabSeedCollectionID is the reported collection node of both shapes.
const traversalLabSeedCollectionID = "n5280"

// traversalLabSeedRelationTargetBase is the first PD_RELATION target, n5281.
const traversalLabSeedRelationTargetBase = 5281

// traversalLabSeedRelationTargetSpan is the reported PD_RELATION target span,
// 172,000 - 5,281 = 166,719.
const traversalLabSeedRelationTargetSpan = traversalLabSeedNodeCount - traversalLabSeedRelationTargetBase

const traversalLabSeedDocumentLabel = "PDDocument"

const traversalLabSeedCollectionLabel = "PDCollection"

const traversalLabSeedGraphNodeLabel = "PDGraphNode"

const traversalLabSeedOriginalLabel = "PDOriginal"

const traversalLabSeedVersionLabel = "PDVersion"

const traversalLabSeedMemberOfType = "MEMBER_OF"

const traversalLabSeedRelationType = "PD_RELATION"

const traversalLabSeedHasOriginalType = "HAS_ORIGINAL"

// traversalLabShapeACollectionLinks is shape A's reported MEMBER_OF population.
const traversalLabShapeACollectionLinks = 351

// traversalLabShapeAEdgeCount is shape A's reported relationship population.
const traversalLabShapeAEdgeCount = 290000

// traversalLabShapeARelationCount is shape A's reported PD_RELATION population.
const traversalLabShapeARelationCount = 289649

// traversalLabShapeAGraphNodeCount is shape A's reported PDGraphNode label
// population: 5,280 documents plus 166,719 plain graph nodes.
const traversalLabShapeAGraphNodeCount = 171999

// traversalLabShapeBCollectionLinks is shape B's reported MEMBER_OF population.
const traversalLabShapeBCollectionLinks = 5280

// traversalLabShapeBUnwindLinks is shape B's reported UNWIND relationship population,
// MEMBER_OF plus PD_RELATION; HAS_ORIGINAL is loaded after it.
const traversalLabShapeBUnwindLinks = 290000

// traversalLabShapeBEdgeCount is shape B's reported relationship population.
const traversalLabShapeBEdgeCount = 290001

// traversalLabShapeBRelationCount is shape B's reported PD_RELATION population.
const traversalLabShapeBRelationCount = 284720

// traversalLabShapeBGraphNodeCount is shape B's reported PDGraphNode label
// population: 5,280 documents plus 166,717 plain graph nodes, since n6000 and
// n6001 belong to PDOriginal and PDVersion.
const traversalLabShapeBGraphNodeCount = 171997

// traversalLabShapeBOriginalID is the reported PDOriginal node.
const traversalLabShapeBOriginalID = "n6000"

// traversalLabShapeBVersionID is the reported PDVersion node.
const traversalLabShapeBVersionID = "n6001"

// traversalLabShapeBOriginalHash is the reported PDOriginal content_hash value.
const traversalLabShapeBOriginalHash = "synthetic-hash"

// traversalLabShapeBOriginalEdgeID is the reported HAS_ORIGINAL relationship.
const traversalLabShapeBOriginalEdgeID = "has-original"

// traversalLabShapeBVersionDocumentID is the reported PDVersion document_id value.
const traversalLabShapeBVersionDocumentID = "n0"

// traversalLabShapeBVersionCreatedAt is the reported PDVersion created_at value.
const traversalLabShapeBVersionCreatedAt = int64(1)

// traversalLabShapeAInverse is the reported shape A failing statement: the inverse
// traversal that starts at the one collection and filters the documents by
// their id list.
const traversalLabShapeAInverse = `MATCH (elig_k:PDCollection)<-[:MEMBER_OF]-(elig_d:PDDocument) WHERE elig_d.id IN $ids RETURN elig_d.id, elig_k.archived`

// traversalLabShapeAForwardControl is the report's original forward control: the same
// eligibility read starting from the matched documents.
const traversalLabShapeAForwardControl = `UNWIND $ids AS id MATCH (elig_d:PDDocument {id: id}) OPTIONAL MATCH (elig_d)-[:MEMBER_OF]->(elig_k:PDCollection) WITH elig_d, collect(CASE WHEN elig_k IS NOT NULL THEN [elig_k.archived] END) AS elig_held WITH elig_d, [elig_f IN elig_held | elig_f[0]] AS elig_flags WITH elig_d, any(elig_f IN elig_flags WHERE elig_f = true) AND all(elig_f IN elig_flags WHERE coalesce(elig_f, false)) AS elig_closed RETURN elig_d.id, elig_closed`

// traversalLabShapeBCollectionDocuments is the reported COLLECTION_DOCUMENTS
// statement: two inline unique keys without a WHERE clause.
const traversalLabShapeBCollectionDocuments = `
UNWIND $ids AS each
WITH DISTINCT each
MATCH (d:PDDocument {id: each})-[:MEMBER_OF]->(:PDCollection {id: $collection_id})
RETURN d.id AS id
`

// traversalLabShapeBContentDocument is the reported CONTENT_DOCUMENT statement: the
// content hash selects the PDOriginal, its PDVersion names the document, and
// the document's membership selects the collection.
const traversalLabShapeBContentDocument = `
MATCH (o:PDOriginal {content_hash: $sha256})<-[:HAS_ORIGINAL]-(v:PDVersion)
MATCH (d:PDDocument {id: v.document_id})-[:MEMBER_OF]->(:PDCollection {id: $collection_id})
RETURN d.id AS id, v.created_at AS at
ORDER BY at, id
LIMIT 1
`

// traversalLabSeedIDs is the reported $ids parameter: n0 through n350.
func traversalLabSeedIDs() []interface{} {
	ids := make([]interface{}, 0, traversalLabSeedIDCount)
	for i := 0; i < traversalLabSeedIDCount; i++ {
		ids = append(ids, fmt.Sprintf("n%d", i))
	}
	return ids
}

// traversalLabSeedRows is a statement's complete expected result over $ids: one row
// per ID n0..n350 whose ID column is followed by tail. The reported statements
// carry no ORDER BY, so only the row population is fixed.
func traversalLabSeedRows(tail ...interface{}) map[string][]interface{} {
	rows := make(map[string][]interface{}, traversalLabSeedIDCount)
	for i := 0; i < traversalLabSeedIDCount; i++ {
		id := fmt.Sprintf("n%d", i)
		rows[id] = append([]interface{}{id}, tail...)
	}
	return rows
}

// traversalLabStatement is one reported statement with its exact parameters and the
// complete result population every read must return.
type traversalLabStatement struct {
	shape    string
	name     string
	query    string
	params   map[string]interface{}
	expected map[string][]interface{}
}

// traversalLabConstraint is one expected unique constraint: its declared name, its
// label or relationship type, and its single property.
type traversalLabConstraint struct {
	name     string
	target   string
	property string
}

// traversalLabShapeAInverseStatement is reported statement A with the reported
// parameters and its complete 351-row result: every document n0..n350 belongs
// to the one collection, which carries archived:false.
func traversalLabShapeAInverseStatement() traversalLabStatement {
	return traversalLabStatement{
		shape:    "a",
		name:     "inverse",
		query:    traversalLabShapeAInverse,
		params:   map[string]interface{}{"ids": traversalLabSeedIDs()},
		expected: traversalLabSeedRows(false),
	}
}

// traversalLabShapeAForwardStatement is the original forward control with the same
// 351-row result: every document is a member of the open collection, so the
// grouped closure flag is false for each of them.
func traversalLabShapeAForwardStatement() traversalLabStatement {
	return traversalLabStatement{
		shape:    "a",
		name:     "forward-control",
		query:    traversalLabShapeAForwardControl,
		params:   map[string]interface{}{"ids": traversalLabSeedIDs()},
		expected: traversalLabSeedRows(false),
	}
}

// traversalLabShapeBCollectionDocumentsStatement is COLLECTION_DOCUMENTS with the
// reported parameters and its complete 351-row result.
func traversalLabShapeBCollectionDocumentsStatement() traversalLabStatement {
	return traversalLabStatement{
		shape:    "b",
		name:     "collection-documents",
		query:    traversalLabShapeBCollectionDocuments,
		params:   map[string]interface{}{"ids": traversalLabSeedIDs(), "collection_id": traversalLabSeedCollectionID},
		expected: traversalLabSeedRows(),
	}
}

// traversalLabShapeBContentDocumentStatement is CONTENT_DOCUMENT with the reported
// parameters and its one row: the version's document at its creation time.
func traversalLabShapeBContentDocumentStatement() traversalLabStatement {
	return traversalLabStatement{
		shape:    "b",
		name:     "content-document",
		query:    traversalLabShapeBContentDocument,
		params:   map[string]interface{}{"collection_id": traversalLabSeedCollectionID, "sha256": traversalLabShapeBOriginalHash},
		expected: map[string][]interface{}{traversalLabShapeBVersionDocumentID: {traversalLabShapeBVersionDocumentID, traversalLabShapeBVersionCreatedAt}},
	}
}

// traversalLabSeedFixture is one reported population read through the public Execute
// path. The store is the real namespaced engine: no probe wraps it, so every
// native optional capability stays visible to the executor.
type traversalLabSeedFixture struct {
	store *storage.NamespacedEngine
}

// newTraversalLabSeedFixture creates the empty in-memory native Badger engine behind
// the reported namespace.
func newTraversalLabSeedFixture(tb testing.TB) *traversalLabSeedFixture {
	tb.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](tb, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = base.Close() })
	return &traversalLabSeedFixture{store: storage.NewNamespacedEngine(base, traversalLabSeedNamespace)}
}

// executor returns a fresh executor with the result cache disabled, so a
// measured read cannot be answered from a cached result and each parser gets
// its own plan cache.
func (f *traversalLabSeedFixture) executor() *StorageExecutor {
	return NewStorageExecutor(f.store, QueryCachePolicy{MaxEntries: 0})
}

// createNode stores one reported node through the native sequential write.
func (f *traversalLabSeedFixture) createNode(tb testing.TB, id string, labels []string, properties map[string]interface{}) {
	tb.Helper()
	node := &storage.Node{ID: storage.NodeID(id), Labels: labels, Properties: properties}
	_, err := f.store.CreateNode(context.Background(), node)
	require.NoError(tb, err, "create node %s", id)
}

// createEdge stores one reported relationship through the native sequential
// write.
func (f *traversalLabSeedFixture) createEdge(tb testing.TB, id, start, end, kind string, properties map[string]interface{}) {
	tb.Helper()
	edge := &storage.Edge{ID: storage.EdgeID(id), StartNode: storage.NodeID(start), EndNode: storage.NodeID(end), Type: kind, Properties: properties}
	require.NoError(tb, f.store.CreateEdge(context.Background(), edge), "create relationship %s", id)
}

// createConstraints declares the reported unique constraints through the same
// public Execute path the laboratory used, after the whole population exists.
func (f *traversalLabSeedFixture) createConstraints(tb testing.TB, statements ...string) {
	tb.Helper()
	writer := f.executor()
	for _, statement := range statements {
		_, err := writer.Execute(context.Background(), statement, nil)
		require.NoError(tb, err, statement)
	}
}

// requirePopulation asserts the exact population the reported statements are
// measured against: the node and relationship totals, every label and
// relationship-type population, the collection's MEMBER_OF in-degree and the
// declared constraint set. It runs once after the population is built, outside
// every measured region.
func (f *traversalLabSeedFixture) requirePopulation(tb testing.TB, edges int64, edgesByType, labels map[string]int64, collectionInDegree int, constraints []traversalLabConstraint) {
	tb.Helper()
	ctx := context.Background()
	nodes, err := f.store.NodeCount(ctx)
	require.NoError(tb, err)
	require.Equal(tb, int64(traversalLabSeedNodeCount), nodes, "fixture node population")
	relationships, err := f.store.EdgeCount(ctx)
	require.NoError(tb, err)
	require.Equal(tb, edges, relationships, "fixture relationship population")
	for label, count := range labels {
		got, err := f.store.NodeCountByLabel(ctx, label)
		require.NoError(tb, err)
		require.Equal(tb, count, got, "label population %s", label)
	}
	for kind, count := range edgesByType {
		got, err := f.store.EdgeCountByType(ctx, kind)
		require.NoError(tb, err)
		require.Equal(tb, count, got, "relationship population %s", kind)
	}
	inDegree, err := f.store.GetInDegree(ctx, traversalLabSeedCollectionID)
	require.NoError(tb, err)
	require.Equal(tb, collectionInDegree, inDegree, "collection MEMBER_OF in-degree")
	f.requireConstraints(tb, constraints)
}

// requireConstraints asserts the exact declared constraint set: every reported
// constraint is present with its target and property, and nothing else is
// declared in the fixture namespace.
func (f *traversalLabSeedFixture) requireConstraints(tb testing.TB, constraints []traversalLabConstraint) {
	tb.Helper()
	declared := f.store.GetSchema().GetAllConstraints()
	require.Len(tb, declared, len(constraints), "constraint population")
	byName := make(map[string]storage.Constraint, len(declared))
	for _, constraint := range declared {
		byName[constraint.Name] = constraint
	}
	for _, expected := range constraints {
		constraint, ok := byName[expected.name]
		require.True(tb, ok, "constraint %s", expected.name)
		require.Equal(tb, expected.target, constraint.Label, "constraint %s target", expected.name)
		require.Equal(tb, []string{expected.property}, constraint.Properties, "constraint %s properties", expected.name)
	}
}

// requireContentDocumentEndpoints asserts the two nodes and the relationship
// CONTENT_DOCUMENT reads: the PDVersion's document_id and created_at, the
// PDOriginal's content_hash, and the HAS_ORIGINAL relationship between them.
func (f *traversalLabSeedFixture) requireContentDocumentEndpoints(tb testing.TB) {
	tb.Helper()
	ctx := context.Background()
	original, err := f.store.GetNode(ctx, traversalLabShapeBOriginalID)
	require.NoError(tb, err)
	require.Equal(tb, traversalLabShapeBOriginalHash, original.Properties["content_hash"], "PDOriginal content_hash")
	version, err := f.store.GetNode(ctx, traversalLabShapeBVersionID)
	require.NoError(tb, err)
	require.Equal(tb, traversalLabShapeBVersionDocumentID, version.Properties["document_id"], "PDVersion document_id")
	require.Equal(tb, traversalLabShapeBVersionCreatedAt, version.Properties["created_at"], "PDVersion created_at")
	edge, err := f.store.GetEdgeBetween(ctx, traversalLabShapeBVersionID, traversalLabShapeBOriginalID, traversalLabSeedHasOriginalType)
	require.NoError(tb, err)
	require.NotNil(tb, edge, "PDVersion -[:HAS_ORIGINAL]-> PDOriginal")
}

// newTraversalLabShapeAFixture builds the reported shape A population and asserts its
// exact size before any statement runs.
func newTraversalLabShapeAFixture(tb testing.TB) *traversalLabSeedFixture {
	tb.Helper()
	fixture := newTraversalLabSeedFixture(tb)
	for i := 0; i < traversalLabSeedNodeCount; i++ {
		id := fmt.Sprintf("n%d", i)
		labels := []string{traversalLabSeedGraphNodeLabel}
		switch {
		case i < traversalLabSeedDocumentCount:
			labels = append(labels, traversalLabSeedDocumentLabel)
		case i == traversalLabSeedDocumentCount:
			labels = []string{traversalLabSeedCollectionLabel}
		}
		fixture.createNode(tb, id, labels, map[string]interface{}{"id": id, "archived": false})
	}
	for i := 0; i < traversalLabShapeAEdgeCount; i++ {
		edgeID := fmt.Sprintf("r%d", i)
		start := fmt.Sprintf("n%d", i%traversalLabSeedIDCount)
		end := traversalLabSeedCollectionID
		kind := traversalLabSeedMemberOfType
		properties := map[string]interface{}{}
		if i >= traversalLabShapeACollectionLinks {
			end = fmt.Sprintf("n%d", traversalLabSeedRelationTargetBase+i%traversalLabSeedRelationTargetSpan)
			kind = traversalLabSeedRelationType
			properties["id"] = edgeID
		}
		fixture.createEdge(tb, edgeID, start, end, kind, properties)
	}
	fixture.createConstraints(tb,
		"CREATE CONSTRAINT pd_document_id IF NOT EXISTS FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT pd_graph_node_id IF NOT EXISTS FOR (d:PDGraphNode) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE",
	)
	fixture.requirePopulation(tb, traversalLabShapeAEdgeCount,
		map[string]int64{traversalLabSeedMemberOfType: traversalLabShapeACollectionLinks, traversalLabSeedRelationType: traversalLabShapeARelationCount},
		map[string]int64{traversalLabSeedDocumentLabel: traversalLabSeedDocumentCount, traversalLabSeedCollectionLabel: 1, traversalLabSeedGraphNodeLabel: traversalLabShapeAGraphNodeCount},
		traversalLabShapeACollectionLinks,
		[]traversalLabConstraint{
			{name: "pd_document_id", target: traversalLabSeedDocumentLabel, property: "id"},
			{name: "pd_graph_node_id", target: traversalLabSeedGraphNodeLabel, property: "id"},
			{name: "pd_relation_id", target: traversalLabSeedRelationType, property: "id"},
		},
	)
	return fixture
}

// newTraversalLabShapeBFixture builds the reported shape B population and asserts its
// exact size, plus the CONTENT_DOCUMENT endpoints, before any statement runs.
func newTraversalLabShapeBFixture(tb testing.TB) *traversalLabSeedFixture {
	tb.Helper()
	fixture := newTraversalLabSeedFixture(tb)
	for i := 0; i < traversalLabSeedNodeCount; i++ {
		id := fmt.Sprintf("n%d", i)
		labels := []string{traversalLabSeedGraphNodeLabel}
		properties := map[string]interface{}{"id": id}
		switch {
		case i < traversalLabSeedDocumentCount:
			labels = append(labels, traversalLabSeedDocumentLabel)
		case id == traversalLabSeedCollectionID:
			labels = []string{traversalLabSeedCollectionLabel}
		case id == traversalLabShapeBOriginalID:
			labels = []string{traversalLabSeedOriginalLabel}
			properties["content_hash"] = traversalLabShapeBOriginalHash
		case id == traversalLabShapeBVersionID:
			labels = []string{traversalLabSeedVersionLabel}
			properties["document_id"] = traversalLabShapeBVersionDocumentID
			properties["created_at"] = traversalLabShapeBVersionCreatedAt
		}
		fixture.createNode(tb, id, labels, properties)
	}
	for i := 0; i < traversalLabShapeBUnwindLinks; i++ {
		edgeID := fmt.Sprintf("r%d", i)
		start := fmt.Sprintf("n%d", i)
		end := traversalLabSeedCollectionID
		kind := traversalLabSeedMemberOfType
		properties := map[string]interface{}{}
		if i >= traversalLabShapeBCollectionLinks {
			start = fmt.Sprintf("n%d", i%traversalLabSeedIDCount)
			end = fmt.Sprintf("n%d", traversalLabSeedRelationTargetBase+i%traversalLabSeedRelationTargetSpan)
			kind = traversalLabSeedRelationType
			properties["id"] = edgeID
		}
		fixture.createEdge(tb, edgeID, start, end, kind, properties)
	}
	fixture.createEdge(tb, traversalLabShapeBOriginalEdgeID, traversalLabShapeBVersionID, traversalLabShapeBOriginalID, traversalLabSeedHasOriginalType, nil)
	fixture.createConstraints(tb,
		"CREATE CONSTRAINT ck_PDDocument IF NOT EXISTS FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT ck_PDCollection IF NOT EXISTS FOR (d:PDCollection) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT ck_PDGraphNode IF NOT EXISTS FOR (d:PDGraphNode) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT ck_PDVersion IF NOT EXISTS FOR (d:PDVersion) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT ck_hash IF NOT EXISTS FOR (d:PDOriginal) REQUIRE d.content_hash IS UNIQUE",
		"CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE",
	)
	fixture.requirePopulation(tb, traversalLabShapeBEdgeCount,
		map[string]int64{traversalLabSeedMemberOfType: traversalLabShapeBCollectionLinks, traversalLabSeedRelationType: traversalLabShapeBRelationCount, traversalLabSeedHasOriginalType: 1},
		map[string]int64{traversalLabSeedDocumentLabel: traversalLabSeedDocumentCount, traversalLabSeedCollectionLabel: 1, traversalLabSeedGraphNodeLabel: traversalLabShapeBGraphNodeCount, traversalLabSeedOriginalLabel: 1, traversalLabSeedVersionLabel: 1},
		traversalLabShapeBCollectionLinks,
		[]traversalLabConstraint{
			{name: "ck_PDDocument", target: traversalLabSeedDocumentLabel, property: "id"},
			{name: "ck_PDCollection", target: traversalLabSeedCollectionLabel, property: "id"},
			{name: "ck_PDGraphNode", target: traversalLabSeedGraphNodeLabel, property: "id"},
			{name: "ck_PDVersion", target: traversalLabSeedVersionLabel, property: "id"},
			{name: "ck_hash", target: traversalLabSeedOriginalLabel, property: "content_hash"},
			{name: "pd_relation_id", target: traversalLabSeedRelationType, property: "id"},
		},
	)
	fixture.requireContentDocumentEndpoints(tb)
	return fixture
}
