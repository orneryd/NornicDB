package storage

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// The generated qualification fixtures cover the relationship property equality
// index's representative shapes at its owner
// (badger_relationship_unique_index.go), whose public Cypher fixture in
// pkg/cypher/relationship_property_equality_* declares one small population:
//
//   - the unrelated relationships of one (namespace, relationship type): the
//     small, 4096 and 32768 populations, so cold preparation is qualified on
//     stored data of each representative size;
//   - the distinct definitions declared, each its own (namespace, relationship
//     type, property set): 1, 4, 16 and 17, so the 16-entry retention bound is
//     exercised from below the cap up to the declaration that retires an entry;
//   - the indexed properties of one definition: 1, 2 and 4;
//   - namespace and type isolation: two namespaces and two types carrying the
//     same indexed values, so an answer can only be the caller's own.
//
// These are a bounded case table rather than a cross product: every dimension has
// its representative cases, and the declaration ladder is not repeated at every
// population.
const (
	relationshipEqualityQualificationNamespace     = "pq"
	relationshipEqualityQualificationPeerNamespace = "pq2"
	// Definitions after the primary one keep a small population: they exercise
	// the retained-entry bound and the isolation between definitions, not the
	// cold-preparation population.
	relationshipEqualityQualificationSecondaryEdges = 4
	// Each isolation definition holds the mid representative population, so a
	// leaked posting would be a whole population rather than one relationship.
	relationshipEqualityQualificationIsolationEdges = 4096
	// The indexed values one target relationship carries — the public Cypher
	// fixture's release value — and a value no definition holds.
	relationshipEqualityQualificationTargetValue  = "r289999"
	relationshipEqualityQualificationMissingValue = "qualification-absent"
	relationshipEqualityQualificationStartNode    = "n73"
	relationshipEqualityQualificationEndNode      = "n128561"
)

// relationshipEqualityQualificationCase is one generated qualification case: the
// unrelated relationships of the primary definition's type, the number of
// distinct definitions declared in order, and the indexed property count of
// every definition.
type relationshipEqualityQualificationCase struct {
	edges int
	defs  int
	arity int
}

func (c relationshipEqualityQualificationCase) name() string {
	return fmt.Sprintf("edges-%d/defs-%d/arity-%d", c.edges, c.defs, c.arity)
}

// relationshipEqualityQualificationCases is the bounded case table: the
// declaration ladder at the small population, then each representative
// population and arity once.
var relationshipEqualityQualificationCases = []relationshipEqualityQualificationCase{
	{edges: 128, defs: 1, arity: 1},
	{edges: 128, defs: 4, arity: 1},
	{edges: 128, defs: relationshipUniqueIndexLimit, arity: 1},
	{edges: 128, defs: relationshipUniqueIndexLimit + 1, arity: 1},
	{edges: 4096, defs: 1, arity: 2},
	{edges: 4096, defs: 1, arity: 4},
	{edges: 32768, defs: 1, arity: 1},
}

// relationshipEqualityQualificationPopulation is one (namespace, relationship
// type) the fixture fills: the endpoint nodes its relationships connect and the
// unrelated relationships it holds beside the one its indexed values select.
type relationshipEqualityQualificationPopulation struct {
	namespace        string
	relationshipType string
	startNode        NodeID
	endNode          NodeID
	unrelated        int
}

// relationshipEqualityQualificationDefinition is one declared definition: the
// (namespace, relationship type, property set) the owner retains an index under,
// the constraint the declaration used, the indexed values its target relationship
// carries, and how many relationships its type and namespace store.
type relationshipEqualityQualificationDefinition struct {
	namespace        string
	relationshipType string
	properties       []string
	values           []interface{}
	startNode        NodeID
	endNode          NodeID
	relationships    int
	constraint       Constraint
}

// indexKey is the key the owner retains this definition's index under.
func (d relationshipEqualityQualificationDefinition) indexKey() relationshipUniqueIndexKey {
	return relationshipUniqueIndexKey{namespace: d.namespace, relType: d.relationshipType, properties: relationshipPropertyKey(d.properties)}
}

// targetID is the ID the namespace's reader sees for the definition's indexed
// relationship; the engine files it under the namespace prefix.
func (d relationshipEqualityQualificationDefinition) targetID() EdgeID {
	return EdgeID("qualification-" + d.relationshipType + "-target")
}

// relationshipEqualityQualificationProperties returns the indexed property names
// of one definition for a representative arity.
func relationshipEqualityQualificationProperties(arity int) []string {
	switch arity {
	case 1:
		return []string{"id"}
	case 2:
		return []string{"id", "kind"}
	case 4:
		return []string{"id", "kind", "owner", "stage"}
	}
	panic(fmt.Sprintf("storage: no qualification properties for arity %d", arity))
}

// relationshipEqualityQualificationValues returns the indexed values one
// relationship carries: the first property holds tag and each further property
// extends it, so every generated relationship's full key is unique inside its
// definition.
func relationshipEqualityQualificationValues(arity int, tag string) []interface{} {
	values := make([]interface{}, arity)
	for i := range values {
		if i == 0 {
			values[i] = tag
			continue
		}
		values[i] = fmt.Sprintf("%s-%d", tag, i)
	}
	return values
}

// relationshipEqualityQualificationFixture is one generated engine population
// with the definitions the case declares against it.
type relationshipEqualityQualificationFixture struct {
	engine *BadgerEngine
	defs   []relationshipEqualityQualificationDefinition
	stores map[string]*NamespacedEngine
}

// newRelationshipEqualityQualificationFixture fills one in-memory engine with the
// populations' relationships and generates the definition each population's
// declaration uses. Relationships and nodes are filed under the namespace prefix
// the namespaced store would give them, so the owner's stored reads and its
// namespace filter see the stored data production writes. Each namespace commits
// through its own transaction, because a transaction pins the namespace of its
// first write.
func newRelationshipEqualityQualificationFixture(t testing.TB, arity int, populations []relationshipEqualityQualificationPopulation) *relationshipEqualityQualificationFixture {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	fixture := &relationshipEqualityQualificationFixture{engine: engine, stores: make(map[string]*NamespacedEngine)}
	ctx := context.Background()
	namespaces := make([]string, 0, len(populations))
	members := make(map[string][]int, len(populations))
	for i, population := range populations {
		if _, seen := members[population.namespace]; !seen {
			namespaces = append(namespaces, population.namespace)
		}
		members[population.namespace] = append(members[population.namespace], i)
	}
	definitions := make([]relationshipEqualityQualificationDefinition, len(populations))
	nodes := make(map[NodeID]bool)
	for _, namespace := range namespaces {
		tx, err := engine.BeginTransaction(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		for _, at := range members[namespace] {
			population := populations[at]
			for _, node := range []NodeID{population.startNode, population.endNode} {
				stored := NodeID(population.namespace + ":" + string(node))
				if nodes[stored] {
					continue
				}
				_, err := tx.CreateNode(ctx, &Node{ID: stored})
				require.NoError(t, err)
				nodes[stored] = true
			}
			properties := relationshipEqualityQualificationProperties(arity)
			definition := relationshipEqualityQualificationDefinition{
				namespace:        population.namespace,
				relationshipType: population.relationshipType,
				properties:       properties,
				values:           relationshipEqualityQualificationValues(arity, relationshipEqualityQualificationTargetValue),
				startNode:        population.startNode,
				endNode:          population.endNode,
				relationships:    population.unrelated + 1,
				constraint: Constraint{
					Name:       "qualification_" + population.relationshipType,
					Type:       ConstraintUnique,
					EntityType: ConstraintEntityRelationship,
					Label:      population.relationshipType,
					Properties: append([]string(nil), properties...),
				},
			}
			require.NoError(t, commitRelationshipEqualityQualificationPopulation(ctx, tx, definition, population.unrelated))
			definitions[at] = definition
		}
		require.NoError(t, tx.Commit())
	}
	fixture.defs = definitions
	return fixture
}

// commitRelationshipEqualityQualificationPopulation files one population's
// relationships: the unrelated ones of its type and namespace, then the one its
// indexed values select.
func commitRelationshipEqualityQualificationPopulation(ctx context.Context, tx *BadgerTransaction, definition relationshipEqualityQualificationDefinition, unrelated int) error {
	create := func(id, tag string) error {
		values := relationshipEqualityQualificationValues(len(definition.properties), tag)
		properties := make(map[string]interface{}, len(values))
		for i, property := range definition.properties {
			properties[property] = values[i]
		}
		return tx.CreateEdge(ctx, &Edge{
			ID:         EdgeID(definition.namespace + ":" + id),
			StartNode:  NodeID(definition.namespace + ":" + string(definition.startNode)),
			EndNode:    NodeID(definition.namespace + ":" + string(definition.endNode)),
			Type:       definition.relationshipType,
			Properties: properties,
		})
	}
	for i := 0; i < unrelated; i++ {
		if err := create(fmt.Sprintf("qualification-%s-unrelated%d", definition.relationshipType, i), fmt.Sprintf("x%d", i)); err != nil {
			return err
		}
	}
	return create(string(definition.targetID()), relationshipEqualityQualificationTargetValue)
}

// store returns the namespace boundary a request for namespace reads through.
func (f *relationshipEqualityQualificationFixture) store(namespace string) *NamespacedEngine {
	if store := f.stores[namespace]; store != nil {
		return store
	}
	store := NewNamespacedEngine(f.engine, namespace)
	f.stores[namespace] = store
	return store
}

// declarationEngine is the engine chain the constraint declaration prepares the
// index through: the database namespace behind the durability layer, as the
// public Cypher declaration passes it.
func (f *relationshipEqualityQualificationFixture) declarationEngine(namespace string) Engine {
	return &WALEngine{engine: f.store(namespace)}
}

// partitionRetained splits the fixture's definitions by whether the engine
// retains their index under the current bound. Which entry a publication retires
// is not fixed — it retires any entry to stay bounded — so a case reads the
// retained set instead of predicting it.
func (f *relationshipEqualityQualificationFixture) partitionRetained() (retained, retired []relationshipEqualityQualificationDefinition) {
	for _, definition := range f.defs {
		if f.engine.relationshipUniqueIndexes.entries[definition.indexKey()] == nil {
			retired = append(retired, definition)
			continue
		}
		retained = append(retained, definition)
	}
	return retained, retired
}

// requireRetainedIndex asserts the definition's index is retained and holds
// exactly the relationships its type and namespace store — each once — so what
// the bound retains is that stored population rather than accumulated,
// duplicated or partial history.
func (f *relationshipEqualityQualificationFixture) requireRetainedIndex(t testing.TB, definition relationshipEqualityQualificationDefinition) {
	t.Helper()
	retained := f.engine.relationshipUniqueIndexes.entries[definition.indexKey()]
	require.NotNil(t, retained, "definition %q is retained", definition.relationshipType)
	require.Len(t, retained.keys, definition.relationships, "the retained index holds every stored relationship of its definition once")
}

// requireAnswer asserts the definition's declared index answers exactly the one
// relationship its indexed values select, as the namespace's public reader sees
// it. A second row could only be another definition's relationship — another
// namespace's or another type's — reaching this answer.
func (f *relationshipEqualityQualificationFixture) requireAnswer(t testing.TB, definition relationshipEqualityQualificationDefinition) {
	t.Helper()
	edges, handled, err := f.store(definition.namespace).LookupEdgesByPropertyEquality(context.Background(), definition.relationshipType, definition.properties, definition.values)
	require.NoError(t, err)
	require.True(t, handled, "the declared definition answers %q from its index", definition.relationshipType)
	require.Len(t, edges, 1)
	require.Equal(t, definition.targetID(), edges[0].ID)
	require.Equal(t, definition.startNode, edges[0].StartNode)
	require.Equal(t, definition.endNode, edges[0].EndNode)
	require.Equal(t, definition.relationshipType, edges[0].Type)
	for i, property := range definition.properties {
		require.Equal(t, definition.values[i], edges[0].Properties[property])
	}
}

// requireNoAnswer asserts the definition's index answers a value it does not
// hold with no relationship at all, rather than with another definition's.
func (f *relationshipEqualityQualificationFixture) requireNoAnswer(t testing.TB, definition relationshipEqualityQualificationDefinition) {
	t.Helper()
	values := relationshipEqualityQualificationValues(len(definition.properties), relationshipEqualityQualificationMissingValue)
	edges, handled, err := f.store(definition.namespace).LookupEdgesByPropertyEquality(context.Background(), definition.relationshipType, definition.properties, values)
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, edges)
}

// relationshipEqualityQualificationCasePopulations generates the case's
// populations in declaration order: the primary definition carries the case's
// unrelated relationships, and every further definition is its own relationship
// type with a small population, so the declaration ladder stays bounded.
func relationshipEqualityQualificationCasePopulations(testCase relationshipEqualityQualificationCase) []relationshipEqualityQualificationPopulation {
	populations := make([]relationshipEqualityQualificationPopulation, 0, testCase.defs)
	for definition := 0; definition < testCase.defs; definition++ {
		population := relationshipEqualityQualificationPopulation{
			namespace:        relationshipEqualityQualificationNamespace,
			relationshipType: relationshipEqualityQualificationType(definition),
			startNode:        relationshipEqualityQualificationStartNode,
			endNode:          relationshipEqualityQualificationEndNode,
			unrelated:        relationshipEqualityQualificationSecondaryEdges,
		}
		if definition == 0 {
			population.unrelated = testCase.edges
		}
		populations = append(populations, population)
	}
	return populations
}

// relationshipEqualityQualificationType names each definition's relationship
// type: the primary definition keeps the public Cypher fixture's type, and every
// further definition is its own type, so each is a distinct retained key.
func relationshipEqualityQualificationType(definition int) string {
	if definition == 0 {
		return "PD_RELATION"
	}
	return fmt.Sprintf("PD_RELATION_%d", definition)
}

// TestRelationshipPropertyEqualityQualificationColdPreparation covers the
// owner's retained set for each generated case: every first declaration builds
// its own definition's index exactly once from the stored relationships, each
// retained index holds exactly its definition's stored relationships, the
// retained set never exceeds the bound, the definitions the bound retired still
// answer deterministically by rebuilding, and every definition answers only its
// own (namespace, type)'s relationship.
func TestRelationshipPropertyEqualityQualificationColdPreparation(t *testing.T) {
	for _, testCase := range relationshipEqualityQualificationCases {
		t.Run(testCase.name(), func(t *testing.T) {
			require.LessOrEqual(t, testCase.defs, relationshipUniqueIndexLimit+1,
				"the case table retires at most one definition, so one rebuild answers the retired read")
			fixture := newRelationshipEqualityQualificationFixture(t, testCase.arity, relationshipEqualityQualificationCasePopulations(testCase))
			engine := fixture.engine
			ctx := context.Background()
			for _, definition := range fixture.defs {
				require.NoError(t, engine.GetSchemaForNamespace(definition.namespace).AddConstraint(definition.constraint, false))
			}

			// Cold declaration: no retained definition exists yet, so each one
			// builds from the stored relationships of its type.
			builds := 0
			restore := setUniqueConstraintScanHook(func() { builds++ })
			t.Cleanup(restore)
			for _, definition := range fixture.defs {
				require.NoError(t, PrepareRelationshipPropertyEqualityIndexForEngine(ctx, fixture.declarationEngine(definition.namespace), definition.constraint))
			}
			restore()
			require.Equal(t, len(fixture.defs), builds, "each first declaration prepares its own definition exactly once")
			retainedCount := min(len(fixture.defs), relationshipUniqueIndexLimit)
			require.Len(t, engine.relationshipUniqueIndexes.entries, retainedCount)
			require.True(t, engine.relationshipUniqueIndexes.active.Load())

			warm, retired := fixture.partitionRetained()
			require.Len(t, retired, len(fixture.defs)-retainedCount, "the bound retires the declarations beyond it")

			// A retained definition answers from its postings, including the
			// value it does not hold.
			builds = 0
			restore = setUniqueConstraintScanHook(func() { builds++ })
			t.Cleanup(restore)
			for _, definition := range warm {
				fixture.requireAnswer(t, definition)
				fixture.requireRetainedIndex(t, definition)
			}
			fixture.requireNoAnswer(t, warm[0])
			restore()
			require.Zero(t, builds, "a retained definition answers from its postings")

			// A retired definition still answers: its next read rebuilds it from
			// the stored relationships and republishes under the same bound.
			builds = 0
			restore = setUniqueConstraintScanHook(func() { builds++ })
			t.Cleanup(restore)
			for _, definition := range retired {
				fixture.requireAnswer(t, definition)
				fixture.requireRetainedIndex(t, definition)
			}
			restore()
			require.Equal(t, len(retired), builds, "a retired definition rebuilds when its next read needs it")
			require.Len(t, engine.relationshipUniqueIndexes.entries, retainedCount, "the rebuild republishes under the same bound")

			// Re-declaring what the bound retained prepares nothing.
			warm, _ = fixture.partitionRetained()
			builds = 0
			restore = setUniqueConstraintScanHook(func() { builds++ })
			t.Cleanup(restore)
			for _, definition := range warm {
				require.NoError(t, PrepareRelationshipPropertyEqualityIndexForEngine(ctx, fixture.declarationEngine(definition.namespace), definition.constraint))
			}
			restore()
			require.Zero(t, builds, "a retained definition is not prepared again")
			require.LessOrEqual(t, len(engine.relationshipUniqueIndexes.entries), relationshipUniqueIndexLimit)
		})
	}
}

// TestRelationshipPropertyEqualityQualificationIsolatesNamespaceAndType covers
// the isolation the retained key encodes: the same indexed values on the same
// relationship type in two namespaces, and on two types in one namespace. Each
// definition answers only its own relationship, and the retained set keeps the
// three definitions and their populations apart.
func TestRelationshipPropertyEqualityQualificationIsolatesNamespaceAndType(t *testing.T) {
	fixture := newRelationshipEqualityQualificationFixture(t, 1, []relationshipEqualityQualificationPopulation{
		{
			namespace:        relationshipEqualityQualificationNamespace,
			relationshipType: "ISO_RELATION",
			startNode:        "n73",
			endNode:          "n128561",
			unrelated:        relationshipEqualityQualificationIsolationEdges,
		},
		{
			namespace:        relationshipEqualityQualificationNamespace,
			relationshipType: "ISO_OTHER",
			startNode:        "n75",
			endNode:          "n128563",
			unrelated:        relationshipEqualityQualificationIsolationEdges,
		},
		{
			namespace:        relationshipEqualityQualificationPeerNamespace,
			relationshipType: "ISO_RELATION",
			startNode:        "n74",
			endNode:          "n128562",
			unrelated:        relationshipEqualityQualificationIsolationEdges,
		},
	})
	engine := fixture.engine
	ctx := context.Background()
	for _, definition := range fixture.defs {
		require.NoError(t, engine.GetSchemaForNamespace(definition.namespace).AddConstraint(definition.constraint, false))
		require.NoError(t, PrepareRelationshipPropertyEqualityIndexForEngine(ctx, fixture.declarationEngine(definition.namespace), definition.constraint))
	}
	require.Len(t, engine.relationshipUniqueIndexes.entries, len(fixture.defs),
		"each (namespace, relationship type, property set) retains its own index")

	// The peer namespace's relationship carries this namespace's user-facing ID
	// and indexed values; only its endpoints tell the two answers apart.
	require.Equal(t, fixture.defs[0].targetID(), fixture.defs[2].targetID())
	require.Equal(t, fixture.defs[0].values, fixture.defs[2].values)
	require.NotEqual(t, fixture.defs[0].startNode, fixture.defs[2].startNode)

	builds := 0
	restore := setUniqueConstraintScanHook(func() { builds++ })
	t.Cleanup(restore)
	for _, definition := range fixture.defs {
		fixture.requireAnswer(t, definition)
		fixture.requireRetainedIndex(t, definition)
		fixture.requireNoAnswer(t, definition)
	}
	restore()
	require.Zero(t, builds, "isolated definitions answer from their own postings")
}
