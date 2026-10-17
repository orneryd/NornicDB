package storage

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// F-PUBLISH: the structures derived from committed entities are a
// commit-order prefix of the committed operations, and a write returns only
// once that prefix covers it (badger_publication_stage.go). Each case holds
// the publication of an older write of an entity inside the ordered stage,
// starts a newer write of the same entity and requires that the newer write
// does not return while the older publication is held. Then it releases the
// older publication and checks every consumer through its public reads, and
// a third writer that takes a value the entity no longer holds.
//
// The consumers and operations come from the extracted publication boundary
// (TestPublicationStageBoundary): a consumer or an operation that boundary
// lists and no case here exercises fails that test.

// The node consumers this matrix checks.
var publicationMatrixNodeConsumers = []string{
	"node cache", "query-node cache", "property index", "composite index",
	"unique-value registry", "label index", "node counts",
}

// The edge consumers this matrix checks.
var publicationMatrixEdgeConsumers = []string{
	"edge cache", "adjacency", "edge-type index", "relationship-uniqueness index", "edge counts",
}

type matrixNodeWrite struct {
	name  string
	apply func(b *BadgerEngine, explicit bool) error
}

const matrixNodeID = NodeID("db:x")

func matrixNode(labels []string, value string) *Node {
	return &Node{ID: matrixNodeID, Labels: labels, Properties: map[string]any{"p": value, "q": "w", "u": value}}
}

func matrixUpdate(labels []string, values ...string) matrixNodeWrite {
	return matrixNodeWrite{name: fmt.Sprintf("update %v %v", labels, values), apply: func(b *BadgerEngine, explicit bool) error {
		return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error {
			for _, value := range values {
				if err := tx.UpdateNode(matrixNode(labels, value)); err != nil {
					return err
				}
			}
			return nil
		}, func() error { return b.UpdateNode(matrixNode(labels, values[len(values)-1])) })
	}}
}

func matrixCreate(labels []string, value string) matrixNodeWrite {
	return matrixNodeWrite{name: "create", apply: func(b *BadgerEngine, explicit bool) error {
		return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(matrixNode(labels, value)); return err },
			func() error { _, err := b.CreateNode(matrixNode(labels, value)); return err })
	}}
}

var matrixDelete = matrixNodeWrite{name: "delete", apply: func(b *BadgerEngine, explicit bool) error {
	return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.DeleteNode(matrixNodeID) }, func() error { return b.DeleteNode(matrixNodeID) })
}}

// matrixNodeState is x's expected committed state: nil when x is gone.
type matrixNodeCase struct {
	name         string
	operations   []string // the boundary operations the case exercises
	initial      *Node
	older, newer matrixNodeWrite
	want         *Node
	replaced     []string // L values x no longer holds
	explicitOnly bool     // the older write needs a transaction
}

func publicationMatrixNodeCases() []matrixNodeCase {
	l, other := []string{"L"}, []string{"Other"}
	return []matrixNodeCase{
		{name: "update overtaken by an update", operations: []string{"update"}, initial: matrixNode(l, "A"), older: matrixUpdate(l, "B"), newer: matrixUpdate(l, "C"), want: matrixNode(l, "C"), replaced: []string{"A", "B"}},
		{name: "update then restore", operations: []string{"update", "restore"}, initial: matrixNode(l, "A"), older: matrixUpdate(l, "B"), newer: matrixUpdate(l, "A"), want: matrixNode(l, "A"), replaced: []string{"B"}},
		{name: "update then delete", operations: []string{"update", "delete"}, initial: matrixNode(l, "A"), older: matrixUpdate(l, "B"), newer: matrixDelete, replaced: []string{"A", "B"}},
		{name: "update then label removal", operations: []string{"update", "label remove"}, initial: matrixNode(l, "A"), older: matrixUpdate(l, "B"), newer: matrixUpdate(other, "B"), want: matrixNode(other, "B"), replaced: []string{"A", "B"}},
		{name: "label removal then re-add", operations: []string{"label remove", "label add"}, initial: matrixNode(l, "A"), older: matrixUpdate(other, "A"), newer: matrixUpdate(l, "A"), want: matrixNode(l, "A")},
		{name: "update then label add", operations: []string{"update", "label add"}, initial: matrixNode(other, "A"), older: matrixUpdate(other, "B"), newer: matrixUpdate([]string{"L", "Other"}, "B"), want: matrixNode([]string{"L", "Other"}, "B"), replaced: []string{"A"}},
		{name: "delete then recreate", operations: []string{"delete", "create"}, initial: matrixNode(l, "A"), older: matrixDelete, newer: matrixCreate(l, "A"), want: matrixNode(l, "A")},
		{name: "create overtaken by an update", operations: []string{"create", "update"}, older: matrixCreate(l, "A"), newer: matrixUpdate(l, "B"), want: matrixNode(l, "B"), replaced: []string{"A"}},
		{name: "two updates in one transaction", operations: []string{"update"}, initial: matrixNode(l, "A"), older: matrixUpdate(l, "B", "C"), newer: matrixUpdate(l, "A"), want: matrixNode(l, "A"), replaced: []string{"B", "C"}, explicitOnly: true},
	}
}

func newPublicationMatrixEngine(t *testing.T, opts ...func(*BadgerOptions)) *BadgerEngine {
	t.Helper()
	options := BadgerOptions{InMemory: true}
	for _, opt := range opts {
		opt(&options)
	}
	b, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	sm := b.GetSchemaForNamespace("db")
	require.NoError(t, sm.AddPropertyIndex("idx_l_p", "L", []string{"p"}))
	require.NoError(t, sm.AddCompositeIndex("idx_l_pq", "L", []string{"p", "q"}))
	require.NoError(t, sm.AddUniqueConstraint("unique_l_u", "L", "u"))
	sm.MarkPropertyIndexesFilled()
	return b
}

func TestPostCommitPublicationOrder(t *testing.T) {
	for _, c := range publicationMatrixNodeCases() {
		for _, olderTx := range []bool{false, true} {
			for _, newerTx := range []bool{false, true} {
				if c.explicitOnly && !olderTx {
					continue
				}
				t.Run(fmt.Sprintf("%s/older-tx=%v/newer-tx=%v", c.name, olderTx, newerTx), func(t *testing.T) {
					b := newPublicationMatrixEngine(t)
					runNodeMatrixCase(t, b, c, func() error { return c.older.apply(b, olderTx) }, func() error { return c.newer.apply(b, newerTx) }, newerTx)
				})
			}
		}
	}
}

// runNodeMatrixCase seeds c.initial, holds older's publication, runs newer,
// and checks the consumers.
func runNodeMatrixCase(t *testing.T, b *BadgerEngine, c matrixNodeCase, older, newer func() error, thirdTx bool) {
	t.Helper()
	sm := b.GetSchemaForNamespace("db")
	if c.initial != nil {
		_, err := b.CreateNode(CopyNode(c.initial))
		require.NoError(t, err)
	}
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(b, sm))
	// Warm the caches with the initial state.
	_, _ = b.GetNode(matrixNodeID)
	_, _ = b.GetQueryNode(matrixNodeID)

	publishHeld(t, older, newer, func() {
		// Held: the publication-only consumers still hold the state before
		// the older write, a commit-order prefix.
		checkNodeIndexes(t, b, c.initial, nil, "while the older publication is held")
	})
	checkNodeConsumers(t, b, c.want, c.replaced, thirdTx)
}

// publishHeld runs older until the stage holds its publication, then newer,
// which must not return while the older publication is held; during runs at
// that point. Then it releases the older publication and waits for both.
func publishHeld(t *testing.T, older, newer func() error, during func()) {
	t.Helper()
	started, resume := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	var release sync.Once
	releaseHeld := func() { release.Do(func() { close(resume) }) }
	defer releaseHeld()
	restore := setPublicationStageHook(func(uint64) {
		if held.CompareAndSwap(false, true) {
			close(started)
			<-resume
		}
	})
	defer restore()

	olderDone := make(chan error, 1)
	go func() { olderDone <- older() }()
	select {
	case <-started:
	case err := <-olderDone:
		t.Fatalf("the older write returned without publishing: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the older write never reached the publication stage")
	}
	newerDone := make(chan error, 1)
	go func() { newerDone <- newer() }()
	select {
	case err := <-newerDone:
		t.Fatalf("the newer write returned (%v) while the older write's publication was held", err)
	case <-time.After(150 * time.Millisecond):
	}
	if during != nil {
		during()
	}
	releaseHeld()
	for name, done := range map[string]chan error{"older": olderDone, "newer": newerDone} {
		select {
		case err := <-done:
			require.NoError(t, err, "%s write", name)
		case <-time.After(5 * time.Second):
			t.Fatalf("the %s write never finished", name)
		}
	}
}

func inLabel(n *Node, label string) bool { return n != nil && hasLabel(n.Labels, label) }

// checkNodeIndexes checks the property index, the composite index and the
// unique-value registry against x's state want; replaced are L values x no
// longer holds.
func checkNodeIndexes(t *testing.T, b *BadgerEngine, want *Node, replaced []string, when string) {
	t.Helper()
	sm := b.GetSchemaForNamespace("db")
	composite, ok := sm.GetCompositeIndex("idx_l_pq")
	require.True(t, ok)
	if inLabel(want, "L") {
		live := want.Properties["p"].(string)
		assert.Equal(t, []NodeID{matrixNodeID}, sm.PropertyIndexLookup("L", "p", live), "property index %s", when)
		assert.Equal(t, []NodeID{matrixNodeID}, composite.LookupFull(live, "w"), "composite index (full) %s", when)
		assert.Equal(t, []NodeID{matrixNodeID}, composite.LookupPrefix(live), "composite index (prefix) %s", when)
		holder, found, complete, _ := sm.lookupUniqueConstraintValueForValidation("L", "u", live)
		assert.True(t, complete && found && holder == matrixNodeID, "unique-value registry holds x's value %s", when)
	}
	for _, value := range replaced {
		assert.NotContains(t, sm.PropertyIndexLookup("L", "p", value), matrixNodeID, "property index keeps replaced %s %s", value, when)
		assert.NotContains(t, composite.LookupFull(value, "w"), matrixNodeID, "composite index keeps replaced %s %s", value, when)
		holder, found, _, _ := sm.lookupUniqueConstraintValueForValidation("L", "u", value)
		assert.False(t, found && holder == matrixNodeID, "unique-value registry keeps replaced %s %s", value, when)
	}
}

// checkNodeConsumers checks every node consumer against want after both
// writes returned, then a third writer.
func checkNodeConsumers(t *testing.T, b *BadgerEngine, want *Node, replaced []string, thirdTx bool) {
	t.Helper()
	assert.Equal(t, committedNodeState(t, b), want != nil, "committed state")
	for name, read := range map[string]func(NodeID) (*Node, error){"node cache": b.GetNode, "query-node cache": b.GetQueryNode} {
		got, err := read(matrixNodeID)
		if want == nil {
			assert.ErrorIs(t, err, ErrNotFound, "%s must not return a deleted node", name)
			continue
		}
		if assert.NoError(t, err, name) {
			assert.Equal(t, want.Properties["p"], got.Properties["p"], "%s must return the committed node", name)
			assert.ElementsMatch(t, want.Labels, got.Labels, "%s labels", name)
		}
	}
	checkNodeIndexes(t, b, want, replaced, "after both writes")

	labelled, err := b.GetNodesByLabel("L")
	require.NoError(t, err)
	assert.Equal(t, inLabel(want, "L"), slices.ContainsFunc(labelled, func(n *Node) bool { return n.ID == matrixNodeID }), "label index")
	count, err := b.NodeCount()
	require.NoError(t, err)
	assert.EqualValues(t, map[bool]int64{true: 1, false: 0}[want != nil], count, "node count")
	labelCount, err := b.NodeCountByLabel("L")
	require.NoError(t, err)
	assert.EqualValues(t, map[bool]int64{true: 1, false: 0}[inLabel(want, "L")], labelCount, "label count")

	// Third writers: a live value is still taken, a replaced one is free.
	if inLabel(want, "L") {
		live := want.Properties["p"].(string)
		assert.Error(t, matrixThirdWriter(b, "db:dup", live, thirdTx), "a second node with x's live unique value must be rejected")
	}
	for _, value := range replaced {
		assert.NoError(t, matrixThirdWriter(b, NodeID("db:third-"+value), value, thirdTx), "no committed node holds %s, so a third node may take it", value)
	}
}

func matrixThirdWriter(b *BadgerEngine, id NodeID, value string, explicit bool) error {
	node := &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"p": value, "q": "w", "u": value}}
	return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(CopyNode(node)); return err },
		func() error { _, err := b.CreateNode(CopyNode(node)); return err })
}

func committedNodeState(t *testing.T, b *BadgerEngine) bool {
	t.Helper()
	exists := false
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(nodeKey(matrixNodeID))
		exists = err == nil
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}))
	return exists
}

// Write-behind replay publishes in the same order: an explicit transaction
// commits directly while a buffered write of the node waits for its replay;
// the replay (FlushWriteBehind) does not return while the transaction's
// publication is held.
func TestPostCommitPublicationOrderWriteBehindReplay(t *testing.T) {
	b := newPublicationMatrixEngine(t, func(o *BadgerOptions) {
		o.InMemory = false
		o.DataDir = t.TempDir()
		o.WriteBehind = true
		o.WriteBehindInterval = time.Hour
	})
	c := matrixNodeCase{initial: matrixNode([]string{"L"}, "A"), want: matrixNode([]string{"L"}, "C"), replaced: []string{"A", "B"}}
	runNodeMatrixCase(t, b, c,
		func() error { return matrixUpdate([]string{"L"}, "B").apply(b, true) },
		func() error {
			if err := b.UpdateNode(matrixNode([]string{"L"}, "C")); err != nil {
				return err
			}
			return b.FlushWriteBehind()
		}, true)
}

// A decay suppression rewrite publishes in the same order.
func TestPostCommitPublicationOrderSuppression(t *testing.T) {
	b := newPublicationMatrixEngine(t)
	old := time.Now().Add(-72 * time.Hour)
	initial := matrixNode([]string{"L"}, "A")
	initial.CreatedAt, initial.UpdatedAt = old, old
	_, err := b.CreateNode(CopyNode(initial))
	require.NoError(t, err)
	// The registry is built before decay hides the node from its scan.
	sm := b.GetSchemaForNamespace("db")
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(b, sm))
	// The binding is created with decay off, so its reconciliation leaves
	// the suppression rewrite to the newer write below.
	require.NoError(t, sm.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "l_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 1, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, sm.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "l_binding", ProfileRef: "l_decay", TargetLabels: []string{"L"}}))
	b.SetDecayEnabled(true)
	older := func() error {
		node := matrixNode([]string{"L"}, "B")
		node.CreatedAt = old
		return b.UpdateNode(node)
	}
	newer := func() error { _, err := b.EnqueueDeindexIfSuppressed(string(matrixNodeID), false); return err }
	publishHeld(t, older, newer, func() { checkNodeIndexes(t, b, initial, nil, "while the older publication is held") })
	checkNodeIndexes(t, b, matrixNode([]string{"L"}, "B"), []string{"A"}, "after both writes")
}

// An embedding sidecar write publishes in the same order.
func TestPostCommitPublicationOrderEmbeddingSidecar(t *testing.T) {
	b := newPublicationMatrixEngine(t)
	b.SetEmbeddingsEnabled(true)
	initial := matrixNode([]string{"L"}, "A")
	_, err := b.CreateNode(CopyNode(initial))
	require.NoError(t, err)
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(b, b.GetSchemaForNamespace("db")))
	written := matrixNode([]string{"L"}, "B")
	older := func() error { return b.UpdateNode(CopyNode(written)) }
	newer := func() error {
		embedded := CopyNode(written)
		embedded.ChunkEmbeddings = [][]float32{{0.1, 0.2}}
		embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1}
		return b.UpdateNodeEmbeddingSidecar(embedded)
	}
	publishHeld(t, older, newer, func() { checkNodeIndexes(t, b, initial, nil, "while the older publication is held") })
	got, err := b.GetNode(matrixNodeID)
	require.NoError(t, err)
	assert.Equal(t, "B", got.Properties["p"])
	assert.Equal(t, [][]float32{{0.1, 0.2}}, got.ChunkEmbeddings, "the node cache must not hide the stored embedding")
	checkNodeConsumers(t, b, written, []string{"A"}, false)
}

// A delete without a pre-delete snapshot (the transaction created the node)
// leaves nothing filed.
func TestPostCommitPublicationWithinOneCommit(t *testing.T) {
	b := newPublicationMatrixEngine(t)
	sm := b.GetSchemaForNamespace("db")
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(b, sm))
	require.NoError(t, writeMaybeInTransaction(b, true, func(tx *BadgerTransaction) error {
		if _, err := tx.CreateNode(matrixNode([]string{"L"}, "A")); err != nil {
			return err
		}
		return tx.DeleteNode(matrixNodeID)
	}, nil))
	checkNodeConsumers(t, b, nil, []string{"A"}, true)
}

type matrixEdgeWrite struct {
	apply func(b *BadgerEngine, explicit bool) error
}

const matrixEdgeID = EdgeID("db:e")

func matrixEdge(edgeType, token string) *Edge {
	return &Edge{ID: matrixEdgeID, StartNode: "db:a", EndNode: "db:b", Type: edgeType, Properties: map[string]any{"token": token}}
}

func matrixEdgeUpdate(edgeType, token string) matrixEdgeWrite {
	return matrixEdgeWrite{apply: func(b *BadgerEngine, explicit bool) error {
		return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.UpdateEdge(matrixEdge(edgeType, token)) },
			func() error { return b.UpdateEdge(matrixEdge(edgeType, token)) })
	}}
}

var matrixEdgeDelete = matrixEdgeWrite{apply: func(b *BadgerEngine, explicit bool) error {
	return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.DeleteEdge(matrixEdgeID) }, func() error { return b.DeleteEdge(matrixEdgeID) })
}}

func matrixEdgeCreate(edgeType, token string) matrixEdgeWrite {
	return matrixEdgeWrite{apply: func(b *BadgerEngine, explicit bool) error {
		return writeMaybeInTransaction(b, explicit, func(tx *BadgerTransaction) error { return tx.CreateEdge(matrixEdge(edgeType, token)) },
			func() error { return b.CreateEdge(matrixEdge(edgeType, token)) })
	}}
}

func TestPostCommitEdgePublicationOrder(t *testing.T) {
	for _, c := range []struct {
		name         string
		operations   []string
		older, newer matrixEdgeWrite
		want         *Edge
		replaced     []string // LINK tokens the edge no longer holds
	}{
		{name: "update overtaken by an update", operations: []string{"update"}, older: matrixEdgeUpdate("LINK", "B"), newer: matrixEdgeUpdate("LINK", "C"), want: matrixEdge("LINK", "C"), replaced: []string{"A", "B"}},
		{name: "update then restore", operations: []string{"update", "restore"}, older: matrixEdgeUpdate("LINK", "B"), newer: matrixEdgeUpdate("LINK", "A"), want: matrixEdge("LINK", "A"), replaced: []string{"B"}},
		{name: "update then delete", operations: []string{"update", "delete"}, older: matrixEdgeUpdate("LINK", "B"), newer: matrixEdgeDelete, replaced: []string{"A", "B"}},
		{name: "update then type change", operations: []string{"update", "label remove", "label add"}, older: matrixEdgeUpdate("LINK", "B"), newer: matrixEdgeUpdate("OTHER", "B"), want: matrixEdge("OTHER", "B"), replaced: []string{"A", "B"}},
		{name: "delete then recreate", operations: []string{"delete", "create"}, older: matrixEdgeDelete, newer: matrixEdgeCreate("LINK", "A"), want: matrixEdge("LINK", "A")},
	} {
		for _, olderTx := range []bool{false, true} {
			for _, newerTx := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/older-tx=%v/newer-tx=%v", c.name, olderTx, newerTx), func(t *testing.T) {
					b := newPublicationMatrixEngine(t)
					for _, endpoint := range []NodeID{"db:a", "db:b"} {
						_, err := b.CreateNode(&Node{ID: endpoint, Labels: []string{"N"}})
						require.NoError(t, err)
					}
					require.NoError(t, b.CreateEdge(matrixEdge("LINK", "A")))
					sm := b.GetSchemaForNamespace("db")
					require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"token"}}))
					// A rejected duplicate builds the relationship-uniqueness index.
					require.Error(t, createEdgeInTransaction(b, &Edge{ID: "db:warm", StartNode: "db:b", EndNode: "db:a", Type: "LINK", Properties: map[string]any{"token": "A"}}))
					// Warm the edge, adjacency and type caches.
					_, _ = b.GetEdge(matrixEdgeID)
					_, _ = b.GetOutgoingEdges("db:a")
					_, _ = b.GetEdgesByType("LINK")

					publishHeld(t, func() error { return c.older.apply(b, olderTx) }, func() error { return c.newer.apply(b, newerTx) }, nil)

					got, err := b.GetEdge(matrixEdgeID)
					if c.want == nil {
						assert.ErrorIs(t, err, ErrNotFound, "edge cache")
					} else if assert.NoError(t, err, "edge cache") {
						assert.Equal(t, c.want.Type, got.Type, "edge cache")
						assert.Equal(t, c.want.Properties["token"], got.Properties["token"], "edge cache")
					}
					// The second read is served from the adjacency and edge caches.
					for read := 0; read < 2; read++ {
						edges, err := b.GetOutgoingEdges("db:a")
						require.NoError(t, err)
						if c.want == nil {
							assert.Empty(t, edges, "adjacency read %d", read)
						} else if assert.Len(t, edges, 1, "adjacency read %d", read) {
							assert.Equal(t, c.want.Properties["token"], edges[0].Properties["token"], "adjacency read %d", read)
						}
					}
					for _, edgeType := range []string{"LINK", "OTHER"} {
						typed, err := b.GetEdgesByType(edgeType)
						require.NoError(t, err)
						assert.Equal(t, c.want != nil && c.want.Type == edgeType, len(typed) == 1, "edge-type index %s", edgeType)
					}
					count, err := b.EdgeCount()
					require.NoError(t, err)
					assert.EqualValues(t, map[bool]int64{true: 1, false: 0}[c.want != nil], count, "edge count")
					if c.want != nil && c.want.Type == "LINK" {
						assert.Error(t, createEdgeInTransaction(b, &Edge{ID: "db:dup", StartNode: "db:b", EndNode: "db:a", Type: "LINK", Properties: map[string]any{"token": c.want.Properties["token"]}}),
							"a second edge with the live unique value must be rejected")
					}
					for _, token := range c.replaced {
						assert.NoError(t, createEdgeInTransaction(b, &Edge{ID: EdgeID("db:third-" + token), StartNode: "db:b", EndNode: "db:a", Type: "LINK", Properties: map[string]any{"token": token}}),
							"no committed edge holds %s, so a third edge may take it", token)
					}
				})
			}
		}
	}
}

func writeMaybeInTransaction(b *BadgerEngine, explicit bool, inTx func(*BadgerTransaction) error, direct func() error) error {
	if !explicit {
		return direct()
	}
	tx, err := b.BeginTransaction()
	if err != nil {
		return err
	}
	if err := inTx(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func createEdgeInTransaction(b *BadgerEngine, edge *Edge) error {
	return writeMaybeInTransaction(b, true, func(tx *BadgerTransaction) error { return tx.CreateEdge(edge) }, nil)
}
