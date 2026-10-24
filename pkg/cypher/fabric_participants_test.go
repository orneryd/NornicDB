package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A remote constituent's engine inside a composite statement is one per
// constituent, which the statement recognises as its own. A constituent
// whose engine is not a remote engine cannot join the statement's remote
// session: every operation on it fails, and its engine is never reached.
func TestRemoteConstituentEngineJoinsTheStatement(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "far")
	c := &cypherFabricExecutor{base: NewStorageExecutor(inner), autoCommit: true, remote: fabric.NewRemoteFragmentExecutor()}
	t.Cleanup(func() { _ = c.remote.Close() })
	composite := c.statementFor(ctx, fabric.NewFabricTransaction("far"))

	unjoined := composite.remote("remote:far", inner)
	require.Same(t, unjoined, composite.remote("remote:far", inner), "one engine per constituent")
	_, err := unjoined.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Far"}})
	require.ErrorContains(t, err, "storage engine has unexpected type for 'remote:far'")
	_, err = unjoined.GetNode("n1")
	require.ErrorContains(t, err, "storage engine has unexpected type for 'remote:far'")
	require.Zero(t, must(inner.NodeCount()), "the engine is never reached")

	raw, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: "http://127.0.0.1:1", Database: "far"})
	require.NoError(t, err)
	bound := composite.remote("remote:raw", raw)
	require.NotSame(t, raw, bound, "the statement's engine is not the remote's own")
	require.True(t, composite.joined(bound))
	require.False(t, composite.joined(raw))
	exec, _, err := composite.on(ctx, "far", NewStorageExecutor(bound))
	require.NoError(t, err)
	require.Same(t, storage.Engine(bound), exec.storage, "a statement on it stays on it")
}

// Every optional storage capability a statement finds by unwrapping its
// storage (storage.EngineUnwrapper) is the database's own through a
// composite statement's participant storage: change capture, stored reads, entity
// identity, transactions and their commit, the WAL and the database name,
// decay, suppressions and the Badger engine. Finding them begins and claims
// nothing; MERGE key reservation is the participant storage's own
// (storage.MergeNodePreparer).
func TestParticipantStorageCapabilityInventory(t *testing.T) {
	ctx := context.Background()
	badger, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	db := storage.NewNamespacedEngine(badger, "one")
	tx := fabric.NewFabricTransaction("inventory")
	c := &cypherFabricExecutor{base: NewStorageExecutor(db), autoCommit: true}
	composite := c.statementFor(ctx, tx)
	participant, err := composite.participant("one", db)
	require.NoError(t, err)

	committer := func(engine storage.Engine) any {
		for engine != nil {
			if committer, ok := engine.(interface {
				CommitTransaction(context.Context, *storage.BadgerTransaction) error
			}); ok {
				return committer
			}
			wrapper, ok := engine.(storage.EngineUnwrapper)
			if !ok {
				return nil
			}
			engine = wrapper.GetInnerEngine()
		}
		return nil
	}
	lookup := func(engine storage.Engine) map[string]any {
		capture, captures := storage.ChangeCaptureOf(engine)
		_, stored := storage.StoredReader(engine)
		exec := &StorageExecutor{storage: engine}
		engines := exec.resolveImplicitTxEngines()
		wal, walDatabase := exec.resolveWALAndDatabase()
		return map[string]any{
			"unwrapped":       storage.UnwrapEngine(engine),
			"change capture":  capture,
			"captures":        captures,
			"stored reads":    stored,
			"entity identity": storage.QualifiedEntityID(engine, "n"),
			"transactions":    engines.txEngine != nil,
			"namespace":       engines.namespace,
			"commit":          committer(engine),
			"wal":             wal,
			"wal database":    walDatabase,
			"database":        storageDatabaseName(engine),
			"decay":           storageHasDecayFiltering(engine),
			"suppressions":    manualSuppressionEngine(engine) != nil,
			"badger":          unwrapBadgerEngine(engine),
		}
	}
	want := lookup(db)
	require.Equal(t, true, want["captures"], "the database captures changes")
	require.Equal(t, true, want["transactions"])
	require.NotNil(t, want["badger"])
	require.Equal(t, want, lookup(participant.storage))
	_, ok := storage.Engine(participant.storage).(storage.MergeNodePreparer)
	require.True(t, ok, "MERGE key reservation goes through the participant's transaction")
	require.Nil(t, participant.storage.lazy.tx.Load(), "finding capabilities begins nothing")
	require.Empty(t, tx.WriteShard(), "and claims nothing")
}

// A participant's storage begins its transaction at the first operation
// that reaches it, a read included; beginning claims nothing, nor does a
// MERGE's key reservation, and the transaction's first write claims the
// database. Every read answers as the database does, and then sees what the
// transaction wrote. Over an engine without transactions none begins, and
// the reads read the engine.
func TestParticipantStorageReadsBeforeItsFirstWrite(t *testing.T) {
	ctx := context.Background()
	db := storage.NewNamespacedEngine(newTestMemoryEngine(t), "one")
	_, err := db.CreateNode(&storage.Node{ID: "a", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(1)}})
	require.NoError(t, err)
	_, err = db.CreateNode(&storage.Node{ID: "b", Labels: []string{"P"}})
	require.NoError(t, err)
	require.NoError(t, db.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "R"}))

	tx := fabric.NewFabricTransaction("reads")
	c := &cypherFabricExecutor{base: NewStorageExecutor(db), autoCommit: true}
	composite := c.statementFor(ctx, tx)
	participant, err := composite.participant("one", db)
	require.NoError(t, err)
	w := participant.storage

	reads := func(t *testing.T, all int) {
		t.Helper()
		node, err := w.GetNode("a")
		require.NoError(t, err)
		require.Equal(t, int64(1), node.Properties["k"])
		_, err = w.GetQueryNode("a")
		require.NoError(t, err)
		projected, err := w.GetNodeProjected("a", []string{"k"})
		require.NoError(t, err)
		require.Equal(t, int64(1), projected.Properties["k"])
		_, _ = w.RelationshipEndpointVisible("a")
		edge, err := w.GetEdge("ab")
		require.NoError(t, err)
		require.Equal(t, "R", edge.Type)
		nodes, err := w.GetNodesByLabel("P")
		require.NoError(t, err)
		require.Len(t, nodes, 2)
		for _, stored := range []bool{false, true} {
			seen := 0
			require.NoError(t, w.streamNodesByLabelProjected("P", []string{"k"}, stored, func(*storage.Node) error { seen++; return nil }))
			require.Equal(t, 2, seen)
		}
		seen := 0
		require.NoError(t, w.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{}, func(*storage.Node) error { seen++; return nil }))
		require.GreaterOrEqual(t, seen, 2)
		for _, edges := range [][]*storage.Edge{
			must(w.GetOutgoingEdges("a")), must(w.GetIncomingEdges("b")), must(w.GetEdgesBetween("a", "b")),
			must(w.GetEdgesByType("R")), must(w.IncidentEdges("a")), must(w.MatchEdgesBetween("a", "b", "R", nil, func(*storage.Edge) bool { return true })),
		} {
			require.Len(t, edges, 1)
		}
		_, _, _ = w.OutgoingEdgeHeaders("a")
		_, _, _ = w.IncomingEdgeHeaders("b")
		require.NotNil(t, w.GetEdgeBetween("a", "b", "R"))
		require.Len(t, must(w.AllNodes()), all)
		require.Len(t, must(w.AllEdges()), 1)
		require.Len(t, w.GetAllNodes(), all)
		count, err := w.NodeCount()
		require.NoError(t, err)
		require.Equal(t, int64(all), count)
		count, err = w.NodeCountByLabel("P")
		require.NoError(t, err)
		require.Equal(t, int64(2), count)
		count, err = w.EdgeCount()
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
		count, err = w.EdgeCountByType("R")
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
		_, err = w.GetStoredNode("a")
		require.NoError(t, err)
		_, err = w.GetStoredEdge("ab")
		require.NoError(t, err)
		require.Len(t, must(w.GetStoredOutgoingEdges("a")), 1)
		require.Len(t, must(w.GetStoredIncomingEdges("b")), 1)
		require.Len(t, must(w.GetStoredNodesByLabel("P")), 2)
	}

	require.Nil(t, w.lazy.tx.Load(), "nothing has reached the storage")
	reads(t, 2)
	require.NotNil(t, w.lazy.tx.Load(), "a read begins the transaction")
	require.Empty(t, tx.WriteShard(), "beginning claims nothing")
	for _, write := range []bool{false, true} {
		require.NoError(t, prepareMergeKeys(ctx, w, []string{"P"}, map[string]interface{}{"k": int64(1)}, write))
	}
	require.Empty(t, tx.WriteShard(), "a MERGE's key reservation claims nothing")
	reads(t, 2)

	_, err = w.CreateNode(&storage.Node{ID: "c", Labels: []string{"Q"}})
	require.NoError(t, err)
	require.NotNil(t, w.transaction())
	require.Equal(t, "local:one", tx.WriteShard())
	reads(t, 3)
	node, err := w.GetNode("c")
	require.NoError(t, err, "the transaction reads its own write")
	require.Equal(t, []string{"Q"}, node.Labels)
	_, err = w.CreateNode(&storage.Node{ID: "d", Labels: []string{"Q"}})
	require.NoError(t, err)
	require.NoError(t, w.DeleteConnectedNode("d"))
	require.NoError(t, tx.Commit(nil, nil))
	_, err = db.GetNode("c")
	require.NoError(t, err)
	_, err = db.GetNode("d")
	require.ErrorIs(t, err, storage.ErrNotFound)

	// Over an engine with only the Engine methods no transaction begins:
	// the reads answer through those methods.
	plain := capabilityFreeEngine{db}
	c = &cypherFabricExecutor{base: NewStorageExecutor(plain), autoCommit: true}
	composite = c.statementFor(ctx, fabric.NewFabricTransaction("plain"))
	participant, err = composite.participant("one", plain)
	require.NoError(t, err)
	w = participant.storage
	reads(t, 3)
	require.Nil(t, w.lazy.tx.Load(), "no transaction began")

	// A participant whose database another one already claimed writes
	// nothing, however it writes.
	tx = fabric.NewFabricTransaction("refused")
	_, err = tx.ClaimWrite("local:other")
	require.NoError(t, err)
	c = &cypherFabricExecutor{base: NewStorageExecutor(db), autoCommit: true}
	composite = c.statementFor(ctx, tx)
	participant, err = composite.participant("one", db)
	require.NoError(t, err)
	w = participant.storage
	_, err = w.CreateNode(&storage.Node{ID: "e", Labels: []string{"Q"}})
	require.ErrorIs(t, err, fabric.ErrSecondWriteShard)
	for name, write := range map[string]func() error{
		"UpdateNode":          func() error { return w.UpdateNode(&storage.Node{ID: "a", Labels: []string{"P"}}) },
		"DeleteNode":          func() error { return w.DeleteNode("a") },
		"DeleteConnectedNode": func() error { return w.DeleteConnectedNode("a") },
		"CreateEdge":          func() error { return w.CreateEdge(&storage.Edge{ID: "x", StartNode: "a", EndNode: "b", Type: "R"}) },
		"UpdateEdge":          func() error { return w.UpdateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "R"}) },
		"DeleteEdge":          func() error { return w.DeleteEdge("ab") },
		"BulkCreateEdges": func() error {
			return w.BulkCreateEdges([]*storage.Edge{{ID: "x", StartNode: "a", EndNode: "b", Type: "R"}})
		},
		"BulkDeleteNodes": func() error { return w.BulkDeleteNodes([]storage.NodeID{"a"}) },
		"BulkDeleteEdges": func() error { return w.BulkDeleteEdges([]storage.EdgeID{"ab"}) },
	} {
		require.ErrorIs(t, write(), fabric.ErrSecondWriteShard, name)
	}
	require.Zero(t, w.transaction().OperationCount(), "the transaction holds nothing")
	require.Equal(t, "local:other", tx.WriteShard())
	require.Equal(t, int64(3), must(db.NodeCount()), "the database is unchanged")
}

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

// A write a composite statement refuses fails the procedures that run
// statements in batches (apoc.periodic.iterate, apoc.periodic.commit), which
// count other failures per operation; and a schema change, a knowledge-policy
// command or a suppression on a second database is refused before it is made.
func TestCompositeRefusalsFailTheirStatement(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	for name, fragment := range map[string]string{
		"periodic_iterate": "CALL apoc.periodic.iterate($iterate, 'CREATE (:AcAtom {run: $r})', {batchSize: 1}) YIELD total RETURN total AS w",
		"periodic_commit":  "CALL apoc.periodic.commit('CREATE (:AcAtom {run: $run}) RETURN 0 AS count', {run: $run}) YIELD updates RETURN updates AS w",
		"fulltext_index":   "CALL db.index.fulltext.createNodeIndex($index, ['AcAtom'], ['run']) YIELD name RETURN 1 AS w",
		"schema_command":   "CALL apoc.cypher.doIt('CREATE INDEX ' + $index + ' FOR (n:AcAtom) ON (n.run)', {}) YIELD value RETURN 1 AS w",
	} {
		t.Run(name, func(t *testing.T) {
			run := "refusal-" + name
			params := fabricEffectParams(run)
			params["iterate"] = "UNWIND ['" + run + "'] AS r RETURN r"
			params["index"] = "refusal_" + name
			query := "USE comp CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } CALL { WITH z USE comp.one " + fragment + " } RETURN w, z"
			_, err := fixture.exec.Execute(ctx, query, params)
			require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
			require.Equal(t, int64(0), fixture.writes(t, "comp.one", run))
			require.Equal(t, int64(0), fixture.writes(t, "comp.two", run))
		})
	}
}

// capabilityFreeEngine is an engine with none of the optional capabilities.
type capabilityFreeEngine struct{ storage.Engine }
