package storage

import (
	"errors"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// Ported from c70549944: a transaction's write admission (SetWriteAdmission)
// decides every typed native write, the schema change and the knowledge-policy
// change before it is made. While it refuses, the write fails with its error
// and the transaction stages nothing and commits nothing; once it admits, later
// writes don't ask again. Reads never ask.
func TestTransactionWriteAdmissionDecidesEveryWrite(t *testing.T) {
	engine := createTestBadgerEngine(t)
	begin := func(admit func() error) *BadgerTransaction {
		t.Helper()
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, tx.SetNamespace("adm"))
		tx.SetWriteAdmission(admit)
		return tx
	}
	seed := begin(nil)
	_, err := seed.CreateNode(&Node{ID: "adm:a", Labels: []string{"A"}})
	require.NoError(t, err)
	_, err = seed.CreateNode(&Node{ID: "adm:b", Labels: []string{"A"}})
	require.NoError(t, err)
	require.NoError(t, seed.CreateEdge(&Edge{ID: "adm:ab", StartNode: "adm:a", EndNode: "adm:b", Type: "R"}))
	require.NoError(t, seed.Commit())

	refused := errors.New("refused")
	asks := 0
	tx := begin(func() error { asks++; return refused })
	node, err := tx.GetNode("adm:a")
	require.NoError(t, err, "reads don't ask")
	require.Equal(t, []string{"A"}, node.Labels)
	_, err = tx.GetEdge("adm:ab")
	require.NoError(t, err)
	_, err = tx.IncidentEdges("adm:a")
	require.NoError(t, err)
	view, err := tx.Schema(KnowledgePolicyChanges)
	require.NoError(t, err, "reading the knowledge-policy schema doesn't ask")
	require.Zero(t, asks)

	writes := []struct {
		name string
		do   func() error
	}{
		{"CreateNode", func() error {
			_, err := tx.CreateNode(&Node{ID: "adm:c", Labels: []string{"A"}})
			return err
		}},
		{"CreateMintedNode", func() error {
			_, err := tx.CreateMintedNode(&Node{ID: "adm:minted", Labels: []string{"A"}})
			return err
		}},
		{"UpdateGraphNode", func() error {
			return tx.UpdateGraphNode(&Node{ID: "adm:a", Labels: []string{"A"}, Properties: map[string]interface{}{"v": int64(1)}}, ClientNodeEdits)
		}},
		{"UpdateGraphNodeStoredImages", func() error {
			return tx.UpdateGraphNode(&Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}}, ID: "adm:a", Labels: []string{"A"}}, StoredNodeImages)
		}},
		{"DeleteGraphNode", func() error { return tx.DeleteGraphNode("adm:a", CascadeEdges) }},
		{"DeleteGraphNodeRejectConnected", func() error {
			return tx.DeleteGraphNode("adm:a", RejectConnected)
		}},
		{"CreateEdge", func() error {
			return tx.CreateEdge(&Edge{ID: "adm:ba", StartNode: "adm:b", EndNode: "adm:a", Type: "R"})
		}},
		{"CreateMintedEdge", func() error {
			return tx.CreateMintedEdge(&Edge{ID: "adm:minted-edge", StartNode: "adm:b", EndNode: "adm:a", Type: "R"})
		}},
		{"BulkCreateEdges", func() error {
			return tx.BulkCreateEdges([]*Edge{{ID: "adm:ba", StartNode: "adm:b", EndNode: "adm:a", Type: "R"}})
		}},
		{"UpdateEdge", func() error {
			return tx.UpdateEdge(&Edge{ID: "adm:ab", StartNode: "adm:a", EndNode: "adm:b", Type: "S"})
		}},
		{"DeleteEdge", func() error { return tx.DeleteEdge("adm:ab") }},
		{"Schema", func() error { _, err := tx.Schema(OrdinarySchemaChanges); return err }},
		{"AdmitWrite", tx.AdmitWrite},
	}
	for _, write := range writes {
		require.ErrorIs(t, write.do(), refused, write.name)
		require.Zero(t, tx.OperationCount(), "%s: a refused write stages nothing", write.name)
		require.False(t, tx.HasKnowledgePolicyChanges(), write.name)
		require.Zero(t, tx.pendingNodeCount(), write.name)
		require.Zero(t, tx.pendingEdgeCount(), write.name)
	}

	// A knowledge-policy change asks the admission when it is persisted.
	require.ErrorContains(t, view.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name: "refused", TargetLabels: []string{"A"}, Enabled: true,
	}), "refused")
	require.False(t, tx.HasKnowledgePolicyChanges(), "a refused policy change stages nothing")
	require.Equal(t, len(writes)+1, asks, "each refused write asked once; the refused policy change asked once; reads never ask")

	// It asks again while it refuses: the refusal was not consumed.
	_, err = tx.CreateNode(&Node{ID: "adm:c", Labels: []string{"A"}})
	require.ErrorIs(t, err, refused, "it asks again while it refuses")
	require.Equal(t, len(writes)+2, asks)

	// A refused transaction that is then rolled back leaves storage unchanged.
	require.NoError(t, tx.Rollback())
	_, err = engine.GetNode("adm:c")
	require.ErrorIs(t, err, ErrNotFound)
	edge, err := engine.GetEdge("adm:ab")
	require.NoError(t, err, "the refused update/delete of adm:ab changed nothing")
	require.Equal(t, "R", edge.Type)
	seeded, err := engine.GetNode("adm:a")
	require.NoError(t, err)
	require.NotContains(t, seeded.Properties, "v", "the refused update changed nothing")

	// Once admitted, later writes don't ask again.
	asks = 0
	tx = begin(func() error { asks++; return nil })
	_, err = tx.CreateNode(&Node{ID: "adm:c", Labels: []string{"A"}})
	require.NoError(t, err)
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "adm:a", Labels: []string{"A"}, Properties: map[string]interface{}{"v": int64(1)}}, ClientNodeEdits))
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "adm:a", Labels: []string{"A"}, Properties: map[string]interface{}{"v": int64(1)}}, StoredNodeImages))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "adm:ca", StartNode: "adm:c", EndNode: "adm:a", Type: "R"}))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "adm:ca", StartNode: "adm:c", EndNode: "adm:a", Type: "S"}))
	require.NoError(t, tx.DeleteEdge("adm:ca"))
	require.NoError(t, tx.DeleteGraphNode("adm:b", CascadeEdges))
	require.NoError(t, tx.AdmitWrite())
	require.Equal(t, 1, asks, "once admitted, later writes don't ask")
	require.NoError(t, tx.Commit())
	_, err = engine.GetNode("adm:c")
	require.NoError(t, err)

	// A schema change is admitted once, and the policy change it persists is
	// not asked again.
	asks = 0
	tx = begin(func() error { asks++; return nil })
	view, err = tx.Schema(OrdinarySchemaChanges)
	require.NoError(t, err, "a schema change is admitted")
	require.NoError(t, view.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name: "admitted", TargetLabels: []string{"A"}, Enabled: true,
	}))
	require.Equal(t, 1, asks)
	require.True(t, tx.HasKnowledgePolicyChanges())
}

// A refusal on one write does not poison the transaction: after the admission
// starts admitting, the same transaction makes the write and commits it. The
// refused attempt staged nothing, so the retry is the only mutation.
func TestTransactionWriteAdmissionRetryAfterRejection(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "retry:a", Labels: []string{"A"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("retry"))

	refused := errors.New("refused")
	asks := 0
	admitted := false
	tx.SetWriteAdmission(func() error {
		asks++
		if !admitted {
			return refused
		}
		return nil
	})

	// Refused: nothing staged, nothing written.
	require.ErrorIs(t, tx.CreateEdge(&Edge{ID: "retry:aa", StartNode: "retry:a", EndNode: "retry:a", Type: "R"}), refused)
	require.Zero(t, tx.OperationCount())
	require.Zero(t, tx.pendingEdgeCount())
	require.Zero(t, tx.pendingNodeCount())
	require.False(t, tx.IsCommitted())

	// The retry of the same write is admitted and is the only staged mutation.
	admitted = true
	require.NoError(t, tx.CreateEdge(&Edge{ID: "retry:aa", StartNode: "retry:a", EndNode: "retry:a", Type: "R"}))
	require.Equal(t, 2, asks, "the retry asked again after the refusal")
	require.Equal(t, 1, tx.OperationCount(), "only the admitted write is staged")
	require.False(t, tx.IsCommitted(), "a staged write is not a commit")

	require.NoError(t, tx.Commit())
	require.True(t, tx.IsCommitted())
	_, err = engine.GetEdge("retry:aa")
	require.NoError(t, err)
	require.Equal(t, 2, asks, "no ask after admission")
}

// The admission runs exactly once per transaction, however many writes it
// makes, and every write it then admits reaches storage at commit.
func TestTransactionWriteAdmissionRunsOnceOnSuccess(t *testing.T) {
	engine := createTestBadgerEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("once"))

	asks := 0
	tx.SetWriteAdmission(func() error { asks++; return nil })
	ids := []NodeID{"once:na", "once:nb", "once:nc", "once:nd", "once:ne"}
	for _, id := range ids {
		_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"A"}})
		require.NoError(t, err)
	}
	require.Equal(t, 1, asks, "the transaction asks once")
	require.False(t, tx.IsCommitted(), "not committed before commit")
	require.NoError(t, tx.Commit())
	require.True(t, tx.IsCommitted())
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, len(ids), count)
}

// IsCommitted reports the transaction's commit status before and after its
// terminal operation, and a rejected write never commits.
func TestTransactionIsCommittedStatus(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Active: not committed.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.False(t, tx.IsCommitted())
	require.NoError(t, tx.SetNamespace("status"))
	require.False(t, tx.IsCommitted())

	// Refused write: still not committed.
	refused := errors.New("refused")
	tx.SetWriteAdmission(func() error { return refused })
	_, err = tx.CreateNode(&Node{ID: "status:a", Labels: []string{"A"}})
	require.ErrorIs(t, err, refused)
	require.False(t, tx.IsCommitted())
	require.NoError(t, tx.Rollback())
	require.False(t, tx.IsCommitted(), "a rolled-back transaction is not committed")
	_, err = engine.GetNode("status:a")
	require.ErrorIs(t, err, ErrNotFound, "a rejected write never reached storage")

	// Committed: IsCommitted is true.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("status"))
	tx.SetWriteAdmission(func() error { return nil })
	_, err = tx.CreateNode(&Node{ID: "status:a", Labels: []string{"A"}})
	require.NoError(t, err)
	require.False(t, tx.IsCommitted())
	require.NoError(t, tx.Commit())
	require.True(t, tx.IsCommitted())
	_, err = engine.GetNode("status:a")
	require.NoError(t, err)

	// Empty commit commits too.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("status"))
	require.False(t, tx.IsCommitted())
	require.NoError(t, tx.Commit())
	require.True(t, tx.IsCommitted())
}

// Reads of every family never ask the write admission, on an active
// transaction and on one whose admission would refuse.
func TestTransactionWriteAdmissionReadsDoNotAsk(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "ro:a", Labels: []string{"A"}, Properties: map[string]interface{}{"v": int64(1)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "ro:b", Labels: []string{"A"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "ro:ab", StartNode: "ro:a", EndNode: "ro:b", Type: "R"}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("ro"))
	asks := 0
	tx.SetWriteAdmission(func() error { asks++; return errors.New("refused") })

	_, err = tx.GetNode("ro:a")
	require.NoError(t, err)
	_, err = tx.GetNodeProjected("ro:a", []string{"v"})
	require.NoError(t, err)
	nodes, err := tx.GetNodesByLabel("A")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.NotEmpty(t, tx.GetAllNodes())
	_, err = tx.GetEdge("ro:ab")
	require.NoError(t, err)
	edges, err := tx.IncidentEdges("ro:a")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	_, err = tx.GetEdgesByType("R")
	require.NoError(t, err)
	invalid, err := tx.Schema(SchemaPurpose(255))
	require.ErrorContains(t, err, "invalid transaction schema purpose")
	require.Nil(t, invalid)
	require.Nil(t, tx.knowledgeSchema, "invalid purpose creates no view")
	require.False(t, tx.schemaTransaction, "invalid purpose stages no DDL")
	_, err = tx.Schema(KnowledgePolicyChanges)
	require.NoError(t, err)
	require.Zero(t, asks, "no read or rejected purpose asks the write admission")

	// Another transaction on the same storage keeps its own admission.
	other, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Rollback() })
	require.NoError(t, other.SetNamespace("ro"))
	other.SetWriteAdmission(func() error { asks++; return nil })
	_, err = other.GetNode("ro:a")
	require.NoError(t, err)
	require.Zero(t, asks, "the other transaction's reads don't ask its admission either")
	_, err = other.CreateNode(&Node{ID: "ro:c", Labels: []string{"A"}})
	require.NoError(t, err)
	require.Equal(t, 1, asks)
}

// A refused delete path stages nothing: the rejection is checked before the
// transaction holds a deletion, relationship removals or a commit-time
// connected-delete decision.
func TestTransactionWriteAdmissionRejectedDeleteStagesNothing(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "del:a", Labels: []string{"A"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "del:b", Labels: []string{"A"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "del:ab", StartNode: "del:a", EndNode: "del:b", Type: "R"}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("del"))
	refused := errors.New("refused")
	tx.SetWriteAdmission(func() error { return refused })

	// RejectConnected stages a commit-time decision, and a refusal must not
	// leave one behind.
	require.ErrorIs(t, tx.DeleteGraphNode("del:a", RejectConnected), refused)
	require.Zero(t, tx.OperationCount())
	require.False(t, tx.RequiresCommitValidation())

	// Cascade also deletes incident relationships; a refusal is checked first.
	require.ErrorIs(t, tx.DeleteGraphNode("del:a", CascadeEdges), refused)
	require.Zero(t, tx.OperationCount())
	require.Zero(t, tx.pendingNodeCount())
	require.Zero(t, tx.pendingEdgeCount())

	// The node and its relationship are still visible to this transaction.
	require.NoError(t, func() error { _, err := tx.GetNode("del:a"); return err }())
	require.NoError(t, func() error { _, err := tx.GetEdge("del:ab"); return err }())
	require.NoError(t, tx.Commit())
	require.True(t, tx.IsCommitted())
	_, err = engine.GetNode("del:a")
	require.NoError(t, err, "the rejected deletes changed nothing")
	_, err = engine.GetEdge("del:ab")
	require.NoError(t, err)

	// A cascade delete admitted normally still works afterwards.
	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx2.Rollback() })
	require.NoError(t, tx2.SetNamespace("del"))
	tx2.SetWriteAdmission(func() error { return nil })
	require.NoError(t, tx2.DeleteGraphNode("del:a", CascadeEdges))
	require.NoError(t, tx2.Commit())
	_, err = engine.GetNode("del:a")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge("del:ab")
	require.ErrorIs(t, err, ErrNotFound)
}

// pendingNodeCount and pendingEdgeCount expose the staged body maps so a test
// can assert a refused write staged no entity body.
func (tx *BadgerTransaction) pendingNodeCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.pendingNodes)
}

func (tx *BadgerTransaction) pendingEdgeCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.pendingEdges)
}

// The admission's steady-state cost is a nil check under the transaction lock.
// BenchmarkTransactionAdmissionOverhead measures exactly that path, with no
// storage write: AdmitWrite is the exported entry to the same
// admitWriteLocked the data-write owners call, so nil versus a bound callback is
// compared on identical work.
func BenchmarkTransactionAdmissionOverhead(b *testing.B) {
	cases := []struct {
		name  string
		admit func() error
	}{
		{"nil", nil},
		{"bound_always_admit", func() error { return nil }},
	}
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatalf("new engine: %v", err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tx, err := engine.BeginTransaction()
				if err != nil {
					b.Fatalf("begin: %v", err)
				}
				tx.SetWriteAdmission(bc.admit)
				if err := tx.AdmitWrite(); err != nil {
					b.Fatalf("admit: %v", err)
				}
				if err := tx.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTransactionFirstWriteAdmission measures a transaction's first data
// write, which is the only write that asks a bound admission: nil is the
// same-source release nil-baseline (no callback installed), and
// bound_always_admit pays the callback plus the nil check once. BeginTransaction
// dominates this figure, so it bounds the admission's share of a whole
// transaction rather than isolating it (see BenchmarkTransactionAdmissionOverhead).
func BenchmarkTransactionFirstWriteAdmission(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatalf("new engine: %v", err)
	}
	b.Cleanup(func() { _ = engine.Close() })

	ids := make([]NodeID, 8192)
	for i := range ids {
		ids[i] = NodeID("bench:" + strconv.Itoa(i))
	}
	cases := []struct {
		name  string
		admit func() error
	}{
		{"nil", nil},
		{"bound_always_admit", func() error { return nil }},
	}
	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tx, err := engine.BeginTransaction()
				if err != nil {
					b.Fatalf("begin: %v", err)
				}
				if err := tx.SetNamespace("bench"); err != nil {
					b.Fatalf("namespace: %v", err)
				}
				tx.SetWriteAdmission(bc.admit)
				if _, err := tx.CreateNode(&Node{ID: ids[i%len(ids)], Labels: []string{"A"}}); err != nil {
					b.Fatalf("create: %v", err)
				}
				_ = tx.Rollback()
			}
		})
	}
}
