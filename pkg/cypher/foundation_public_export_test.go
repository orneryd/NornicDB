package cypher

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/enginecontract"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This bridge exists only in the test binary. External tests can compose the
// private Cypher views with replication/auth without adding production imports.
type FoundationPublicFixture struct {
	Engine       storage.Engine
	QueryEngine  storage.Engine
	PublicNode   func(*storage.Node) *storage.Node
	Owner        *StorageExecutor
	Committed    storage.Engine
	Empty        bool
	Leaf         bool
	NodeID       storage.NodeID
	Execute      func(context.Context, string) (*ExecuteResult, error)
	Consume      func(*testing.T, string)
	LeaseContext context.Context
}

func FoundationIssueLeafFixture(t *testing.T, engine storage.Engine) FoundationPublicFixture {
	id := storage.NodeID("logical")
	if storage.DatabaseOf(engine) == "" {
		id = "nornic:logical"
	}
	_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"FoundationLogical"}, Properties: map[string]interface{}{"id": "logical", "v": int64(7)}})
	require.NoError(t, err)
	queryEngine := engine
	if storage.DatabaseOf(engine) == "" {
		queryEngine = storage.NewNamespacedEngine(engine, "nornic")
	}
	owner := NewStorageExecutor(queryEngine)
	_, err = owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if owner.txContext != nil && owner.txContext.active {
			_, _ = owner.Execute(context.Background(), "ROLLBACK", nil)
		}
	})
	return FoundationPublicFixture{Engine: engine, QueryEngine: queryEngine, Owner: owner, Committed: queryEngine, Leaf: true, NodeID: id}
}

func FoundationIssuePublicFixture(t *testing.T, kind string, wrap func(storage.Engine) storage.Engine) FoundationPublicFixture {
	base := enginePopulationBase(t)
	tenant := storage.NewNamespacedEngine(base, "tenant")
	_, err := tenant.CreateNode(&storage.Node{ID: "logical", Labels: []string{"FoundationLogical"}, Properties: map[string]interface{}{"id": "logical", "v": int64(7)}})
	require.NoError(t, err)
	var original storage.Engine = tenant
	if kind == "NamespacedEngine" {
		original = base
	}
	owner := NewStorageExecutor(original)
	_, err = owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if owner.txContext != nil && owner.txContext.active {
			_, _ = owner.Execute(context.Background(), "ROLLBACK", nil)
		}
	})
	view := owner.transactionStorageFor(owner.txContext.tx.(*storage.BadgerTransaction))
	var issued storage.Engine = view
	switch kind {
	case "transactionStorageWrapper":
	case "algorithmRelationshipReader":
		issued = &algorithmRelationshipReader{Engine: view, types: map[string]struct{}{"R": {}}}
	case "graphIdentityStorage":
		issued = &graphIdentityStorage{Engine: view}
	case "systemGraphView":
		issued = systemGraphView{Engine: view}
	default:
		require.NotNil(t, wrap)
		issued = wrap(view)
	}
	f := FoundationPublicFixture{Engine: issued, Owner: owner, Committed: tenant, Empty: kind == "systemGraphView", NodeID: "logical"}
	if kind == "graphIdentityStorage" {
		f.PublicNode = graphIdentityNodeValue
	}
	return f
}

// Every consumer runs against a fresh real issued view. No test-only owner
// tokens, copied state flags or expected values from capability discovery.
func FoundationRunPublicConsumer(t *testing.T, f FoundationPublicFixture, consumer string) {
	t.Helper()
	if f.Consume != nil {
		f.Consume(t, consumer)
		return
	}
	ctx := context.Background()
	queryEngine := f.Engine
	if f.QueryEngine != nil {
		queryEngine = f.QueryEngine
	}
	member := NewStorageExecutor(queryEngine)
	// Native leaves own issuance, rather than carrying another transaction's
	// membership. Exercise their lifecycle-dependent consumers through that
	// public BEGIN's actual adapter; data/exact operations retain the leaf.
	if f.Leaf && (consumer == "membership" || consumer == "periodic" || consumer == "actions" || consumer == "schema" || consumer == "lease" || consumer == "mode") {
		member = NewStorageExecutor(f.Owner.transactionStorageFor(f.Owner.txContext.tx.(*storage.BadgerTransaction)))
	}
	run := func(callCtx context.Context, query string) (*ExecuteResult, error) {
		if f.Execute != nil {
			return f.Execute(callCtx, query)
		}
		return member.Execute(callCtx, query, nil)
	}
	execute := func(query string) *ExecuteResult { r, err := run(ctx, query); require.NoError(t, err); return r }
	if f.Empty && (consumer == "rows" || consumer == "identity" || consumer == "membership" || consumer == "mutation" || consumer == "actions" || consumer == "observed-node" || consumer == "observed-edge" || consumer == "periodic") {
		query := "MATCH(n) RETURN n"
		if consumer == "mutation" || consumer == "observed-node" || consumer == "observed-edge" {
			query = "CREATE (:FoundationLogicalWrite)"
		}
		_, err := member.Execute(ctx, query, nil)
		require.Error(t, err, "system public graph operations must refuse before access")
		result, err := member.Execute(ctx, "CALL db.schema.visualization()", nil)
		require.NoError(t, foundationEmptyGraphOutcome(result, err))
		return
	}
	switch consumer {
	case "rows", "identity":
		r := execute("MATCH (n:FoundationLogical) RETURN n.v, id(n) LIMIT 1")
		if f.Empty {
			require.Empty(t, r.Rows)
		} else {
			require.Len(t, r.Rows, 1)
			require.Equal(t, int64(7), r.Rows[0][0])
			require.NotEmpty(t, r.Rows[0][1])
		}
	case "visualization":
		// Invoke the registered handler at the issued view, before a router can
		// issue another system scope and conceal a substituted adapter.
		r, err := member.callDbSchemaVisualizationWithContext(ctx)
		require.NoError(t, err)
		if f.Empty {
			require.NoError(t, foundationEmptyGraphOutcome(r, err))
		} else {
			require.NotEmpty(t, r.Rows[0][0])
		}
	case "membership", "mutation", "observed-node", "observed-edge":
		if f.Empty {
			r := execute("MATCH (n:FoundationLogical) RETURN n")
			require.Empty(t, r.Rows)
			return
		}
		if consumer == "observed-node" {
			before, err := f.Engine.GetNode(f.NodeID)
			require.NoError(t, err)
			require.NotNil(t, before)
			if f.PublicNode != nil {
				before = f.PublicNode(before)
			}
			after := *before
			after.Properties = map[string]interface{}{"v": int64(8)}
			require.NoError(t, storage.UpdateObservedNode(f.Engine, before, &after))
			r := execute("MATCH (n:FoundationLogical) RETURN n.v")
			require.Equal(t, int64(8), r.Rows[0][0])
		} else if consumer == "observed-edge" {
			// Refusal is exact: there is no optional observed implementation on any
			// current member. The ordinary mutation still targets the issued Engine.
			edgeID := storage.EdgeID("edge")
			if f.Leaf && storage.DatabaseOf(f.Engine) == "" {
				edgeID = "nornic:edge"
			}
			before := &storage.Edge{ID: edgeID, StartNode: f.NodeID, EndNode: f.NodeID, Type: "R", Properties: map[string]interface{}{"v": int64(1)}}
			require.NoError(t, f.Engine.CreateEdge(before))
			after := *before
			after.Properties = map[string]interface{}{"v": int64(2)}
			require.NoError(t, storage.UpdateObservedEdge(f.Engine, before, &after))
			got, err := f.Engine.GetEdge(edgeID)
			require.NoError(t, err)
			require.Equal(t, int64(2), got.Properties["v"])
		} else {
			execute("CREATE (:FoundationLogicalWrite {v:8})")
		}
		nodes, err := f.Committed.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		rawLeaf := f.Leaf && consumer != "membership"
		if rawLeaf && consumer == "mutation" {
			require.Len(t, nodes, 1)
		} else {
			require.Empty(t, nodes, "reconstructed execution must not independently publish")
		}
		_, err = f.Owner.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		nodes, err = f.Committed.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		if rawLeaf && consumer == "mutation" {
			require.Len(t, nodes, 1)
		} else {
			require.Empty(t, nodes)
		}
		_, err = member.Execute(ctx, "RETURN 1", nil)
		if rawLeaf {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, storage.ErrTransactionClosed)
		}
	case "control":
		if f.Leaf {
			for _, command := range []string{"ROLLBACK", "BEGIN", "COMMIT"} {
				_, err := f.Owner.Execute(ctx, command, nil)
				require.NoError(t, err)
			}
			return
		}
		for _, command := range []string{"COMMIT", "ROLLBACK", "BEGIN"} {
			_, err := run(ctx, command)
			require.Error(t, err, "membership never grants controller authority")
		}
		require.True(t, f.Owner.txContext.active)
	case "periodic":
		if f.Empty {
			r := execute("MATCH (n) RETURN count(n)")
			require.Equal(t, int64(0), r.Rows[0][0])
			return
		}
		execute("CALL apoc.periodic.iterate('UNWIND [1,2] AS x RETURN x','CREATE (:FoundationLogicalWrite {v:x})',{batchSize:1}) YIELD total RETURN total")
		_, err := f.Owner.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		nodes, err := f.Committed.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		require.Empty(t, nodes)
	case "schema":
		if f.Empty {
			_, err := member.Execute(ctx, "CREATE INDEX logical_idx FOR (n:FoundationLogical) ON (n.v)", nil)
			require.Error(t, err)
			return
		}
		execute("CREATE INDEX logical_idx FOR (n:FoundationLogical) ON (n.v)")
		_, err := f.Owner.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		r, err := NewStorageExecutor(f.Committed).Execute(ctx, "SHOW INDEXES YIELD name WHERE name = 'logical_idx' RETURN name", nil)
		require.NoError(t, err)
		require.Len(t, r.Rows, 1)
	case "actions":
		if f.Empty {
			r := execute("MATCH (n) RETURN n")
			require.Empty(t, r.Rows)
			return
		}
		if f.PublicNode != nil {
			node, err := f.Engine.GetNode(f.NodeID)
			require.NoError(t, err)
			_, err = member.Execute(ctx, "CALL kp.suppress($entity)", map[string]interface{}{"entity": f.PublicNode(node)})
			require.NoError(t, err)
		} else {
			execute("MATCH (n:FoundationLogical) CALL kp.suppress(n) RETURN n")
		}
		_, err := f.Owner.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		nodes, err := f.Committed.GetNodesByLabel("FoundationLogical")
		require.NoError(t, err)
		require.Empty(t, nodes)
	case "lease":
		adapter, ok := storage.MembershipForEngine(f.Engine).Adapter.(*transactionStorageWrapper)
		if !ok {
			adapter = f.Owner.transactionStorageFor(f.Owner.txContext.tx.(*storage.BadgerTransaction))
		}
		sourceCtx := ctx
		if f.LeaseContext != nil {
			sourceCtx = f.LeaseContext
		}
		lease, release := storage.BeginQueryRevealScopeContext(sourceCtx, f.Committed, false)
		unbind := adapter.bindReadLease(lease, func() {})
		done := make(chan error, 1)
		go func() {
			query := "RETURN 1"
			if f.Empty {
				query = "CALL db.schema.visualization()"
			}
			_, err := run(context.Background(), query)
			done <- err
			close(done)
		}()
		t.Cleanup(func() { <-done })
		select {
		case err := <-done:
			release()
			unbind()
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			release()
			unbind()
			t.Error("reconstructed consumer reacquired its live owner's lease")
		}
	case "mode":
		if f.Empty {
			_, err := member.Execute(ctx, "CALL apoc.cypher.run('CREATE (:FoundationLogicalWrite)',{}) YIELD value RETURN value", nil)
			require.Error(t, err)
			return
		}
		adapter, ok := storage.MembershipForEngine(f.Engine).Adapter.(*transactionStorageWrapper)
		if !ok {
			adapter = f.Owner.transactionStorageFor(f.Owner.txContext.tx.(*storage.BadgerTransaction))
		}
		mode := &resolvedStatementMode{}
		mode.active.Store(true)
		adapter.statementMode = mode
		execute("CALL apoc.cypher.run('CREATE (:FoundationLogicalWrite) RETURN 1 AS v',{}) YIELD value RETURN value")
		if !f.Empty {
			require.True(t, mode.write.Load(), "actual nested write must report through the carried live observer")
		}
	case "limits":
		adapter, ok := storage.MembershipForEngine(f.Engine).Adapter.(*transactionStorageWrapper)
		if !ok {
			adapter = f.Owner.transactionStorageFor(f.Owner.txContext.tx.(*storage.BadgerTransaction))
		}
		provider := &fakeLimitedEngine{Engine: adapter.underlying, checker: fakeQueryLimitChecker{limits: fakeMaxResults(2)}}
		adapter.underlying = provider
		// Enforcement is an issuance dependency; publish a new executor after the
		// source replacement, then change only the provider's live values.
		if f.Leaf {
			member = NewStorageExecutor(adapter)
		} else {
			member = NewStorageExecutor(f.Engine)
		}
		for _, limit := range []int{2, 3} {
			provider.checker = fakeQueryLimitChecker{limits: fakeMaxResults(limit)}
			query := "UNWIND [1,2,3,4] AS x RETURN x"
			if f.Empty {
				query = "SHOW PROCEDURES"
			}
			r := execute(query)
			require.Len(t, r.Rows, limit)
		}
	default:
		t.Fatalf("unclassified public consumer %s", consumer)
	}
}

// FoundationCheckPublicPolicy pairs each logical field with executable public
// consumer adapters. The policy string is independent test fixture data.
func FoundationCheckPublicPolicy(t *testing.T, name string, dispositions []storage.BindingDisposition, issue func(*testing.T) FoundationPublicFixture) *enginecontract.PublicProof {
	f := issue(t)
	typ := reflect.TypeOf(f.Engine)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	require.Equal(t, name, typ.PkgPath()+"."+typ.Name())
	policy := f.Engine.(storage.EngineBoundaryDeclaration).EngineCapabilityPolicy()
	value := reflect.ValueOf(policy)
	require.Len(t, dispositions, value.NumField()-1)
	obligations := map[string]enginecontract.PublicObligation{}
	for i := 1; i < value.NumField(); i++ {
		field := value.Type().Field(i).Name
		consumers := map[string]func(*testing.T){}
		for _, consumer := range enginecontract.LogicalConsumers[field] {
			consumers[consumer] = func(t *testing.T) { FoundationRunPublicConsumer(t, issue(t), consumer) }
		}
		obligations[field] = enginecontract.PublicObligation{Disposition: dispositions[i-1], Consumers: consumers}
	}
	return enginecontract.CheckPublicObligations(t, f.Engine, obligations)
}
