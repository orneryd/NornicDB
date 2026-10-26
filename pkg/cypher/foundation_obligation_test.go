package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func foundationWAL(t *testing.T, engine storage.Engine) *storage.WALEngine {
	t.Helper()
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	return storage.NewWALEngine(engine, wal)
}

// The oracle consumes actual procedure output, not the resolver's references.
// The same oracle must reject an adapter substituted for the issued view.
func foundationEmptyGraphOutcome(result *ExecuteResult, err error) error {
	if err != nil {
		return err
	}
	if result == nil || len(result.Rows) != 1 || len(result.Rows[0]) != 2 {
		return fmt.Errorf("missing visualization result: %v", result)
	}
	nodes, ok := result.Rows[0][0].([]*storage.Node)
	if !ok || len(nodes) != 0 {
		return fmt.Errorf("system graph exposed nodes: %v", result.Rows[0][0])
	}
	edges, ok := result.Rows[0][1].([]*storage.Edge)
	if !ok || len(edges) != 0 {
		return fmt.Errorf("system graph exposed edges: %v", result.Rows[0][1])
	}
	return nil
}

func TestFoundationIssuedSystemViewObligation(t *testing.T) {
	base := enginePopulationBase(t)
	system := storage.NewNamespacedEngine(base, "system")
	_, err := system.CreateNode(&storage.Node{ID: "secret", Labels: []string{"PrivateCatalog"}})
	require.NoError(t, err)
	owner := NewStorageExecutor(system)
	_, err = owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	defer owner.Execute(context.Background(), "ROLLBACK", nil)
	result, err := owner.Execute(context.Background(), "CALL db.schema.visualization()", nil)
	require.NoError(t, foundationEmptyGraphOutcome(result, err))

	adapter := owner.transactionStorageFor(owner.txContext.tx.(*storage.BadgerTransaction))
	issued := owner.cloneWithStorage(systemGraphView{Engine: adapter})
	result, err = issued.callDbSchemaVisualizationWithContext(context.Background())
	require.NoError(t, foundationEmptyGraphOutcome(result, err))
	// Deliberately reproduce the old binding substitution at the consumer.
	mutant := owner.cloneWithStorage(adapter)
	result, err = mutant.callDbSchemaVisualizationWithContext(context.Background())
	require.Error(t, foundationEmptyGraphOutcome(result, err), "negative control must fail the SAME public outcome oracle")
}

func TestFoundationReconstructedPeriodicObligation(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			owner, committed := newProcedureAdmissionExecutor(t, false)
			if explicit {
				_, err := owner.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
			}
			const name = "foundation.reconstructedPeriodic"
			cleanupAdmissionProcedure(t, name)
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "total", Type: "INTEGER"}}}, func(ctx context.Context, caller *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				issued, err := caller.FunctionStorage("")
				if err != nil {
					return nil, err
				}
				member := NewStorageExecutor(storage.NewTracedEngine(foundationWAL(t, issued)))
				return member.Execute(context.Background(), "CALL apoc.periodic.iterate('UNWIND [1,2] AS x RETURN x','CREATE (:FoundationPeriodic {v:x})',{batchSize:1}) YIELD total RETURN total", nil)
			}))
			_, callErr := owner.Execute(context.Background(), "CALL "+name+"() YIELD total RETURN total", nil)
			if explicit {
				_, err := owner.Execute(context.Background(), "ROLLBACK", nil)
				require.NoError(t, err)
			}
			nodes, err := committed.GetNodesByLabel("FoundationPeriodic")
			require.NoError(t, err)
			want := 2
			if explicit {
				want = 0
			}
			require.Len(t, nodes, want, "reconstruction must preserve the issuer's batch ownership")
			require.NoError(t, callErr)
		})
	}
}

func TestFoundationPositiveSchemaPublicationObligation(t *testing.T) {
	base := enginePopulationBase(t)
	tenant := storage.NewNamespacedEngine(base, "tenant")
	owner := NewStorageExecutor(tenant)
	_, err := owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	view := owner.transactionStorageFor(owner.txContext.tx.(*storage.BadgerTransaction))
	member := NewStorageExecutor(storage.NewTracedEngine(foundationWAL(t, view)))
	_, err = member.Execute(context.Background(), "CREATE INDEX foundation_positive FOR (n:FoundationPositive) ON (n.v)", nil)
	require.NoError(t, err)
	_, err = owner.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
	result, err := NewStorageExecutor(tenant).Execute(context.Background(), "SHOW INDEXES YIELD name WHERE name = 'foundation_positive' RETURN name", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}

func TestFoundationActionPublicationObligation(t *testing.T) {
	for _, commit := range []bool{false, true} {
		for _, drop := range []bool{false, true} {
			t.Run(fmt.Sprintf("commit=%v/drop-binding=%v", commit, drop), func(t *testing.T) {
				base := enginePopulationBase(t)
				tenant := storage.NewNamespacedEngine(base, "tenant")
				owner := NewStorageExecutor(tenant)
				_, err := owner.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
				view := owner.transactionStorageFor(owner.txContext.tx.(*storage.BadgerTransaction))
				var issued storage.Engine = storage.NewTracedEngine(foundationWAL(t, view))
				if drop {
					binding := view.StorageMembership()
					binding.PendingActions = nil
					issued = &foundationDroppedBinding{Engine: issued, binding: binding}
				}
				member := NewStorageExecutor(issued)
				_, writeErr := member.Execute(context.Background(), "MATCH (n:L) CALL kp.suppress(n) RETURN n", nil)
				command := "ROLLBACK"
				if commit {
					command = "COMMIT"
				}
				_, err = owner.Execute(context.Background(), command, nil)
				require.NoError(t, err)
				nodes, readErr := tenant.GetNodesByLabel("L")
				want := 1
				if commit {
					want = 0
				}
				outcome := writeErr
				if outcome == nil {
					outcome = readErr
				}
				if outcome == nil && len(nodes) != want {
					outcome = fmt.Errorf("action publication: visible=%d want=%d", len(nodes), want)
				}
				if drop && !commit {
					require.Error(t, outcome, "dropping a real action binding must fail the rollback outcome oracle")
				} else {
					require.NoError(t, outcome)
				}
			})
		}
	}
}

// Mutate the actual issued binding, retaining every other causal owner. This
// leaves an observable premature action; it does not compare test-only tokens.
type foundationDroppedBinding struct {
	storage.Engine
	binding storage.MembershipBinding
}

func (v *foundationDroppedBinding) StorageMembership() storage.MembershipBinding { return v.binding }

func TestFoundationReconstructedCoordinatorObligation(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%v", remote), func(t *testing.T) {
			var owner *StorageExecutor
			var committed storage.Engine
			graph := "admission_comp.one"
			if remote {
				var stores [2]storage.Engine
				owner, stores = newRemoteModeExecutor(t, [2]string{"owner", "owner"})
				committed = stores[0]
				graph = "remote_modes.one"
			} else {
				owner, committed = newProcedureAdmissionExecutor(t, true)
			}
			const name = "foundation.reconstructedCoordinator"
			cleanupAdmissionProcedure(t, name)
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "total", Type: "INTEGER"}}}, func(ctx context.Context, caller *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				issued, err := caller.FunctionStorage("")
				if err != nil {
					return nil, err
				}
				member := NewStorageExecutor(storage.NewTracedEngine(foundationWAL(t, issued)))
				return member.Execute(context.Background(), "CALL apoc.periodic.iterate('RETURN 1 AS x','CREATE (:FoundationPeriodic)',{batchSize:1}) YIELD total RETURN total", nil)
			}))
			_, err := owner.Execute(context.Background(), "BEGIN", nil)
			require.NoError(t, err)
			_, err = owner.Execute(context.Background(), "CALL { USE "+graph+" CALL "+name+"() YIELD total RETURN total } RETURN *", nil)
			require.ErrorContains(t, err, "unsupported query type in transaction")
			_, err = owner.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, err)
			nodes, err := committed.GetNodesByLabel("FoundationPeriodic")
			require.NoError(t, err)
			require.Empty(t, nodes, "coordinator refusal precedes every batch action")
		})
	}
}
