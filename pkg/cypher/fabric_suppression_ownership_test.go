package cypher

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFabricSuppressionOwnsCompletionAndWriter(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	ctx := context.Background()
	leaf, err := f.mgr.GetStorage("one")
	require.NoError(t, err)
	for _, edge := range []bool{false, true} {
		for _, completion := range []string{"success", "failure", "commit", "rollback", "second_writer_before", "second_writer_after"} {
			t.Run(fmt.Sprintf("edge_%t/%s", edge, completion), func(t *testing.T) {
				run := t.Name()
				id := storage.NodeID(run + "/target")
				peer := storage.NodeID(run + "/peer")
				_, err := leaf.CreateNode(&storage.Node{ID: id, Labels: []string{"VisibilityProof"}, Properties: map[string]interface{}{"run": run, "payload": "retained"}})
				require.NoError(t, err)
				_, err = leaf.CreateNode(&storage.Node{ID: peer, Labels: []string{"VisibilityPeer"}})
				require.NoError(t, err)
				eid := storage.EdgeID(run + "/edge")
				require.NoError(t, leaf.CreateEdge(&storage.Edge{ID: eid, StartNode: id, EndNode: peer, Type: "VisibilityProofLink", Properties: map[string]interface{}{"run": run, "payload": "retained"}}))
				kind, key := storage.StoredEntityNodeKind, string(id)
				if edge {
					kind, key = storage.StoredEntityEdgeKind, string(eid)
				}
				suppressor := manualSuppressionEngine(leaf)
				require.NotNil(t, suppressor)
				require.NoError(t, suppressor.SetManualSuppression(kind, key, false))
				assertHidden := func(hidden bool) {
					t.Helper()
					if edge {
						edges, err := leaf.GetOutgoingEdges(id)
						require.NoError(t, err)
						found := false
						for _, candidate := range edges {
							found = found || candidate.ID == eid
						}
						require.Equal(t, !hidden, found)
						raw, err := storage.GetStoredEdge(leaf, eid)
						require.NoError(t, err)
						require.Equal(t, "retained", raw.Properties["payload"])
					} else {
						_, err := leaf.GetNode(id)
						if hidden {
							require.ErrorIs(t, err, storage.ErrNotFound)
						} else {
							require.NoError(t, err)
						}
						raw, err := storage.GetStoredNode(leaf, id)
						require.NoError(t, err)
						require.Equal(t, "retained", raw.Properties["payload"])
					}
				}
				entity, match := "n", "MATCH (n:VisibilityProof {run:$run})"
				if edge {
					entity, match = "r", "MATCH ()-[r:VisibilityProofLink {run:$run}]->()"
				}
				fragment := "CALL { USE comp.one " + match + " CALL kp.suppress(" + entity + ") RETURN 1 AS w }"
				writer := "CALL { USE comp.two CREATE (:AcAtom {run:$run}) RETURN 1 AS z }"
				query := fragment + " RETURN w"
				switch completion {
				case "failure":
					query = fragment + " CALL { USE comp.two RETURN 1 / $zero AS z } RETURN w,z"
				case "second_writer_before":
					query = writer + " " + fragment + " RETURN w,z"
				case "second_writer_after":
					query = fragment + " " + writer + " RETURN w,z"
				}
				e := f.compositeSession(t)
				explicit := completion == "commit" || completion == "rollback"
				if explicit {
					_, err = e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err = e.Execute(ctx, query, map[string]interface{}{"run": run, "zero": int64(0)})
				switch completion {
				case "failure":
					require.Error(t, err)
				case "second_writer_before", "second_writer_after":
					require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
				default:
					require.NoError(t, err)
				}
				if explicit {
					assertHidden(false)
					command := "COMMIT"
					if completion == "rollback" {
						command = "ROLLBACK"
					}
					_, err = e.Execute(ctx, command, nil)
					require.NoError(t, err)
				}
				hidden := false
				if completion == "success" || completion == "commit" {
					hidden = !hidden
				}
				assertHidden(hidden)
				require.Zero(t, f.writes(t, "two", run))
			})
		}
	}
}

func TestFabricKnowledgePolicyRefusalKeepsPhysicalSchema(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	leaf, err := f.mgr.GetStorage("one")
	require.NoError(t, err)
	for name, ddl := range map[string]string{
		"decay":     "CREATE DECAY PROFILE rejected_decay OPTIONS {halfLifeSeconds:3600,function:'exponential',scope:'NODE',scoreFrom:'CREATED'}",
		"promotion": "CREATE PROMOTION PROFILE rejected_promotion OPTIONS {multiplier:1.5}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := f.exec.Execute(context.Background(), "CALL { USE comp.two CREATE (:AcAtom {run:$run}) RETURN 1 AS z } CALL { USE comp.one CALL apoc.cypher.doIt($ddl,{}) YIELD value RETURN 1 AS w } RETURN z,w", map[string]interface{}{"run": t.Name(), "ddl": ddl})
			require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
			bundles, bindings := leaf.GetSchema().ShowDecayProfiles()
			for _, bundle := range bundles {
				require.NotEqual(t, "rejected_decay", bundle.Name)
			}
			require.Empty(t, bindings)
			for _, profile := range leaf.GetSchema().ShowPromotionProfiles() {
				require.NotEqual(t, "rejected_promotion", profile.Name)
			}
			require.Zero(t, f.writes(t, "two", t.Name()))
		})
	}
}

func TestFabricRevealFindsSuppressedCandidates(t *testing.T) {
	ctx := context.Background()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	mgr, err := multidb.NewDatabaseManager(ctx, engine, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.NoError(t, mgr.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "one"}))
	require.NoError(t, mgr.CreateCompositeDatabase(ctx, "comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"}}))
	base, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(base)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	f := &fabricAtomicityFixture{mgr: mgr, exec: exec}
	leaf, err := f.mgr.GetStorage("one")
	require.NoError(t, err)
	_, err = leaf.CreateNode(&storage.Node{ID: "reveal-target", Labels: []string{"RevealProof"}, Properties: map[string]interface{}{"key": "target"}})
	require.NoError(t, err)
	require.NoError(t, manualSuppressionEngine(leaf).SetManualSuppression(storage.StoredEntityNodeKind, "reveal-target", true))
	for name, query := range map[string]string{
		"direct":   "MATCH (n:RevealProof {key:'target'}) RETURN reveal(n).key AS key",
		"selected": "USE comp.one MATCH (n:RevealProof {key:'target'}) RETURN reveal(n).key AS key",
		"fragment": "CALL { USE comp.one MATCH (n:RevealProof {key:'target'}) RETURN reveal(n).key AS key } RETURN key",
	} {
		t.Run(name, func(t *testing.T) {
			e := f.exec
			if name == "direct" {
				e = NewStorageExecutor(leaf)
			}
			result, err := e.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"target"}}, result.Rows)
		})
	}
	_, err = f.exec.Execute(ctx, "CALL { USE comp.one MATCH (n:RevealProof {key:'target'}) WHERE reveal(n) IS NOT NULL CALL kp.unsuppress(n) RETURN 1 AS w } RETURN w", nil)
	require.NoError(t, err)
	node, err := leaf.GetNode("reveal-target")
	require.NoError(t, err)
	require.Equal(t, "target", node.Properties["key"])

}
