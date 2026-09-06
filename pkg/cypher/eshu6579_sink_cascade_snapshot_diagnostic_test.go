// SPDX-License-Identifier: MIT
// Deterministic diagnostic for Eshu #6579. It is a test-only race harness, not a fix.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// eshu6579AfterOutgoingEngine delegates all production storage operations to a
// real transaction wrapper. It invokes after only after the bound DELETE has
// obtained its outgoing candidates, which places the peer's public DETACH
// DELETE in the narrow read-to-delete interval under investigation.
type eshu6579AfterOutgoingEngine struct {
	storage.Engine
	after   func([]*storage.Edge) error
	fired   bool
	hookErr error
}

func (e *eshu6579AfterOutgoingEngine) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	edges, err := e.Engine.GetOutgoingEdges(nodeID)
	if err != nil || e.fired {
		return edges, err
	}
	e.fired = true
	// The traversal fallback can normalize an adjacency-read error. Preserve
	// any barrier assertion out-of-band so the test cannot false-green.
	e.hookErr = e.after(edges)
	return edges, nil
}

func TestEshu6579BoundDeleteSnapshotSurvivesPeerEndpointCascade(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		deletedUIDKey string
	}{
		{name: "peer_detach_deletes_source", deletedUIDKey: "source"},
		{name: "peer_detach_deletes_sink", deletedUIDKey: "sink"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			defer func() { require.NoError(t, base.Close()) }()

			const namespace = "eshu6579"
			store := storage.NewNamespacedEngine(base, namespace)
			exec := NewStorageExecutor(store)
			// Keep fixture values free of parser keywords so the test targets
			// the production relationship-delete route.
			prefix := "eshu6579-cascade-" + tc.deletedUIDKey
			uids := map[string]string{
				"source":  prefix + "-source",
				"sink":    prefix + "-sink",
				"control": prefix + "-control",
			}
			const evidence = "eshu6579-snapshot-evidence"

			_, err = exec.Execute(ctx, "CREATE INDEX eshu6579_function_uid IF NOT EXISTS FOR (f:Function) ON (f.uid)", nil)
			require.NoError(t, err)
			for _, uid := range uids {
				_, err = exec.Execute(ctx, "CREATE (:Function {uid:$uid})", map[string]interface{}{"uid": uid})
				require.NoError(t, err)
			}
			_, err = exec.Execute(ctx, `
MATCH (s:Function {uid:$source})
MATCH (t:Function {uid:$sink})
CREATE (s)-[:TAINT_FLOWS_TO {evidence_source:$evidence}]->(t)`, map[string]interface{}{"source": uids["source"], "sink": uids["sink"], "evidence": evidence})
			require.NoError(t, err)
			_, err = exec.Execute(ctx, `
MATCH (s:Function {uid:$source})
MATCH (c:Function {uid:$control})
CREATE (s)-[:TAINT_FLOWS_TO {evidence_source:'unaffected'}]->(c)`, map[string]interface{}{"source": uids["source"], "control": uids["control"]})
			require.NoError(t, err)

			require.NoError(t, base.EnsureNamespaceMVCC(namespace))
			tx, err := base.BeginTransaction()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			require.NoError(t, tx.SetNamespace(namespace))
			txStore := &transactionStorageWrapper{tx: tx, underlying: store, namespace: namespace, separator: ":", mutatedNodeIDs: make(map[string]struct{})}

			var snapshotEdge *storage.Edge
			intercept := &eshu6579AfterOutgoingEngine{Engine: txStore}
			intercept.after = func(candidates []*storage.Edge) error {
				for _, edge := range candidates {
					if edge != nil && fmt.Sprint(edge.Properties["evidence_source"]) == evidence {
						snapshotEdge = edge
						break
					}
				}
				if snapshotEdge == nil {
					return fmt.Errorf("exact DELETE did not enumerate the evidence edge")
				}

				// Execute the competitor through the public routed Cypher path,
				// rather than touching storage directly.
				_, peerErr := exec.Execute(ctx, `
UNWIND $uids AS uid
MATCH (f:Function {uid:uid})
DETACH DELETE f`, map[string]interface{}{"uids": []string{uids[tc.deletedUIDKey]}})
				if peerErr != nil {
					return fmt.Errorf("peer public DETACH DELETE: %w", peerErr)
				}

				// A reader that saw the relationship must still resolve its body
				// and both pre-existing endpoints at its pinned snapshot after the
				// peer cascade has committed.
				gotEdge, readErr := txStore.GetEdge(snapshotEdge.ID)
				if readErr != nil {
					return fmt.Errorf("snapshot edge after peer cascade: %w", readErr)
				}
				if gotEdge.StartNode != snapshotEdge.StartNode || gotEdge.EndNode != snapshotEdge.EndNode || fmt.Sprint(gotEdge.Properties["evidence_source"]) != evidence {
					return fmt.Errorf("snapshot edge body changed after peer cascade")
				}
				for endpoint, nodeID := range map[string]storage.NodeID{
					"source": snapshotEdge.StartNode,
					"sink":   snapshotEdge.EndNode,
				} {
					if _, readErr = txStore.GetNode(nodeID); readErr != nil {
						return fmt.Errorf("snapshot %s endpoint after peer cascade: %w", endpoint, readErr)
					}
				}
				return nil
			}

			// Do not place txStore in ctxKeyTxStorage: getStorage would then
			// bypass the interceptor. The receiver still delegates to the same
			// real Badger transaction wrapper, so this executes the production
			// single-MATCH bound-delete body with its real snapshot semantics.
			txExec := exec.cloneWithStorage(intercept)
			result, err := txExec.executeDelete(ctx, `
MATCH (s:Function {uid:'`+uids["source"]+`'})-[rel:TAINT_FLOWS_TO]->(:Function)
WHERE rel.evidence_source = '`+evidence+`'
DELETE rel`)
			require.NoError(t, intercept.hookErr, "barrier assertions must not be swallowed by traversal routing")
			require.NoError(t, err, "enumerated snapshot edge must not become ErrNotFound before BulkDeleteEdges")

			require.NotNil(t, snapshotEdge)
			require.Equal(t, 1, result.Stats.RelationshipsDeleted)

			// The peer changed the edge and one endpoint after this transaction's
			// read version, so the only valid terminal result is a retryable
			// commit conflict, not a missing-edge statement error.
			commitErr := tx.Commit()
			require.ErrorIs(t, commitErr, storage.ErrConflict)
			require.Contains(t, commitErr.Error(), "changed after transaction start")

			remaining, err := exec.Execute(ctx, `
MATCH (:Function {uid:$source})-[rel:TAINT_FLOWS_TO {evidence_source:$evidence}]->(:Function)
RETURN count(rel)`, map[string]interface{}{"source": uids["source"], "evidence": evidence})
			require.NoError(t, err)
			require.Equal(t, int64(0), remaining.Rows[0][0], "peer cascade owns the committed terminal graph state")
		})
	}
}
