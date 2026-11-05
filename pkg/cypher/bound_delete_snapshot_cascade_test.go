// SPDX-License-Identifier: MIT
// Bound-delete snapshot/cascade regression: the production single-MATCH bound
// DELETE reads its candidates and applies its delete in one transaction. When a
// peer's public DETACH DELETE cascades the enumerated relationship and one of
// its endpoints in that read-to-apply interval, the only valid terminal result
// is a retryable commit conflict — never a missing-edge statement error — and
// the peer's committed cascade owns the terminal graph.
package cypher

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	boundDeleteEvidence  = "snapshot-evidence"
	boundDeleteNamespace = "snapshot_cascade"
)

// boundDeleteFixture is one seeded graph: an evidence edge (source → sink) plus
// a control edge out of the same source node.
type boundDeleteFixture struct {
	base   *storage.BadgerEngine
	exec   *StorageExecutor
	peer   *StorageExecutor
	params map[string]interface{}
	uids   map[string]string
}

func newBoundDeleteFixture(t *testing.T, deletedUIDKey string) *boundDeleteFixture {
	t.Helper()
	ctx := context.Background()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if base != nil {
		fixtureowner.Own(t, base)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })

	store := storage.NewNamespacedEngine(base, boundDeleteNamespace)
	exec := NewStorageExecutor(store)
	// Keep fixture values free of parser keywords so the test targets the
	// production relationship-delete route.
	prefix := "bound-delete-cascade-" + deletedUIDKey
	uids := map[string]string{
		"source":  prefix + "-source",
		"sink":    prefix + "-sink",
		"control": prefix + "-control",
	}

	_, err = exec.Execute(ctx, "CREATE INDEX snapshot_cascade_function_uid IF NOT EXISTS FOR (f:Function) ON (f.uid)", nil)
	require.NoError(t, err)
	for _, uid := range uids {
		_, err = exec.Execute(ctx, "CREATE (:Function {uid:$uid})", map[string]interface{}{"uid": uid})
		require.NoError(t, err)
	}
	_, err = exec.Execute(ctx, `
MATCH (s:Function {uid:$source})
MATCH (t:Function {uid:$sink})
CREATE (s)-[:TAINT_FLOWS_TO {evidence_source:$evidence}]->(t)`, map[string]interface{}{"source": uids["source"], "sink": uids["sink"], "evidence": boundDeleteEvidence})
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (s:Function {uid:$source})
MATCH (c:Function {uid:$control})
CREATE (s)-[:TAINT_FLOWS_TO {evidence_source:'unaffected'}]->(c)`, map[string]interface{}{"source": uids["source"], "control": uids["control"]})
	require.NoError(t, err)

	return &boundDeleteFixture{
		base:   base,
		exec:   exec,
		peer:   NewStorageExecutor(store),
		params: map[string]interface{}{"source": uids["source"], "evidence": boundDeleteEvidence},
		uids:   uids,
	}
}

// peerCascade runs the competitor through the public routed Cypher path.
func (f *boundDeleteFixture) peerCascade(deletedUIDKey string) error {
	_, err := f.peer.Execute(context.Background(), `
UNWIND $uids AS uid
MATCH (f:Function {uid:uid})
DETACH DELETE f`, map[string]interface{}{"uids": []string{f.uids[deletedUIDKey]}})
	if err != nil {
		return fmt.Errorf("peer public DETACH DELETE: %w", err)
	}
	return nil
}

func (f *boundDeleteFixture) countEvidence(t *testing.T) int64 {
	t.Helper()
	return f.countByEvidence(t, boundDeleteEvidence)
}

func (f *boundDeleteFixture) countControl(t *testing.T) int64 {
	t.Helper()
	return f.countByEvidence(t, "unaffected")
}

func (f *boundDeleteFixture) countByEvidence(t *testing.T, evidence string) int64 {
	t.Helper()
	result, err := f.exec.Execute(context.Background(), `
MATCH (:Function {uid:$source})-[rel:TAINT_FLOWS_TO {evidence_source:$evidence}]->(:Function)
RETURN count(rel)`, map[string]interface{}{"source": f.uids["source"], "evidence": evidence})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return result.Rows[0][0].(int64)
}

// boundDeleteQuery is the production single-MATCH bound delete. The gate sits in
// the WHERE predicate, so its evaluation is the statement's read-to-apply
// interval: the candidates are read and nothing is deleted yet.
const boundDeleteQuery = `
MATCH (s:Function {uid:$source})-[rel:TAINT_FLOWS_TO]->(:Function)
WHERE rel.evidence_source = $evidence AND ` + statementGateFunction + `(rel.evidence_source) IS NOT NULL
DELETE rel`

// An implicit-transaction bound DELETE that loses its relationship to a peer
// cascade ends in a retryable commit conflict, not a missing-edge error.
func TestBoundDeleteSnapshotSurvivesPeerEndpointCascade(t *testing.T) {
	for _, tc := range []struct {
		name          string
		deletedUIDKey string
		// controlAfter is what the peer's own cascade leaves of the control
		// edge: deleting the source node cascades both relationships.
		controlAfter int64
	}{
		{name: "peer_detach_deletes_source", deletedUIDKey: "source", controlAfter: 0},
		{name: "peer_detach_deletes_sink", deletedUIDKey: "sink", controlAfter: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newBoundDeleteFixture(t, tc.deletedUIDKey)
			held := newStatementGate(func(context.Context) error {
				// The statement read its candidates and applied nothing yet, so a
				// committed reader still sees the exact evidence relationship.
				if got := fixture.countEvidence(t); got != 1 {
					return fmt.Errorf("bound DELETE reached its gate after the relationship left committed storage: %d", got)
				}
				return fixture.peerCascade(tc.deletedUIDKey)
			}).install(t)

			result, err := fixture.exec.Execute(context.Background(), boundDeleteQuery, fixture.params)
			require.Equal(t, int64(1), held.Calls(), "the bound DELETE must evaluate its gate exactly once")
			require.NoError(t, held.Err(), "the interleaving assertions must reach the case")

			// The peer changed the relationship and one endpoint after this
			// transaction's read version, so the only valid terminal result is a
			// retryable commit conflict, not a missing-edge statement error.
			require.Error(t, err, "a peer cascade after the statement's read must not commit the bound DELETE")
			require.ErrorIs(t, err, storage.ErrConflict)
			require.NotErrorIs(t, err, storage.ErrNotFound)
			require.Contains(t, err.Error(), "changed after transaction start")
			require.Nil(t, result, "a conflicted statement returns no result")

			// The peer cascade owns the committed terminal graph; the rolled-back
			// statement added no deletion of its own.
			require.Zero(t, fixture.countEvidence(t), "peer cascade owns the committed terminal graph state")
			require.Equal(t, tc.controlAfter, fixture.countControl(t), "only the peer's cascade touched the control relationship")
		})
	}
}

// The sensitivity control for the case above: the same bound DELETE, with no
// competitor in its read-to-apply interval, enumerates and deletes its
// relationship.
func TestBoundDeleteAppliesEnumeratedRelationshipWithoutPeerCascade(t *testing.T) {
	fixture := newBoundDeleteFixture(t, "source")
	held := newStatementGate(nil).install(t)

	result, err := fixture.exec.Execute(context.Background(), boundDeleteQuery, fixture.params)
	require.Equal(t, int64(1), held.Calls(), "the bound DELETE must evaluate its gate exactly once")
	require.NoError(t, err, "the bound DELETE applies its enumerated relationship without a competitor")
	require.Equal(t, 1, result.Stats.RelationshipsDeleted)
	require.Zero(t, fixture.countEvidence(t), "the bound DELETE removed the relationship it enumerated")
	require.Equal(t, int64(1), fixture.countControl(t), "the untouched control relationship stays")
}
