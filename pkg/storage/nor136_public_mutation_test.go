// SPDX-License-Identifier: MIT
package storage_test

// NOR136/NOR143 public mutation acceptance: the acknowledged-write mutation
// matrix over the real public Execute, Bolt and HTTP transaction boundaries.
//
// The matrix is derived from NOR136 test matrix #1/#2/#4 and the NOR143
// unique-key transition rows: constraint enforcement against an accepted
// holder, key release by relabel/rename, reuse of the released key without any
// intervening flush, cascade deletion, label-scoped uniqueness, and the same
// public reads and rejections after a close/reopen of the same store. It runs
// for write-behind off and on, for an autocommit and an explicit producer, over
// each public route (nor136ProtocolRoutes). The Cypher parser is process-wide
// (config.init), so both parsers run these identical statements in separate
// NORNICDB_PARSER=nornic and NORNICDB_PARSER=antlr runs of this test;
// nor136Parsers records which parser a run actually used.
//
// Fixture: protocolServerFixture owns the directory, the real Badger engine and
// the real server, and pr1655OpenPublicClient owns the transport adapters, so
// this file adds no server, engine or transport fixture of its own. The seed
// writes the initial graph and the Doc(key) UNIQUE constraint through the
// production Cypher executor and then flushes, so the fixture graph is durable
// and the holder claims are enforced by the production constraint owner:
//
//	a  (:Doc {key:'held', old:'value', peer:'kept'})
//	b  (:Doc {key:'delete'})
//	c  (:Doc {key:'peer'})
//	a-[:LINK]->b  (cascaded by deleting b)
//	a-[:LINK]->c  (must survive)
//
// Public sequence, with no test-driven flush between the release and the reuse:
//
//  1. the actual Doc holder rejects a second Doc holding 'held' with the
//     canonical constraint status;
//  2. the holder relabels to Parked (SET/REMOVE labels) and renames value
//     through renamed = old before old is removed;
//  3. without an explicit intervening flush, a new Doc claims the released key;
//  4. a second actual duplicate is rejected, now by the replacement holder;
//  5. the 'delete' Doc is deleted and its incident edge is cascaded away.
//
// Expected end state, asserted identically through public reads and through the
// native engine both before and after the reopen: Doc=2, Parked=1, total
// relevant label cardinality 3, one surviving edge a->c, b absent, the
// preserved peer property intact, a.renamed='value' with a.old removed, and the
// unconstrained Parked holder allowed to share 'held' with the constrained Doc.
//
// An explicit BEGIN may drain the write-behind buffer as documented, so the
// sequence never adds a flush to keep an acknowledgement buffered; it only
// requires that a released key is reusable and that every rejection is atomic.

import (
	"context"
	"fmt"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor136FixtureSeed is the durable initial graph plus the Doc(key) UNIQUE
// constraint the acceptance needs. The constraint is created before the nodes so
// the holder claims come from the constrained path, and the fixture flushes at
// the end of seed, so a later writer validates against a stored head.
func nor136FixtureSeed() []string {
	return []string{
		"CREATE CONSTRAINT doc_key FOR (n:Doc) REQUIRE n.key IS UNIQUE",
		"CREATE (:Doc {key:'held', old:'value', peer:'kept'})",
		"CREATE (:Doc {key:'delete'})",
		"CREATE (:Doc {key:'peer'})",
		"MATCH (a:Doc {key:'held'}), (b:Doc {key:'delete'}) CREATE (a)-[:LINK]->(b)",
		"MATCH (a:Doc {key:'held'}), (c:Doc {key:'peer'}) CREATE (a)-[:LINK]->(c)",
	}
}

const (
	// nor136DuplicateHolder attempts a further Doc holding the claimed key.
	nor136DuplicateHolder = "CREATE (:Doc {key:'held'})"
	// nor136ReuseReleasedKey claims the released key and reports what it wrote.
	nor136ReuseReleasedKey = "CREATE (n:Doc {key:'held'}) RETURN n.key AS key"
	// nor136RelabelToParked adds the unconstrained label to the holder.
	nor136RelabelToParked = "MATCH (n:Doc {key:'held'}) SET n:Parked"
	// nor136RemoveDocLabel removes the constrained label, releasing the claim.
	nor136RemoveDocLabel = "MATCH (n:Parked {key:'held'}) REMOVE n:Doc"
	// nor136RenameValue copies old into renamed before old is removed.
	nor136RenameValue = "MATCH (n:Parked {key:'held'}) SET n.renamed = n.old"
	// nor136RemoveOldValue removes the renamed-away property.
	nor136RemoveOldValue = "MATCH (n:Parked {key:'held'}) REMOVE n.old"
	// nor136CascadeDelete deletes the 'delete' Doc and its incident edge.
	nor136CascadeDelete = "MATCH (n:Doc {key:'delete'}) DETACH DELETE n"
)

// nor136PublicReads are the exact public reads the acceptance repeats before and
// after the reopen. They are ordinary Cypher and have the same expected rows
// under both parsers.
const (
	nor136ReadDocKeys       = "MATCH (n:Doc) RETURN n.key AS key ORDER BY n.key"
	nor136ReadParkedHolder  = "MATCH (n:Parked) RETURN n.key AS key, n.peer AS peer, n.renamed AS renamed"
	nor136ReadParkedOld     = "MATCH (n:Parked) RETURN n.old AS old"
	nor136ReadDeletedNode   = "MATCH (n {key:'delete'}) RETURN n"
	nor136ReadSurvivingEdge = "MATCH (a:Parked {key:'held'})-[r:LINK]->(c:Doc {key:'peer'}) RETURN c.key AS key"
	nor136ReadCascadedEdge  = "MATCH (a:Parked {key:'held'})-[r:LINK]->(b:Doc {key:'delete'}) RETURN b"
	nor136CountDocNodes     = "MATCH (n:Doc) RETURN count(n) AS count"
	nor136CountParkedNodes  = "MATCH (n:Parked) RETURN count(n) AS count"
	nor136CountHeldKey      = "MATCH (n {key:'held'}) RETURN count(n) AS count"
	nor136CountLinkEdges    = "MATCH ()-[r:LINK]->() RETURN count(r) AS count"
)

// nor136PublicMutation runs fixture statements through one public client at the
// producer boundary under test: one explicit transaction per step, or one
// autocommit statement per statement. It also owns the explicit rollback so a
// statement the store rejects leaves the client reusable for the next
// transaction.
type nor136PublicMutation struct {
	t        *testing.T
	client   *pr1655PublicClient
	explicit bool
}

// mutate runs statements as one explicit transaction (explicit producer) or as
// autocommit statements, and returns the rows the last statement produced. It
// never flushes: an explicit BEGIN may drain the write-behind buffer as
// documented, and the schedule must not depend on a test-driven flush.
func (m *nor136PublicMutation) mutate(statements ...string) [][]interface{} {
	m.t.Helper()
	if m.explicit {
		require.NoError(m.t, m.client.begin())
		var rows [][]interface{}
		for _, statement := range statements {
			got, err := m.client.run(statement)
			if err != nil {
				m.client.rollback()
				require.NoError(m.t, err, statement)
			}
			rows = got
		}
		require.NoError(m.t, m.client.commit())
		return rows
	}
	var rows [][]interface{}
	for _, statement := range statements {
		got, err := m.client.run(statement)
		require.NoError(m.t, err, statement)
		rows = got
	}
	return rows
}

// rejected runs a statement the store must reject and returns its error. An
// explicit transaction is rolled back with it, so the next transaction starts
// from a clean client state.
func (m *nor136PublicMutation) rejected(statement string) error {
	m.t.Helper()
	if m.explicit {
		require.NoError(m.t, m.client.begin())
		_, err := m.client.run(statement)
		m.client.rollback()
		require.Error(m.t, err, statement)
		return err
	}
	_, err := m.client.run(statement)
	require.Error(m.t, err, statement)
	return err
}

// nor136PublicRows runs one read statement through the public client.
func nor136PublicRows(t *testing.T, client *pr1655PublicClient, statement string) [][]interface{} {
	t.Helper()
	rows, err := client.run(statement)
	require.NoError(t, err, statement)
	return rows
}

// nor136PublicCount reads one count row. Execute and Bolt carry the scalar as
// int64 while the HTTP route carries it as a JSON number, so the wire scalar is
// normalised to int64; the count and its expected value are unchanged.
func nor136PublicCount(t *testing.T, client *pr1655PublicClient, statement string) int64 {
	t.Helper()
	rows := nor136PublicRows(t, client, statement)
	require.Len(t, rows, 1, statement)
	require.Len(t, rows[0], 1, statement)
	switch value := rows[0][0].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case float64:
		require.Equal(t, float64(int64(value)), value, "%s: a count must be a whole number", statement)
		return int64(value)
	default:
		t.Fatalf("%s: count value has unexpected type %T", statement, rows[0][0])
		return 0
	}
}

// nor136RequirePublicGraph asserts the exact public graph state the mutation
// sequence must produce. The write phase and the reopen phase both call it, so
// the reads and their expected semantics are identical for both parsers and all
// routes.
func nor136RequirePublicGraph(t *testing.T, client *pr1655PublicClient) {
	t.Helper()
	docs := nor136PublicCount(t, client, nor136CountDocNodes)
	parked := nor136PublicCount(t, client, nor136CountParkedNodes)
	require.Equal(t, int64(2), docs, "one replacement Doc plus the untouched peer Doc")
	require.Equal(t, int64(1), parked, "the relabeled holder is the only Parked node")
	require.Equal(t, int64(3), docs+parked, "total relevant label cardinality")
	require.Equal(t, int64(2), nor136PublicCount(t, client, nor136CountHeldKey),
		"the constrained Doc and the unconstrained Parked holder share the key once each")
	require.Equal(t, int64(1), nor136PublicCount(t, client, nor136CountLinkEdges),
		"only the a->c edge survives the cascade")

	require.Equal(t, [][]interface{}{{"held"}, {"peer"}}, nor136PublicRows(t, client, nor136ReadDocKeys),
		"the surviving Docs are the replacement holder and the untouched peer")
	require.Equal(t, [][]interface{}{{"held", "kept", "value"}}, nor136PublicRows(t, client, nor136ReadParkedHolder),
		"the holder keeps its key and peer property and reports the renamed value")
	require.Equal(t, [][]interface{}{{nil}}, nor136PublicRows(t, client, nor136ReadParkedOld),
		"the renamed-away property is removed")
	require.Empty(t, nor136PublicRows(t, client, nor136ReadDeletedNode), "the deleted Doc is gone")
	require.Empty(t, nor136PublicRows(t, client, nor136ReadCascadedEdge),
		"the cascade removed the deleted Doc's incident edge")
	require.Equal(t, [][]interface{}{{"peer"}}, nor136PublicRows(t, client, nor136ReadSurvivingEdge),
		"the surviving edge still points at the peer Doc")
}

// nor136PublicFixtureIDs are the stored identities of the seeded fixture nodes
// plus the replacement Doc that reuses the released key.
type nor136PublicFixtureIDs struct {
	holder      storage.NodeID
	deleted     storage.NodeID
	peer        storage.NodeID
	replacement storage.NodeID
}

// nor136RequireNativeGraph asserts the same outcome on the native engine: the
// current label counts, the preserved properties, the expected surviving node
// identities and the cascade. It returns the surviving edge identity so the
// reopen phase can require that the same edge survived the restart.
func nor136RequireNativeGraph(t *testing.T, f *protocolServerFixture, ids nor136PublicFixtureIDs) storage.EdgeID {
	t.Helper()
	ctx := context.Background()

	docs, err := f.engine.NodeCountByLabelInNamespace(ctx, "nornic", "Doc")
	require.NoError(t, err)
	parked, err := f.engine.NodeCountByLabelInNamespace(ctx, "nornic", "Parked")
	require.NoError(t, err)
	require.Equal(t, int64(2), docs, "the native Doc count must match the public count")
	require.Equal(t, int64(1), parked, "the native Parked count must match the public count")
	require.Equal(t, int64(3), docs+parked, "total relevant label cardinality")

	holder, err := f.engine.GetNode(ctx, ids.holder)
	require.NoError(t, err, "the relabeled holder must survive")
	require.Equal(t, []string{"Parked"}, holder.Labels, "the holder left the constrained Doc label")
	require.Equal(t, "held", holder.Properties["key"])
	require.Equal(t, "kept", holder.Properties["peer"], "the preserved peer property was lost")
	require.Equal(t, "value", holder.Properties["renamed"], "the renaming lost the copied value")
	require.NotContains(t, holder.Properties, "old", "the renamed-away property must be removed")

	peer, err := f.engine.GetNode(ctx, ids.peer)
	require.NoError(t, err, "the untouched peer Doc must survive")
	require.Equal(t, []string{"Doc"}, peer.Labels)
	require.Equal(t, "peer", peer.Properties["key"])

	replacement, err := f.engine.GetNode(ctx, ids.replacement)
	require.NoError(t, err, "the replacement holder must survive")
	require.Equal(t, []string{"Doc"}, replacement.Labels)
	require.Equal(t, "held", replacement.Properties["key"])

	deleted, err := f.engine.GetNode(ctx, ids.deleted)
	require.Error(t, err, "the deleted Doc must not survive")
	require.Nil(t, deleted)

	edges, err := f.engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: ids.holder})
	require.NoError(t, err)
	require.Len(t, edges, 1, "the cascade removes the a->b edge and keeps the a->c edge")
	require.Equal(t, "LINK", edges[0].Type)
	require.Equal(t, ids.holder, edges[0].StartNode)
	require.Equal(t, ids.peer, edges[0].EndNode, "the surviving edge must still point at the peer Doc")
	return edges[0].ID
}

// TestNOR136PublicMutationRestartMatrix is the public NOR136/NOR143 mutation
// acceptance: write-behind off/on, an autocommit and an explicit producer, over
// Execute, Bolt and HTTP, each asserting the public read surface and the native
// engine before a close/reopen and again after it.
func TestNOR136PublicMutationRestartMatrix(t *testing.T) {
	nor136Parsers(t)
	for _, writeBehind := range []bool{false, true} {
		for _, producer := range []string{"autocommit", "explicit"} {
			for _, route := range nor136ProtocolRoutes() {
				t.Run(fmt.Sprintf("writebehind=%t/%s/%s", writeBehind, producer, route), func(t *testing.T) {
					explicit := producer == "explicit"
					// The stopped background worker keeps the acknowledged writes
					// buffered until an explicit drain, so autocommit holds its
					// acknowledgements for the whole write phase.
					f := openProtocolServerFixture(t, protocolServerOptions{writeBehind: writeBehind, flushInterval: time.Hour, manualDrain: true})
					f.seed(t, nor136FixtureSeed()...)
					ids := nor136PublicFixtureIDs{
						holder:  nor136NodeIDByProperty(t, f.engine, "Doc", "key", "held"),
						deleted: nor136NodeIDByProperty(t, f.engine, "Doc", "key", "delete"),
						peer:    nor136NodeIDByProperty(t, f.engine, "Doc", "key", "peer"),
					}
					var survivingEdge storage.EdgeID

					// The write phase owns its clients. The nested t.Run ends
					// before f.close/f.start, so the Bolt peer listener and the
					// HTTP client are released before the store is reopened.
					t.Run("write", func(t *testing.T) {
						client := pr1655OpenPublicClient(t, f.server, route)
						mutation := &nor136PublicMutation{t: t, client: client, explicit: explicit}

						// 1. The actual Doc holder rejects a duplicate key with
						// the canonical constraint status.
						require.Equal(t, nornicerrors.ConstraintValidationFailed,
							pr1655PublicStatus(mutation.rejected(nor136DuplicateHolder)),
							"the actual Doc holder must reject a second Doc holding its key")

						// 2. The holder relabels to Parked and renames old into
						// renamed before old is removed, releasing the key.
						mutation.mutate(nor136RelabelToParked, nor136RemoveDocLabel, nor136RenameValue, nor136RemoveOldValue)

						// 3. The released key is reusable without an explicit
						// intervening flush.
						require.Equal(t, [][]interface{}{{"held"}}, mutation.mutate(nor136ReuseReleasedKey),
							"the released key must be claimable by a new Doc")

						// 4. A second actual duplicate is rejected, now by the
						// replacement holder.
						require.Equal(t, nornicerrors.ConstraintValidationFailed,
							pr1655PublicStatus(mutation.rejected(nor136DuplicateHolder)),
							"the replacement holder must reject a second Doc holding its key")

						// 5. Deleting the 'delete' Doc cascades its incident edge.
						mutation.mutate(nor136CascadeDelete)

						ids.replacement = nor136NodeIDByProperty(t, f.engine, "Doc", "key", "held")
						require.NotEqual(t, ids.holder, ids.replacement,
							"the released key must be held by a new Doc, not by the relabeled holder")

						nor136RequirePublicGraph(t, client)
						survivingEdge = nor136RequireNativeGraph(t, f, ids)

						// Explicit final drain: every acknowledgement the public
						// statements returned must be durable before the store
						// is closed and reopened.
						require.NoError(t, f.engine.FlushWriteBehind(context.Background()))
					})

					t.Run("reopen", func(t *testing.T) {
						f.close(t)
						f.start(t)
						client := pr1655OpenPublicClient(t, f.server, route)
						mutation := &nor136PublicMutation{t: t, client: client, explicit: explicit}

						nor136RequirePublicGraph(t, client)
						require.Equal(t, survivingEdge, nor136RequireNativeGraph(t, f, ids),
							"the reopened store must keep the same surviving node and edge identities")
						require.Equal(t, nornicerrors.ConstraintValidationFailed,
							pr1655PublicStatus(mutation.rejected(nor136DuplicateHolder)),
							"the persisted constraint must still reject a duplicate of the actual holder")
					})
				})
			}
		}
	}
}
