package bolt

// gh745_review_fixes_test.go — driver-decoded Bolt 5.0 entity tests for the
// PR #768 review: the neo4j-go-driver negotiates 5.0 through a range
// proposal, then decodes the 8-field relationship (B8 52), a path whose
// unbound relationships carry element ids (B4 72), and a node nested in a
// map — all equal to the elementId() the query engine returns.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4jdriverdb "github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGh745_RealDriverBolt5Entities(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": storage.NewNamespacedEngine(base, "nornic"),
		},
		defaultDB: "nornic",
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)

	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, driver.Close(context.Background()))
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	defer func() {
		require.NoError(t, session.Close(ctx))
	}()

	// The driver proposes version ranges; the server must reply with a
	// concrete selected version and the session must end up on Bolt 5.0.
	sumResult, err := session.Run(ctx, "RETURN 1 AS x", nil)
	require.NoError(t, err)
	summary, err := sumResult.Consume(ctx)
	require.NoError(t, err)
	require.Equal(t, neo4jdriverdb.ProtocolVersion{Major: 5, Minor: 0}, summary.Server().ProtocolVersion(), "a range offer must negotiate Bolt 5.0")

	createResult, err := session.Run(ctx, "CREATE (a:V5 {k: 1})-[:R5 {w: 2}]->(b:V5 {k: 2})", nil)
	require.NoError(t, err)
	_, err = createResult.Consume(ctx)
	require.NoError(t, err)

	result, err := session.Run(ctx,
		"MATCH p = (a:V5 {k: 1})-[r:R5]->(b) RETURN r, p, {n: a} AS m", nil)
	require.NoError(t, err)
	require.True(t, result.Next(ctx), "expected a path row")
	record := result.Record()

	// Relationship: the driver decodes the 8-field Bolt 5.0 structure with
	// the relationship and both endpoint element ids.
	relValue, ok := record.Get("r")
	require.True(t, ok)
	rel, ok := relValue.(neo4jdriver.Relationship)
	require.True(t, ok, "relationship value type %T", relValue)
	require.NotEmpty(t, rel.ElementId, "relationship element id must be populated")
	require.True(t, strings.HasPrefix(rel.ElementId, "5:nornic:"), "relationship element id %q", rel.ElementId)
	require.True(t, strings.HasPrefix(rel.StartElementId, "4:nornic:"), "start element id %q", rel.StartElementId)
	require.True(t, strings.HasPrefix(rel.EndElementId, "4:nornic:"), "end element id %q", rel.EndElementId)

	// Path: its unbound relationships carry the same element id (B4 72).
	pathValue, ok := record.Get("p")
	require.True(t, ok)
	path, ok := pathValue.(neo4jdriver.Path)
	require.True(t, ok, "path value type %T", pathValue)
	require.Len(t, path.Nodes, 2)
	require.Len(t, path.Relationships, 1)
	require.Equal(t, rel.ElementId, path.Relationships[0].ElementId, "path relationship must round-trip its element id")
	require.Equal(t, rel.StartElementId, path.Nodes[0].ElementId)
	require.Equal(t, rel.EndElementId, path.Nodes[1].ElementId)

	// A node nested in a map decodes as a Bolt 5.0 node with its element id.
	mapValue, ok := record.Get("m")
	require.True(t, ok)
	nestedMap, ok := mapValue.(map[string]any)
	require.True(t, ok, "nested map value type %T", mapValue)
	nestedNode, ok := nestedMap["n"].(neo4jdriver.Node)
	require.True(t, ok, "nested node type %T", nestedMap["n"])
	require.Equal(t, rel.StartElementId, nestedNode.ElementId, "nested node must carry the same element id as the top-level node")

	// elementId() in Cypher names the same entity, and the id round-trips.
	idResult, err := session.Run(ctx,
		"MATCH (a:V5 {k: 1})-[r:R5]->() RETURN elementId(a) AS ea, elementId(r) AS er", nil)
	require.NoError(t, err)
	require.True(t, idResult.Next(ctx))
	idRecord := idResult.Record()
	ea, _ := idRecord.Get("ea")
	er, _ := idRecord.Get("er")
	require.Equal(t, ea, rel.StartElementId)
	require.Equal(t, er, rel.ElementId)

	lookupResult, err := session.Run(ctx,
		"MATCH (n:V5) WHERE elementId(n) = $e RETURN n.k AS k", map[string]any{"e": rel.StartElementId})
	require.NoError(t, err)
	require.True(t, lookupResult.Next(ctx))
	k, _ := lookupResult.Record().Get("k")
	require.Equal(t, int64(1), k, "the driver-returned element id must find the node")
}
