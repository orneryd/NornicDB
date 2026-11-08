package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// creationTimeStatements create :Stamp nodes and relationships. Each one but
// the controls edits what it creates in the same statement, in each Cypher
// form that writes an entity again after creating it (NOR-295).
var creationTimeStatements = []string{
	"CREATE (n:Stamp {id: 'control'})",
	"CREATE (:Stamp {id: 'plain-start'})-[:PLAIN]->(:Stamp {id: 'plain-end'})",
	"CREATE (n:Stamp {id: 'set'}) SET n.payload = 'x'",
	"CREATE (n:Stamp {id: 'set-map'}) SET n += {payload: 'x'}",
	"MERGE (n:Stamp {id: 'merge'}) ON CREATE SET n.payload = 'x'",
	"CREATE (n:Stamp {id: 'remove', scratch: 1}) SET n.payload = 'x' REMOVE n.scratch",
	"CREATE (n:Stamp {id: 'label'}) SET n:Labelled, n.payload = 'x'",
	"CREATE (a:Stamp {id: 'start'})-[r:LINKS]->(b:Stamp {id: 'end'}) SET r.payload = 'x', a.payload = 'x', b.payload = 'x'",
	"MERGE (a:Stamp {id: 'merge-start'})-[r:MERGED]->(b:Stamp {id: 'merge-end'}) ON CREATE SET r.payload = 'x', a.payload = 'x', b.payload = 'x'",
}

// creationTimeControls are the entities their statement creates without
// editing them again.
var creationTimeControls = map[string]bool{"control": true, "plain-start": true, "plain-end": true, "PLAIN": true}

// stampImages are the stored :Stamp nodes by id and their relationships by
// type, read from the stored images rather than through query visibility.
type stampImages struct {
	nodes map[string]*storage.Node
	edges map[string]*storage.Edge
}

func readStampImages(t *testing.T, h *hiddenDeleteServer, label string) stampImages {
	t.Helper()
	store, err := h.server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	nodes, err := storage.GetStoredNodesByLabel(t.Context(), store, label)
	require.NoError(t, err)
	images := stampImages{nodes: map[string]*storage.Node{}, edges: map[string]*storage.Edge{}}
	for _, node := range nodes {
		id, _ := node.Properties["id"].(string)
		images.nodes[id] = node
		outgoing, err := storage.GetStoredOutgoingEdges(t.Context(), store, node.ID)
		require.NoError(t, err)
		for _, edge := range outgoing {
			images.edges[edge.Type] = edge
		}
	}
	return images
}

// assertCreatedWithin asserts a creation time stamped during [from, to] and
// an update time that does not precede it. It reports every entity, not only
// the first one that fails.
func assertCreatedWithin(t *testing.T, what string, created, updated, from, to time.Time) {
	t.Helper()
	assert.False(t, created.IsZero(), "%s: CreatedAt is the zero time; UpdatedAt=%s", what, updated)
	assert.False(t, created.Before(from) || created.After(to),
		"%s: CreatedAt %s is outside the request interval [%s, %s]", what, created, from, to)
	assert.False(t, updated.Before(created) || updated.After(to),
		"%s: UpdatedAt %s precedes CreatedAt %s or follows the request interval end %s", what, updated, created, to)
}

// A node or relationship created and edited in one statement keeps the
// creation time its transaction stamped, through every public route, both
// engines and write-behind, after a later update and after a restart; a
// rolled-back statement leaves nothing (NOR-295).
func TestCreateThenEditRetainsCreationTime(t *testing.T) {
	setups := []struct {
		engine      string
		writeBehind bool
	}{{"memory", false}, {"badger", false}, {"badger", true}}
	for _, setup := range setups {
		for _, route := range []string{"http", "http-explicit", "bolt", "bolt-explicit", "embedded"} {
			t.Run(fmt.Sprintf("%s/writeBehind=%t/%s", setup.engine, setup.writeBehind, route), func(t *testing.T) {
				dir := ""
				if setup.engine == "badger" {
					dir = t.TempDir()
				}
				open := func() *hiddenDeleteServer {
					protocol := route
					if route == "embedded" {
						protocol = "http"
					}
					h := openHiddenDeleteServer(t, dir, setup.writeBehind, protocol)
					if route == "embedded" {
						h.run = func(t *testing.T, statement string) hiddenDeleteOutcome {
							t.Helper()
							if _, err := h.db.ExecuteCypher(t.Context(), statement, nil); err != nil {
								return hiddenDeleteOutcome{code: "embedded", message: err.Error()}
							}
							return hiddenDeleteOutcome{}
						}
					}
					return h
				}
				h := open()
				t.Cleanup(func() {
					if h != nil {
						h.shutdown(t)
					}
				})
				settle := func() {
					if setup.writeBehind {
						h.flush(t)
					}
				}

				started := time.Now()
				for _, statement := range creationTimeStatements {
					h.requireSuccess(t, statement)
				}
				finished := time.Now()
				settle()

				created := readStampImages(t, h, "Stamp")
				require.Len(t, created.nodes, 12)
				require.Len(t, created.edges, 3)
				for id, node := range created.nodes {
					assertCreatedWithin(t, "node "+id, node.CreatedAt, node.UpdatedAt, started, finished)
					if !creationTimeControls[id] {
						require.Equal(t, "x", node.Properties["payload"], "node %s keeps its same-statement edit", id)
					}
				}
				require.NotContains(t, created.nodes["remove"].Properties, "scratch")
				require.Contains(t, created.nodes["label"].Labels, "Labelled")
				for edgeType, edge := range created.edges {
					assertCreatedWithin(t, "relationship "+edgeType, edge.CreatedAt, edge.UpdatedAt, started, finished)
					if !creationTimeControls[edgeType] {
						require.Equal(t, "x", edge.Properties["payload"], "relationship %s keeps its same-statement edit", edgeType)
					}
				}
				require.False(t, t.Failed(), "every created entity keeps its creation time")

				// A later update advances only the update time.
				laterFrom := time.Now()
				h.requireSuccess(t, "MATCH (n:Stamp) SET n.later = 1")
				h.requireSuccess(t, "MATCH (:Stamp)-[r]->(:Stamp) SET r.later = 1")
				laterTo := time.Now()
				settle()
				updated := readStampImages(t, h, "Stamp")
				require.Len(t, updated.nodes, len(created.nodes))
				require.Len(t, updated.edges, len(created.edges))
				for id, node := range updated.nodes {
					require.True(t, created.nodes[id].CreatedAt.Equal(node.CreatedAt), "node %s: a later update keeps CreatedAt %s, got %s", id, created.nodes[id].CreatedAt, node.CreatedAt)
					require.False(t, node.UpdatedAt.Before(laterFrom) || node.UpdatedAt.After(laterTo), "node %s: the later update advances UpdatedAt", id)
					require.EqualValues(t, 1, node.Properties["later"])
				}
				for edgeType, edge := range updated.edges {
					require.True(t, created.edges[edgeType].CreatedAt.Equal(edge.CreatedAt), "relationship %s: a later update keeps CreatedAt %s, got %s", edgeType, created.edges[edgeType].CreatedAt, edge.CreatedAt)
					require.False(t, edge.UpdatedAt.Before(laterFrom) || edge.UpdatedAt.After(laterTo), "relationship %s: the later update advances UpdatedAt", edgeType)
					require.EqualValues(t, 1, edge.Properties["later"])
				}

				// A statement that fails after creating and editing leaves nothing.
				failed := h.run(t, "CREATE (n:Rolled {id: 'rolled'}) SET n.payload = 'x' RETURN 1 / (size(n.payload) - 1) AS failure")
				require.NotEmpty(t, failed.code, "the division by zero fails the statement")
				settle()
				require.Empty(t, readStampImages(t, h, "Rolled").nodes, "a failed statement stores no node")

				if dir == "" {
					return
				}
				h.shutdown(t)
				h = nil
				h = open()
				reopened := readStampImages(t, h, "Stamp")
				require.Len(t, reopened.nodes, len(updated.nodes))
				require.Len(t, reopened.edges, len(updated.edges))
				for id, node := range reopened.nodes {
					require.True(t, updated.nodes[id].CreatedAt.Equal(node.CreatedAt), "node %s: CreatedAt survives a restart", id)
					require.True(t, updated.nodes[id].UpdatedAt.Equal(node.UpdatedAt), "node %s: UpdatedAt survives a restart", id)
					require.Equal(t, updated.nodes[id].Properties, node.Properties)
				}
				for edgeType, edge := range reopened.edges {
					require.True(t, updated.edges[edgeType].CreatedAt.Equal(edge.CreatedAt), "relationship %s: CreatedAt survives a restart", edgeType)
					require.True(t, updated.edges[edgeType].UpdatedAt.Equal(edge.UpdatedAt), "relationship %s: UpdatedAt survives a restart", edgeType)
					require.Equal(t, updated.edges[edgeType].Properties, edge.Properties)
				}
				require.Empty(t, readStampImages(t, h, "Rolled").nodes)
			})
		}
	}
}
