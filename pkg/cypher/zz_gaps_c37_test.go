package cypher

// Report gap C37 (v1.5.2 verification campaign, test-side only): can a
// caller's change to a node returned by the Go API reach the engine's node
// point-read cache, so that a later Cypher statement (or a later Go read)
// sees it? Same fixture as the close-out reproduction
// (newPublicConsumerViewExecutor: in-memory Badger engine with caches,
// namespace nor310, :Doc nodes), property decay off and on.
// Routes that return a node to the caller:
//   storage-GetNode: BadgerEngine.GetNode (the Go storage point read);
//   cypher-RETURN:   a Cypher result's node (RETURN d).
// After changing the returned node's title and its tags list, it reads the
// node with a later Cypher statement, with BadgerEngine.GetNode again, and
// with GetStoredNode (the stored bytes). Diagnostic: logs GAPC37 lines.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func gapC37First(v interface{}) interface{} {
	switch l := v.(type) {
	case []interface{}:
		if len(l) > 0 {
			return l[0]
		}
	case []string:
		if len(l) > 0 {
			return l[0]
		}
	}
	return fmt.Sprintf("<%T>", v)
}

func gapC37Mutate(props map[string]interface{}) bool {
	props["title"] = "mutated-title"
	switch l := props["tags"].(type) {
	case []interface{}:
		if len(l) > 0 {
			l[0] = "mutated-list"
			return true
		}
	case []string:
		if len(l) > 0 {
			l[0] = "mutated-list"
			return true
		}
	}
	return false
}

func TestGapC37NodePointReadCache(t *testing.T) {
	for _, decay := range []bool{false, true} {
		for _, route := range []string{"storage-GetNode", "cypher-RETURN"} {
			t.Run(fmt.Sprintf("decay=%v/%s", decay, route), func(t *testing.T) {
				exec, be := newPublicConsumerViewExecutor(t, 1)
				be.SetDecayEnabled(decay)
				ctx := context.Background()
				_, err := exec.Execute(ctx, "MATCH (d:Doc {title:'a'}) SET d.tags=['original']", nil)
				require.NoError(t, err)
				// Warm the point-read cache with a Go read and a Cypher read.
				_, err = be.GetNode(ctx, "nor310:a")
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "MATCH (d:Doc) WHERE id(d) IS NOT NULL RETURN d.title", nil)
				require.NoError(t, err)

				var listChanged bool
				switch route {
				case "storage-GetNode":
					n, err := be.GetNode(ctx, "nor310:a")
					require.NoError(t, err)
					listChanged = gapC37Mutate(n.Properties)
				case "cypher-RETURN":
					res, err := exec.Execute(ctx, "MATCH (d:Doc {title:'a'}) RETURN d", nil)
					require.NoError(t, err)
					require.Len(t, res.Rows, 1)
					var props map[string]interface{}
					switch e := res.Rows[0][0].(type) {
					case *storage.Node:
						props = e.Properties
					case map[string]interface{}:
						if p, ok := e["properties"].(map[string]interface{}); ok {
							props = p
						} else {
							props = e
						}
					default:
						t.Fatalf("unexpected node carrier %T", res.Rows[0][0])
					}
					listChanged = gapC37Mutate(props)
				}

				later, err := exec.Execute(ctx, "MATCH (d:Doc) RETURN d.title AS title, d.tags AS tags ORDER BY title", nil)
				require.NoError(t, err)
				var titles []interface{}
				var tagFirst []interface{}
				for _, row := range later.Rows {
					titles = append(titles, row[0])
					tagFirst = append(tagFirst, gapC37First(row[1]))
				}
				byTitle, err := exec.Execute(ctx, "MATCH (d:Doc {title:'a'}) RETURN count(d)", nil)
				require.NoError(t, err)
				again, err := be.GetNode(ctx, "nor310:a")
				require.NoError(t, err)
				stored, err := be.GetStoredNode(ctx, "nor310:a")
				require.NoError(t, err)
				leakedCypher := false
				for i := range titles {
					if titles[i] == "mutated-title" || tagFirst[i] == "mutated-list" {
						leakedCypher = true
					}
				}
				leakedGo := again.Properties["title"] == "mutated-title" || gapC37First(again.Properties["tags"]) == "mutated-list"
				t.Logf("GAPC37 decay=%v route=%s list_mutated=%t | later Cypher titles=%v tags[0]=%v count(title='a')=%v | later Go GetNode title=%v tags[0]=%v | stored title=%v tags[0]=%v | LEAK_to_later_cypher=%t LEAK_to_later_go_read=%t",
					decay, route, listChanged, titles, tagFirst, byTitle.Rows, again.Properties["title"], gapC37First(again.Properties["tags"]), stored.Properties["title"], gapC37First(stored.Properties["tags"]), leakedCypher, leakedGo)
			})
		}
	}
}
