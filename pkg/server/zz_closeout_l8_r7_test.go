package server

// Close-out lane L8, round 7 (2026-11-22): new item found in L-H9-017's run:
// POST /nornicdb/similar answers 404 "Node not found" for a node's own
// elementId and finds the node only by its bare stored id. Test: two nodes
// with managed chunk vectors; the request with the bare id (control) and with
// the elementId Cypher returns for the same node. Owner step:
// handleSimilar passes node_id to GetNode unchanged
// (server_nornicdb.go:931, storage.NodeID(req.NodeID)); the run calls GetNode
// with both forms and with the local id storage.ParseElementID extracts from
// the elementId. It also records which id form the server hands out (Cypher
// elementId(n) and id(n), the ids in /nornicdb/similar's own 200 response).
// Test-only, not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL8_Similar_ElementID(t *testing.T) {
	h := l8Server(t)
	defer h.Close(t)
	ctx := context.Background()
	engine := h.db.GetStorage()
	for i, vec := range [][]float32{{1, 0, 0}, {0.9, 0.1, 0}, {0, 1, 0}} {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("sim-%d", i)), Labels: []string{"Sim"}, Properties: map[string]interface{}{"i": int64(i)}, ChunkEmbeddings: [][]float32{vec}}
		if _, err := engine.CreateNode(ctx, node); err != nil {
			t.Fatalf("create: %v", err)
		}
	}
	result, err := neo4jdriver.ExecuteQuery(ctx, h.driver, "MATCH (n:Sim {i: 0}) RETURN elementId(n) AS e, id(n) AS i", nil, neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase("nornic"))
	if err != nil || len(result.Records) != 1 {
		t.Fatalf("elementId: %v", err)
	}
	elementID, _ := result.Records[0].Values[0].(string)
	idValue := fmt.Sprint(result.Records[0].Values[1])
	_, database, local, ok := storage.ParseElementID(elementID)
	l8Log(t, "L8_SIMILAR_IDS", map[string]any{"elementId": elementID, "id()": idValue, "parsed_database": database, "parsed_local": local, "parsed_ok": ok})

	call := func(id string) (int, string) {
		time.Sleep(10 * time.Millisecond)
		return l8HTTP(t, h, "similar-elementid", "POST", "/nornicdb/similar", `{"node_id":"`+id+`","limit":5}`)
	}
	bareStatus, bareBody := call("sim-0")
	elemStatus, elemBody := call(elementID)
	idStatus, _ := call(idValue)
	returned := []string{}
	var parsed []map[string]any
	if json.Unmarshal([]byte(bareBody), &parsed) == nil {
		for _, item := range parsed {
			if node, ok := item["node"].(map[string]any); ok {
				returned = append(returned, fmt.Sprint(node["id"]), fmt.Sprint(node["elementId"]))
			} else {
				returned = append(returned, fmt.Sprint(item["id"]))
			}
		}
	}
	l8Log(t, "L8_SIMILAR", map[string]any{"bare_status": bareStatus, "elementId_status": elemStatus, "elementId_body": l8Cut(elemBody), "id()_status": idStatus, "ids_in_200_response": returned, "bare_body": l8Cut(bareBody)})

	// Owner step: the lookup handleSimilar performs, with each form.
	for _, form := range []string{"sim-0", elementID, local} {
		node, err := engine.GetNode(ctx, storage.NodeID(form))
		l8Log(t, "L8_OWNER", map[string]any{"lead": "similar-elementid", "step": "GetNode(storage.NodeID(node_id)) as server_nornicdb.go:931", "node_id": form, "found": err == nil && node != nil, "err": fmt.Sprint(err)})
	}

	if bareStatus != http.StatusOK {
		t.Fatalf("control: bare id %d %s", bareStatus, bareBody)
	}
	if elemStatus != http.StatusOK {
		t.Errorf("node_id = the node's elementId %q: %d %s (bare id answers 200)", elementID, elemStatus, strings.TrimSpace(elemBody))
	}
}
