package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type graphOutgoingErrorEngine struct {
	storage.Engine
}

func (e *graphOutgoingErrorEngine) GetOutgoingEdges(storage.NodeID) ([]*storage.Edge, error) {
	return nil, fmt.Errorf("boom")
}

type graphEdgesStubEngine struct {
	storage.Engine
	outgoing      []*storage.Edge
	incoming      []*storage.Edge
	outErr        error
	inErr         error
	cancel        context.CancelFunc
	cancelIn      context.CancelFunc
	incomingCalls int
}

func (e *graphEdgesStubEngine) GetOutgoingEdges(storage.NodeID) ([]*storage.Edge, error) {
	if e.cancel != nil {
		e.cancel()
	}
	if e.outErr != nil {
		return nil, e.outErr
	}
	return e.outgoing, nil
}

func (e *graphEdgesStubEngine) GetIncomingEdges(storage.NodeID) ([]*storage.Edge, error) {
	e.incomingCalls++
	if e.cancelIn != nil {
		e.cancelIn()
	}
	if e.inErr != nil {
		return nil, e.inErr
	}
	return e.incoming, nil
}

type graphNeighborhoodStubEngine struct {
	*graphEdgesStubEngine
	nodes map[storage.NodeID]*storage.Node
}

func (e *graphNeighborhoodStubEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	return e.nodes[id], nil
}

type graphNoMVCCEngine struct {
	storage.Engine
}

type graphMVCCVisibilityOnlyEngine struct {
	storage.Engine
	nodes map[storage.NodeID]*storage.Node
}

func (e *graphMVCCVisibilityOnlyEngine) GetNodeLatestVisible(id storage.NodeID) (*storage.Node, error) {
	return e.GetNodeVisibleAt(id, storage.MVCCVersion{})
}

func (e *graphMVCCVisibilityOnlyEngine) GetNodeVisibleAt(id storage.NodeID, _ storage.MVCCVersion) (*storage.Node, error) {
	node, ok := e.nodes[id]
	if !ok {
		return nil, nil
	}
	return node, nil
}

func (e *graphMVCCVisibilityOnlyEngine) GetEdgeLatestVisible(storage.EdgeID) (*storage.Edge, error) {
	return nil, nil
}

func (e *graphMVCCVisibilityOnlyEngine) GetEdgeVisibleAt(storage.EdgeID, storage.MVCCVersion) (*storage.Edge, error) {
	return nil, nil
}

type graphSnapshotIndexedErrorEngine struct {
	*graphSnapshotIndexEngine
	err error
}

func (e *graphSnapshotIndexedErrorEngine) GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error) {
	return nil, e.err
}

func nodeHeadVersionOrNow(t *testing.T, engine storage.Engine, nodeID storage.NodeID) storage.MVCCVersion {
	t.Helper()
	if headEngine, ok := engine.(storage.MVCCHeadEngine); ok {
		head, err := headEngine.GetNodeCurrentHead(nodeID)
		require.NoError(t, err)
		return head.Version
	}
	return storage.MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: ^uint64(0)}
}

func edgeHeadVersionOrNow(t *testing.T, engine storage.Engine, edgeID storage.EdgeID) storage.MVCCVersion {
	t.Helper()
	if headEngine, ok := engine.(storage.MVCCHeadEngine); ok {
		head, err := headEngine.GetEdgeCurrentHead(edgeID)
		require.NoError(t, err)
		return head.Version
	}
	return storage.MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: ^uint64(0)}
}

func advanceMVCCClockPast(t *testing.T, engine storage.Engine, minTimestamp time.Time) {
	t.Helper()
	headEngine, ok := engine.(storage.MVCCHeadEngine)
	if !ok {
		waitDeadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(waitDeadline) {
			if time.Now().UTC().After(minTimestamp) {
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
		require.True(t, time.Now().UTC().After(minTimestamp), "current time did not advance beyond baseline timestamp")
		return
	}

	clockID := storage.NodeID(fmt.Sprintf("__mvcc-clock-%d", time.Now().UnixNano()))
	_, err := engine.CreateNode(&storage.Node{
		ID:         clockID,
		Labels:     []string{"Clock"},
		Properties: map[string]interface{}{"tick": 0},
	})
	require.NoError(t, err)

	waitDeadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(waitDeadline) {
		head, err := headEngine.GetNodeCurrentHead(clockID)
		require.NoError(t, err)
		if head.Version.CommitTimestamp.After(minTimestamp) {
			return
		}
		node, err := engine.GetNode(clockID)
		require.NoError(t, err)
		require.NotNil(t, node)
		require.NoError(t, engine.UpdateNode(node))
	}
	head, err := headEngine.GetNodeCurrentHead(clockID)
	require.NoError(t, err)
	require.True(t, head.Version.CommitTimestamp.After(minTimestamp), "mvcc clock did not advance beyond baseline timestamp")
}

type graphSnapshotIndexEngine struct {
	storage.Engine
	nodes                map[storage.NodeID]*storage.Node
	edges                []*storage.Edge
	getEdgesByTypeCalls  int
	getEdgesBetweenCalls int
}

func (e *graphSnapshotIndexEngine) GetNodeLatestVisible(id storage.NodeID) (*storage.Node, error) {
	return e.GetNodeVisibleAt(id, storage.MVCCVersion{})
}

func (e *graphSnapshotIndexEngine) GetNodeVisibleAt(id storage.NodeID, _ storage.MVCCVersion) (*storage.Node, error) {
	node, ok := e.nodes[id]
	if !ok {
		return nil, nil
	}
	return node, nil
}

func (e *graphSnapshotIndexEngine) GetEdgeLatestVisible(id storage.EdgeID) (*storage.Edge, error) {
	return e.GetEdgeVisibleAt(id, storage.MVCCVersion{})
}

func (e *graphSnapshotIndexEngine) GetEdgeVisibleAt(id storage.EdgeID, _ storage.MVCCVersion) (*storage.Edge, error) {
	for _, edge := range e.edges {
		if edge != nil && edge.ID == id {
			return edge, nil
		}
	}
	return nil, nil
}

func (e *graphSnapshotIndexEngine) GetNodesByLabelVisibleAt(label string, _ storage.MVCCVersion) ([]*storage.Node, error) {
	out := make([]*storage.Node, 0, len(e.nodes))
	for _, node := range e.nodes {
		if node == nil {
			continue
		}
		if label == "" {
			out = append(out, node)
			continue
		}
		for _, l := range node.Labels {
			if l == label {
				out = append(out, node)
				break
			}
		}
	}
	return out, nil
}

func (e *graphSnapshotIndexEngine) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	out := make([]*storage.Edge, 0, len(e.edges))
	for _, edge := range e.edges {
		if edge != nil && edge.StartNode == nodeID {
			out = append(out, edge)
		}
	}
	return out, nil
}

func (e *graphSnapshotIndexEngine) GetIncomingEdgesVisibleAt(nodeID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	out := make([]*storage.Edge, 0, len(e.edges))
	for _, edge := range e.edges {
		if edge != nil && edge.EndNode == nodeID {
			out = append(out, edge)
		}
	}
	return out, nil
}

func (e *graphSnapshotIndexEngine) GetEdgesByTypeVisibleAt(edgeType string, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	e.getEdgesByTypeCalls++
	out := make([]*storage.Edge, 0, len(e.edges))
	for _, edge := range e.edges {
		if edge == nil {
			continue
		}
		if edgeType == "" || edge.Type == edgeType {
			out = append(out, edge)
		}
	}
	return out, nil
}

func (e *graphSnapshotIndexEngine) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	e.getEdgesBetweenCalls++
	out := make([]*storage.Edge, 0)
	for _, edge := range e.edges {
		if edge != nil && edge.StartNode == startID && edge.EndNode == endID {
			out = append(out, edge)
		}
	}
	return out, nil
}

func decodeGraphPayload(t *testing.T, recorderBody interface{ Bytes() []byte }) graphPayload {
	t.Helper()
	var payload graphPayload
	require.NoError(t, json.Unmarshal(recorderBody.Bytes(), &payload))
	return payload
}

func getDefaultStorage(t *testing.T, server *Server) storage.Engine {
	t.Helper()
	engine, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	return engine
}

func defaultGraphPath(server *Server, operation string) string {
	return "/nornicdb/graph/" + server.dbManager.DefaultDatabaseName() + "/" + operation
}

func TestGraphNeighborhoodEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "d", Labels: []string{"Topic"}, Properties: map[string]interface{}{"name": "Databases"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "KNOWS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "cd", StartNode: "c", EndNode: "d", Type: "LIKES"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":           []string{"a"},
		"depth":              2,
		"relationship_types": []string{"KNOWS"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 3, payload.Meta.NodeCount)
	require.Equal(t, 2, payload.Meta.EdgeCount)
	require.Equal(t, "node", payload.Meta.GeneratedFrom)
	require.Equal(t, 2, payload.Meta.Depth)
	require.Equal(t, []string{"a", "b", "c"}, []string{payload.Nodes[0].ID, payload.Nodes[1].ID, payload.Nodes[2].ID})
}

func TestGraphNeighborhoodEndpoint_RejectsWhitespaceOnlyNodeIDs(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{" ", "\t", "\n"},
		"depth":    1,
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
}

func TestGraphNeighborhoodEndpoint_LimitPreservesEdgesBetweenIncludedNodes(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))

	// limit=2 means only the two seed nodes are allowed in the node set.
	// The connecting edge between those already-included nodes must still be returned.
	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a", "b"},
		"depth":    1,
		"limit":    2,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.Equal(t, "ab", payload.Edges[0].ID)
}

func TestGraphNeighborhoodEndpoint_LimitTruncatesSeedNodes(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for _, id := range []string{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Person"}})
		require.NoError(t, err)
	}

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a", "b", "c"},
		"depth":    1,
		"limit":    2,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.True(t, payload.Meta.Truncated)
}

func TestGraphPathEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for _, id := range []string{"a", "b", "c", "z"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": id}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "az", StartNode: "a", EndNode: "z", Type: "OTHER"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id":     "a",
		"target_node_id":     "c",
		"relationship_types": []string{"LINKS"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 3, payload.Meta.NodeCount)
	require.Equal(t, 2, payload.Meta.EdgeCount)
	require.Equal(t, "query", payload.Meta.GeneratedFrom)
	require.Equal(t, "ab", payload.Edges[0].ID)
	require.Equal(t, "bc", payload.Edges[1].ID)
}

func TestGraphPathEndpoint_LimitExceededDoesNotReturnNotFound(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for _, id := range []string{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": id}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "LINKS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id":     "a",
		"target_node_id":     "c",
		"relationship_types": []string{"LINKS"},
		"limit":              2,
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "path search limit exceeded")
}

func TestGraphTemporalEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v1"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS", Properties: map[string]interface{}{"weight": 1}}))
	asOfVersion := edgeHeadVersionOrNow(t, engine, "ab")
	asOf := asOfVersion.CommitTimestamp.UTC().Format(time.RFC3339Nano)

	advanceMVCCClockPast(t, engine, asOfVersion.CommitTimestamp)
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v2"}}))
	require.NoError(t, engine.UpdateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LIKES", Properties: map[string]interface{}{"weight": 2}}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": []string{"a", "b"},
		"as_of":    asOf,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.Equal(t, "Alice v1", payload.Nodes[0].Properties["name"])
	require.Equal(t, "KNOWS", payload.Edges[0].Type)
	require.Equal(t, asOf, payload.Meta.AsOf)
}

func TestGraphTemporalEndpoint_RejectsWhitespaceOnlyNodeIDs(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": []string{" ", "\t", "\n"},
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
}

func TestGraphTemporalEndpoint_AdditionalValidationBranches(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	asOf := time.Now().UTC().Format(time.RFC3339Nano)

	badJSONReq := httptest.NewRequest(http.MethodPost, defaultGraphPath(server, "temporal"), strings.NewReader("{"))
	badJSONRec := httptest.NewRecorder()
	server.handleGraphTemporal(badJSONRec, badJSONReq)
	require.Equal(t, 400, badJSONRec.Code)
	require.Contains(t, badJSONRec.Body.String(), "invalid request body")

	missingIDs := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, "temporal"), map[string]interface{}{
		"as_of": asOf,
	}, "Bearer "+token)
	require.Equal(t, 400, missingIDs.Code)
	require.Contains(t, missingIDs.Body.String(), "node_ids is required")

	missingDBReq := httptest.NewRequest(http.MethodPost, "/nornicdb/graph//temporal", strings.NewReader(`{"node_ids":["a"],"as_of":"`+asOf+`"}`))
	missingDBReq.Header.Set("Content-Type", "application/json")
	missingDBRec := httptest.NewRecorder()
	server.handleGraphTemporal(missingDBRec, missingDBReq)
	require.Equal(t, 400, missingDBRec.Code)
	require.Contains(t, missingDBRec.Body.String(), "database path parameter is required")
}

func TestGraphDiffEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v1"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))
	asOfVersion := edgeHeadVersionOrNow(t, engine, "ab")
	asOf := asOfVersion.CommitTimestamp.UTC().Format(time.RFC3339Nano)

	advanceMVCCClockPast(t, engine, asOfVersion.CommitTimestamp)
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v2"}}))
	require.NoError(t, engine.DeleteEdge("ab"))
	require.NoError(t, engine.DeleteNode("b"))
	_, err = engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "KNOWS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": []string{"a", "b", "c"},
		"as_of":    asOf,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	nodesByID := make(map[string]graphNodePayload, len(payload.Nodes))
	for _, node := range payload.Nodes {
		nodesByID[node.ID] = node
	}
	edgesByID := make(map[string]graphEdgePayload, len(payload.Edges))
	for _, edge := range payload.Edges {
		edgesByID[edge.ID] = edge
	}
	require.Equal(t, "changed", nodesByID["a"].Status)
	require.Equal(t, "added", nodesByID["b"].Status)
	require.Equal(t, "removed", nodesByID["c"].Status)
	require.Equal(t, "added", edgesByID["ab"].Status)
	require.Equal(t, "removed", edgesByID["ac"].Status)
	require.Equal(t, asOf, payload.Meta.AsOf)
	require.Equal(t, "current", payload.Meta.CompareTo)
}

func TestGraphNeighborhoodEndpoint_RespectsResolvedDatabaseReadAccess(t *testing.T) {
	server, authenticator := setupTestServer(t)
	readerToken := getAuthToken(t, authenticator, "reader")
	require.NotNil(t, server.allowlistStore)
	require.NotNil(t, server.privilegesStore)

	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{dbName}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", dbName, false, false))

	engine := getDefaultStorage(t, server)
	_, err := engine.CreateNode(&storage.Node{ID: "rbac-a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"rbac-a"},
	}, "Bearer "+readerToken)
	require.Equal(t, 403, resp.Code)

	var payload struct {
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.NotEmpty(t, payload.Errors)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", payload.Errors[0].Code)
	require.Contains(t, payload.Errors[0].Message, "not allowed")
}

func TestGraphEdgesForNode_RespectsContextCancellation(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := graphEdgesForNode(ctx, engine, storage.NodeID("any"))
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestGraphEdgesForNode_DedupesOutgoingAndIncoming(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "self", Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "self-loop", StartNode: "self", EndNode: "self", Type: "LOOPS"}))

	edges, err := graphEdgesForNode(context.Background(), engine, "self")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, storage.EdgeID("self-loop"), edges[0].ID)
}

func TestGraphEdgesForNode_ErrorAndFilterBranches(t *testing.T) {
	t.Run("outgoing error", func(t *testing.T) {
		wantErr := fmt.Errorf("outgoing failed")
		engine := &graphEdgesStubEngine{Engine: storage.NewMemoryEngine(), outErr: wantErr}
		t.Cleanup(func() { _ = engine.Close() })

		_, err := graphEdgesForNode(context.Background(), engine, "n1")
		require.ErrorIs(t, err, wantErr)
		require.Zero(t, engine.incomingCalls)
	})

	t.Run("context canceled after outgoing lookup", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		engine := &graphEdgesStubEngine{Engine: storage.NewMemoryEngine(), cancel: cancel}
		t.Cleanup(func() { _ = engine.Close() })

		_, err := graphEdgesForNode(ctx, engine, "n1")
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, engine.incomingCalls)
	})

	t.Run("incoming error", func(t *testing.T) {
		wantErr := fmt.Errorf("incoming failed")
		engine := &graphEdgesStubEngine{Engine: storage.NewMemoryEngine(), inErr: wantErr}
		t.Cleanup(func() { _ = engine.Close() })

		_, err := graphEdgesForNode(context.Background(), engine, "n1")
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, 1, engine.incomingCalls)
	})

	t.Run("filters nil and duplicate edges", func(t *testing.T) {
		shared := &storage.Edge{ID: "shared", StartNode: "n1", EndNode: "n2", Type: "REL"}
		incomingOnly := &storage.Edge{ID: "incoming", StartNode: "n3", EndNode: "n1", Type: "REL"}
		engine := &graphEdgesStubEngine{
			Engine:   storage.NewMemoryEngine(),
			outgoing: []*storage.Edge{nil, shared},
			incoming: []*storage.Edge{nil, shared, incomingOnly},
		}
		t.Cleanup(func() { _ = engine.Close() })

		edges, err := graphEdgesForNode(context.Background(), engine, "n1")
		require.NoError(t, err)
		require.Equal(t, []storage.EdgeID{"shared", "incoming"}, []storage.EdgeID{edges[0].ID, edges[1].ID})
	})
}

func TestGraphExpandEndpoint_DelegatesToNeighborhood(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "expand"), map[string]interface{}{
		"node_ids": []string{"a"},
		"depth":    1,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)
	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, "node", payload.Meta.GeneratedFrom)
	require.Equal(t, 2, payload.Meta.NodeCount)
}

func TestGraphPathEndpoint_NotFoundReturns404(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}})
	require.NoError(t, err)

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id": "a",
		"target_node_id": "b",
	}, "Bearer "+token)
	require.Equal(t, 404, resp.Code)
}

func TestGraphPathEndpoint_RejectsAsOfWithDatabaseRouteHint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id": "a",
		"target_node_id": "b",
		"as_of":          time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/path")
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/temporal")
}

func TestGraphDiffEndpoint_WithCompareToUsesBaselineTimestamp(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{
		ID:         "a",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice v1"},
	})
	require.NoError(t, err)
	baselineVersion := nodeHeadVersionOrNow(t, engine, "a")
	baseline := baselineVersion.CommitTimestamp.UTC().Format(time.RFC3339Nano)

	advanceMVCCClockPast(t, engine, baselineVersion.CommitTimestamp)
	require.NoError(t, engine.UpdateNode(&storage.Node{
		ID:         "a",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice v2"},
	}))
	target := nodeHeadVersionOrNow(t, engine, "a").CommitTimestamp.UTC().Format(time.RFC3339Nano)

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids":   []string{"a"},
		"as_of":      target,
		"compare_to": baseline,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)
	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, target, payload.Meta.AsOf)
	require.Equal(t, baseline, payload.Meta.CompareTo)
}

func TestGraphDiffEndpoint_AdditionalValidationBranches(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	asOf := time.Now().UTC().Format(time.RFC3339Nano)

	wrongMethod := makeRequest(t, server, http.MethodGet, defaultGraphPath(server, "diff"), nil, "Bearer "+token)
	require.Equal(t, 405, wrongMethod.Code)
	require.Contains(t, wrongMethod.Body.String(), "POST required")

	badJSONReq := httptest.NewRequest(http.MethodPost, defaultGraphPath(server, "diff"), strings.NewReader("{"))
	badJSONRec := httptest.NewRecorder()
	server.handleGraphDiff(badJSONRec, badJSONReq)
	require.Equal(t, 400, badJSONRec.Code)
	require.Contains(t, badJSONRec.Body.String(), "invalid request body")

	whitespaceIDs := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": []string{" ", "\t", "\n"},
		"as_of":    asOf,
	}, "Bearer "+token)
	require.Equal(t, 400, whitespaceIDs.Code)
	require.Contains(t, whitespaceIDs.Body.String(), "node_ids is required")

	invalidAsOf := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": []string{"a"},
		"as_of":    "not-a-date",
	}, "Bearer "+token)
	require.Equal(t, 400, invalidAsOf.Code)
	require.Contains(t, invalidAsOf.Body.String(), "as_of must be a valid datetime")

	missingAsOf := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": []string{"a"},
		"as_of":    " ",
	}, "Bearer "+token)
	require.Equal(t, 400, missingAsOf.Code)
	require.Contains(t, missingAsOf.Body.String(), "as_of is required")

	invalidCompareTo := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids":   []string{"a"},
		"as_of":      asOf,
		"compare_to": "not-a-date",
	}, "Bearer "+token)
	require.Equal(t, 400, invalidCompareTo.Code)
	require.Contains(t, invalidCompareTo.Body.String(), "compare_to must be a valid datetime")

	missingDBReq := httptest.NewRequest(http.MethodPost, "/nornicdb/graph//diff", strings.NewReader(`{"node_ids":["a"],"as_of":"`+asOf+`"}`))
	missingDBReq.Header.Set("Content-Type", "application/json")
	missingDBRec := httptest.NewRecorder()
	server.handleGraphDiff(missingDBRec, missingDBReq)
	require.Equal(t, 400, missingDBRec.Code)
	require.Contains(t, missingDBRec.Body.String(), "database path parameter is required")
}

func TestWriteGraphResolveError_MapsSentinels(t *testing.T) {
	server, _ := setupTestServer(t)

	recNotFound := httptest.NewRecorder()
	server.writeGraphResolveError(recNotFound, multidb.ErrDatabaseNotFound)
	require.Equal(t, 404, recNotFound.Code)

	recOffline := httptest.NewRecorder()
	server.writeGraphResolveError(recOffline, multidb.ErrDatabaseOffline)
	require.Equal(t, 503, recOffline.Code)

	recBadRequest := httptest.NewRecorder()
	server.writeGraphResolveError(recBadRequest, ErrBadRequest)
	require.Equal(t, 400, recBadRequest.Code)
}

func TestResolveGraphStorage_Branches(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	_, _, err := server.resolveGraphStorage(req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database path parameter is required")

	req = httptest.NewRequest(http.MethodPost, "/", nil)
	req.SetPathValue("database", "nornic")
	_, _, err = server.resolveGraphStorage(req)
	require.ErrorIs(t, err, errGraphForbidden)

	server.auth = nil
	req = httptest.NewRequest(http.MethodPost, "/", nil).WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Roles: []string{"unknown"}}))
	req.SetPathValue("database", "nornic")
	_, _, err = server.resolveGraphStorage(req)
	require.ErrorIs(t, err, errGraphForbidden)

	req = httptest.NewRequest(http.MethodPost, "/", nil)
	req.SetPathValue("database", "nornic")
	dbName, engine, err := server.resolveGraphStorage(req)
	require.NoError(t, err)
	require.Equal(t, "nornic", dbName)
	require.NotNil(t, engine)

	req = httptest.NewRequest(http.MethodPost, "/", nil)
	req.SetPathValue("database", "missing")
	_, _, err = server.resolveGraphStorage(req)
	require.ErrorIs(t, err, multidb.ErrDatabaseNotFound)

	require.NoError(t, server.dbManager.CreateCompositeDatabase("cmp_graph_resolve", []multidb.ConstituentRef{{
		Alias:        "main",
		DatabaseName: "nornic",
		Type:         "local",
		AccessMode:   "read_write",
	}}))
	req = httptest.NewRequest(http.MethodPost, "/", nil)
	req.SetPathValue("database", "cmp_graph_resolve")
	_, _, err = server.resolveGraphStorage(req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "graph endpoints on composite database")
}

func TestGraphHelperParsersAndNormalizers(t *testing.T) {
	ids := normalizeGraphNodeIDs([]string{" a ", "", "a", "\t", "b", "b"})
	require.Equal(t, []string{"a", "b"}, ids)

	normalized := normalizeNodeIDs([]string{"b", " a ", "b", "", "a"})
	require.Equal(t, []string{"a", "b"}, normalized)

	unixVer, err := parseGraphVersion("1710000000")
	require.NoError(t, err)
	require.Equal(t, int64(1710000000), unixVer.CommitTimestamp.Unix())

	rfcVer, err := parseGraphVersion("2026-03-31T13:29:57Z")
	require.NoError(t, err)
	require.Equal(t, "2026-03-31T13:29:57Z", rfcVer.CommitTimestamp.Format(time.RFC3339))

	_, err = parseGraphVersion("not-a-date")
	require.Error(t, err)
}

func TestGraphHelperComparators(t *testing.T) {
	left := graphEdgePayload{
		ID:         "ab",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Semantic:   true,
		Properties: map[string]interface{}{"weight": 1},
	}
	right := graphEdgePayload{
		ID:         "ab",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Semantic:   true,
		Properties: map[string]interface{}{"weight": 1},
	}
	require.True(t, sameEdgePayload(left, right))

	right.Properties["weight"] = 2
	require.False(t, sameEdgePayload(left, right))
}

func TestGraphHandlers_MethodNotAllowed(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	for _, op := range []string{"neighborhood", "expand", "path", "temporal", "diff"} {
		resp := makeRequest(t, server, "GET", defaultGraphPath(server, op), nil, "Bearer "+token)
		require.Equal(t, 405, resp.Code)
	}
}

func TestGraphNeighborhoodEndpoint_RejectsAsOf(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a"},
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/temporal")
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/diff")
}

func TestGraphPathEndpoint_RequiresSourceAndTarget(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id": "a",
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "source_node_id and target_node_id are required")
}

func TestGraphTemporalAndDiff_RequireAsOf(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	temporal := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": []string{"a"},
		"as_of":    "invalid",
	}, "Bearer "+token)
	require.Equal(t, 400, temporal.Code)
	require.Contains(t, temporal.Body.String(), "as_of must be a valid datetime")

	diff := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": []string{"a"},
	}, "Bearer "+token)
	require.Equal(t, 400, diff.Code)
	require.Contains(t, diff.Body.String(), "as_of is required")

	diffInvalidCompareTo := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids":   []string{"a"},
		"as_of":      time.Now().UTC().Format(time.RFC3339Nano),
		"compare_to": "not-a-date",
	}, "Bearer "+token)
	require.Equal(t, 400, diffInvalidCompareTo.Code)
	require.Contains(t, diffInvalidCompareTo.Body.String(), "compare_to must be a valid datetime")
}

func TestGraphTemporalAndDiff_RejectExcessiveNodeIDs(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	nodeIDs := make([]string, 0, maxGraphTemporalDiffNodeIDs+1)
	for i := 0; i < maxGraphTemporalDiffNodeIDs+1; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("n-%d", i))
	}

	temporal := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": nodeIDs,
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, temporal.Code)
	require.Contains(t, temporal.Body.String(), "node_ids exceeds maximum")
	require.Contains(t, temporal.Body.String(), "temporal graph requests")

	diff := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": nodeIDs,
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, diff.Code)
	require.Contains(t, diff.Body.String(), "node_ids exceeds maximum")
	require.Contains(t, diff.Body.String(), "diff graph requests")
}

func TestCollectLatestNeighborhood_CanceledContext(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = server.collectLatestNeighborhood(ctx, engine, []string{"a"}, 1, 10, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, context.Canceled)
}

func TestCollectLatestNeighborhood_SeedAndFilterBranches(t *testing.T) {
	server := &Server{}
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()
	engine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{
			Engine: base,
			outgoing: []*storage.Edge{
				{ID: "blocked", StartNode: "seed", EndNode: "blocked-target", Type: "BLOCKED"},
				{ID: "missing", StartNode: "seed", EndNode: "missing-neighbor", Type: "KNOWS"},
				{ID: "hidden", StartNode: "seed", EndNode: "hidden-target", Type: "KNOWS"},
				{ID: "good", StartNode: "seed", EndNode: "friend", Type: "KNOWS"},
			},
		},
		nodes: map[storage.NodeID]*storage.Node{
			"seed":           {ID: "seed", Labels: []string{"Seed"}},
			"blocked-target": {ID: "blocked-target", Labels: []string{"Person"}},
			"hidden-target":  {ID: "hidden-target", Labels: []string{"Hidden"}},
			"friend":         {ID: "friend", Labels: []string{"Person"}},
		},
	}

	collection, err := server.collectLatestNeighborhood(
		context.Background(),
		engine,
		[]string{"", "seed", "seed", "missing-seed"},
		1,
		0,
		newGraphFilterSet([]string{"Person"}, []string{"KNOWS"}),
	)
	require.NoError(t, err)
	require.False(t, collection.truncated)
	require.Contains(t, collection.nodes, "seed")
	require.Contains(t, collection.nodes, "friend")
	require.NotContains(t, collection.nodes, "blocked-target")
	require.NotContains(t, collection.nodes, "hidden-target")
	require.NotContains(t, collection.nodes, "missing-neighbor")
	require.Contains(t, collection.edges, "good")
	require.NotContains(t, collection.edges, "blocked")
	require.NotContains(t, collection.edges, "hidden")
	require.NotContains(t, collection.edges, "missing")
}

func TestCollectLatestNeighborhood_TruncatesSeedListAndPropagatesEdgeError(t *testing.T) {
	server := &Server{}
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	engine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{Engine: base},
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Node"}},
			"b": {ID: "b", Labels: []string{"Node"}},
		},
	}
	collection, err := server.collectLatestNeighborhood(context.Background(), engine, []string{"a", "b"}, 1, 1, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.True(t, collection.truncated)
	require.Contains(t, collection.nodes, "a")
	require.NotContains(t, collection.nodes, "b")

	wantErr := fmt.Errorf("edge lookup failed")
	errorEngine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{Engine: base, outErr: wantErr},
		nodes:                map[storage.NodeID]*storage.Node{"a": {ID: "a", Labels: []string{"Node"}}},
	}
	_, err = server.collectLatestNeighborhood(context.Background(), errorEngine, []string{"a"}, 1, 10, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, wantErr)
}

func TestCollectLatestPath_SourceEqualsTargetBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)

	_, err := server.collectLatestPath(context.Background(), engine, "missing", "missing", 0, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, storage.ErrNotFound)

	_, err = engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)
	collection, err := server.collectLatestPath(context.Background(), engine, "a", "a", 0, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.Len(t, collection.nodes, 1)
}

func TestCollectLatestInducedSubgraph_PropagatesOutgoingError(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()
	_, err := base.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"Node"}})
	require.NoError(t, err)

	engine := &graphOutgoingErrorEngine{Engine: base}
	_, err = server.collectLatestInducedSubgraph(engine, []string{"nornic:a"}, newGraphFilterSet(nil, nil))
	require.EqualError(t, err, "boom")
}

func TestCollectSnapshotInducedSubgraph_RequiresMVCCInterfaces(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	engine := &graphNoMVCCEngine{Engine: base}
	_, err := server.collectSnapshotInducedSubgraph(engine, []string{"a"}, storage.MVCCVersion{}, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, storage.ErrNotImplemented)
}

func TestCollectSnapshotInducedSubgraph_UsesSingleVisibleEdgeScan(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	engine := &graphSnapshotIndexEngine{
		Engine: base,
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Node"}},
			"b": {ID: "b", Labels: []string{"Node"}},
			"c": {ID: "c", Labels: []string{"Node"}},
		},
		edges: []*storage.Edge{
			{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINKS"},
			{ID: "bc", StartNode: "b", EndNode: "c", Type: "LINKS"},
			{ID: "ca", StartNode: "c", EndNode: "a", Type: "OTHER"},
			{ID: "aa", StartNode: "a", EndNode: "a", Type: "LINKS"}, // self-loop excluded to mirror previous induced-subgraph behavior
		},
	}
	version := storage.MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: ^uint64(0)}

	collection, err := server.collectSnapshotInducedSubgraph(engine, []string{"a", "b", "c"}, version, newGraphFilterSet(nil, []string{"LINKS"}))
	require.NoError(t, err)
	require.Equal(t, 1, engine.getEdgesByTypeCalls)
	require.Equal(t, 0, engine.getEdgesBetweenCalls)
	require.Len(t, collection.edges, 2)
	_, hasAB := collection.edges["ab"]
	_, hasBC := collection.edges["bc"]
	_, hasCA := collection.edges["ca"]
	_, hasAA := collection.edges["aa"]
	require.True(t, hasAB)
	require.True(t, hasBC)
	require.False(t, hasCA)
	require.False(t, hasAA)
}

func TestGraphFilterSetAndCollectionHelpers(t *testing.T) {
	filtered := newGraphFilterSet([]string{" Person ", ""}, []string{" KNOWS ", ""})
	require.False(t, filtered.allowNode(nil))
	require.False(t, filtered.allowNode(&storage.Node{ID: "x", Labels: []string{"Topic"}}))
	require.True(t, filtered.allowNode(&storage.Node{ID: "p", Labels: []string{"Person"}}))

	require.False(t, filtered.allowEdge(nil))
	require.False(t, filtered.allowEdge(&storage.Edge{ID: "e1", Type: "LIKES"}))
	require.True(t, filtered.allowEdge(&storage.Edge{ID: "e2", Type: "KNOWS"}))

	unfiltered := newGraphFilterSet(nil, nil)
	require.True(t, unfiltered.allowNode(&storage.Node{ID: "any"}))
	require.True(t, unfiltered.allowEdge(&storage.Edge{ID: "any"}))

	collection := newGraphCollection()
	collection.addNode(nil, "added")
	collection.addEdge(nil, "added")

	collection.addNode(&storage.Node{ID: "n1", Labels: []string{"L"}, Properties: map[string]interface{}{"k": "v1"}}, "")
	collection.addNode(&storage.Node{ID: "n1", Labels: []string{"L2"}, Properties: map[string]interface{}{"k": "v2"}}, "added")
	collection.addNode(&storage.Node{ID: "n1", Labels: []string{"L3"}, Properties: map[string]interface{}{"k": "v3"}}, "removed")
	require.Equal(t, "added", collection.nodes["n1"].Status)
	require.Equal(t, "v3", collection.nodes["n1"].Properties["k"])

	collection.addEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"k": "v1"}}, "")
	collection.addEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"k": "v2"}}, "added")
	collection.addEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"k": "v3"}}, "removed")
	require.Equal(t, "added", collection.edges["e1"].Status)
	require.Equal(t, "v3", collection.edges["e1"].Properties["k"])

	collection.truncated = true
	payload := collection.payload(graphMetaPayload{Database: "nornic", GeneratedFrom: "node"})
	require.Equal(t, 1, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.True(t, payload.Meta.Truncated)
}

func TestDiffGraphCollections_OmitsUnchangedEntries(t *testing.T) {
	baseline := newGraphCollection()
	target := newGraphCollection()

	baseline.nodes["same-node"] = graphNodePayload{
		ID:         "same-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	target.nodes["same-node"] = graphNodePayload{
		ID:         "same-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	baseline.nodes["changed-node"] = graphNodePayload{
		ID:         "changed-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Before"},
	}
	target.nodes["changed-node"] = graphNodePayload{
		ID:         "changed-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "After"},
	}

	baseline.edges["same-edge"] = graphEdgePayload{
		ID:         "same-edge",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"w": 1},
	}
	target.edges["same-edge"] = graphEdgePayload{
		ID:         "same-edge",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"w": 1},
	}
	baseline.edges["changed-edge"] = graphEdgePayload{
		ID:         "changed-edge",
		Source:     "a",
		Target:     "c",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"w": 1},
	}
	target.edges["changed-edge"] = graphEdgePayload{
		ID:         "changed-edge",
		Source:     "a",
		Target:     "c",
		Type:       "LIKES",
		Properties: map[string]interface{}{"w": 2},
	}

	diff := diffGraphCollections(baseline, target)

	_, hasSameNode := diff.nodes["same-node"]
	_, hasSameEdge := diff.edges["same-edge"]
	require.False(t, hasSameNode)
	require.False(t, hasSameEdge)

	require.Equal(t, "changed", diff.nodes["changed-node"].Status)
	require.Equal(t, "changed", diff.edges["changed-edge"].Status)
}

func TestWriteGraphResolveError_NilNoWrite(t *testing.T) {
	server, _ := setupTestServer(t)
	rec := httptest.NewRecorder()

	server.writeGraphResolveError(rec, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "", rec.Body.String())
}

func TestCollectLatestNeighborhood_CancelledDuringEdgeIteration(t *testing.T) {
	server := &Server{}
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	engine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{
			Engine: base,
			outgoing: []*storage.Edge{
				{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"},
			},
			cancel: cancel,
		},
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Node"}},
			"b": {ID: "b", Labels: []string{"Node"}},
		},
	}

	_, err := server.collectLatestNeighborhood(ctx, engine, []string{"a"}, 1, 10, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, context.Canceled)
}

func TestCollectLatestPath_CanceledContextBranches(t *testing.T) {
	server := &Server{}
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := server.collectLatestPath(ctx, base, "a", "b", 10, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, context.Canceled)

	ctx2, cancel2 := context.WithCancel(context.Background())
	engine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{
			Engine: base,
			outgoing: []*storage.Edge{
				{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"},
			},
			cancel: cancel2,
		},
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Node"}},
			"b": {ID: "b", Labels: []string{"Node"}},
		},
	}
	_, err = server.collectLatestPath(ctx2, engine, "a", "b", 10, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, context.Canceled)
}

func TestCollectLatestInducedSubgraph_SkipsSelfAndOutsideEdges(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}})
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "self", StartNode: "a", EndNode: "a", Type: "REL"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "REL"}))

	collection, err := server.collectLatestInducedSubgraph(engine, []string{"a", "b"}, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.Contains(t, collection.edges, "ab")
	require.NotContains(t, collection.edges, "self")
	require.NotContains(t, collection.edges, "ac")
}

func TestCollectSnapshotInducedSubgraph_AdditionalErrorBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	visibilityOnly := &graphMVCCVisibilityOnlyEngine{
		Engine: base,
		nodes:  map[storage.NodeID]*storage.Node{"a": {ID: "a", Labels: []string{"Node"}}},
	}
	_, err := server.collectSnapshotInducedSubgraph(visibilityOnly, []string{"a"}, storage.MVCCVersion{}, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	errEngine := &graphSnapshotIndexedErrorEngine{
		graphSnapshotIndexEngine: &graphSnapshotIndexEngine{
			Engine: base,
			nodes: map[storage.NodeID]*storage.Node{
				"a": {ID: "a", Labels: []string{"Node"}},
			},
		},
		err: fmt.Errorf("indexed edge scan failed"),
	}
	_, err = server.collectSnapshotInducedSubgraph(errEngine, []string{"a"}, storage.MVCCVersion{}, newGraphFilterSet(nil, nil))
	require.EqualError(t, err, "indexed edge scan failed")
}

func TestCollectSnapshotInducedSubgraph_EmptyResolvedNodesReturnsEarly(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	engine := &graphSnapshotIndexEngine{
		Engine: base,
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Person"}},
		},
		edges: []*storage.Edge{
			{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"},
		},
	}
	collection, err := server.collectSnapshotInducedSubgraph(engine, []string{"a"}, storage.MVCCVersion{}, newGraphFilterSet([]string{"Topic"}, nil))
	require.NoError(t, err)
	require.Empty(t, collection.nodes)
	require.Empty(t, collection.edges)
	require.Equal(t, 0, engine.getEdgesByTypeCalls)
}

func TestGraphVersionAndEdgeIterationCancellationBranches(t *testing.T) {
	_, err := parseGraphVersionForField(" ", "as_of")
	require.EqualError(t, err, "as_of must be a valid datetime")

	ctxOut, cancelOut := context.WithCancel(context.Background())
	engineOut := &graphEdgesStubEngine{
		Engine:   storage.NewMemoryEngine(),
		outgoing: []*storage.Edge{{ID: "o1", StartNode: "a", EndNode: "b", Type: "REL"}},
		cancel:   cancelOut,
	}
	t.Cleanup(func() { _ = engineOut.Close() })
	_, err = graphEdgesForNode(ctxOut, engineOut, "a")
	require.ErrorIs(t, err, context.Canceled)

	ctxIn, cancelIn := context.WithCancel(context.Background())
	engineIn := &graphEdgesStubEngine{
		Engine:   storage.NewMemoryEngine(),
		outgoing: []*storage.Edge{},
		incoming: []*storage.Edge{{ID: "i1", StartNode: "b", EndNode: "a", Type: "REL"}},
		cancel:   cancelIn,
	}
	t.Cleanup(func() { _ = engineIn.Close() })
	_, err = graphEdgesForNode(ctxIn, engineIn, "a")
	require.ErrorIs(t, err, context.Canceled)
}

func TestGraphHandlers_DirectBadJSONBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	nReq := httptest.NewRequest(http.MethodPost, "/nornicdb/graph/nornic/neighborhood", strings.NewReader("{"))
	nRec := httptest.NewRecorder()
	server.handleGraphNeighborhood(nRec, nReq)
	require.Equal(t, http.StatusBadRequest, nRec.Code)
	require.Contains(t, nRec.Body.String(), "invalid request body")

	pReq := httptest.NewRequest(http.MethodPost, "/nornicdb/graph/nornic/path", strings.NewReader("{"))
	pRec := httptest.NewRecorder()
	server.handleGraphPath(pRec, pReq)
	require.Equal(t, http.StatusBadRequest, pRec.Code)
	require.Contains(t, pRec.Body.String(), "invalid request body")
}

func TestCollectLatestNeighborhood_TruncatesWhenNewNeighborExceedsLimit(t *testing.T) {
	server := &Server{}
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()
	engine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{
			Engine:   base,
			outgoing: []*storage.Edge{{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"}},
		},
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Node"}},
			"b": {ID: "b", Labels: []string{"Node"}},
		},
	}
	collection, err := server.collectLatestNeighborhood(context.Background(), engine, []string{"a"}, 1, 1, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.True(t, collection.truncated)
	require.Contains(t, collection.nodes, "a")
	require.NotContains(t, collection.nodes, "b")
}

func TestCollectLatestPath_SkipsMissingOrFilteredNeighborBranches(t *testing.T) {
	server := &Server{}
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()
	engine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{
			Engine:   base,
			outgoing: []*storage.Edge{{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"}},
		},
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Source"}},
			// "b" intentionally missing to hit nil-node skip branch
		},
	}

	_, err := server.collectLatestPath(context.Background(), engine, "a", "b", 10, newGraphFilterSet([]string{"Target"}, nil))
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestCollectSnapshotInducedSubgraph_SkipsEdgesOutsideResolvedSet(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	engine := &graphSnapshotIndexEngine{
		Engine: base,
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Node"}},
			"b": {ID: "b", Labels: []string{"Node"}},
		},
		edges: []*storage.Edge{
			{ID: "ac", StartNode: "a", EndNode: "c", Type: "REL"},
			{ID: "db", StartNode: "d", EndNode: "b", Type: "REL"},
			{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"},
		},
	}

	collection, err := server.collectSnapshotInducedSubgraph(engine, []string{"a", "b"}, storage.MVCCVersion{}, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.Contains(t, collection.edges, "ab")
	require.NotContains(t, collection.edges, "ac")
	require.NotContains(t, collection.edges, "db")
}

func TestGraphEdgesForNode_OutgoingDuplicateAndLoopCancellationBranches(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	dupEngine := &graphEdgesStubEngine{
		Engine: base,
		outgoing: []*storage.Edge{
			{ID: "e1", StartNode: "a", EndNode: "b", Type: "REL"},
			{ID: "e1", StartNode: "a", EndNode: "b", Type: "REL"},
		},
	}
	edges, err := graphEdgesForNode(context.Background(), dupEngine, "a")
	require.NoError(t, err)
	require.Len(t, edges, 1)

	ctxOut, cancelOut := context.WithCancel(context.Background())
	outCancel := &graphEdgesStubEngine{
		Engine:   base,
		outgoing: []*storage.Edge{{ID: "o1", StartNode: "a", EndNode: "b", Type: "REL"}},
		cancelIn: cancelOut,
	}
	_, err = graphEdgesForNode(ctxOut, outCancel, "a")
	require.ErrorIs(t, err, context.Canceled)

	ctxIn, cancelIn := context.WithCancel(context.Background())
	inCancel := &graphEdgesStubEngine{
		Engine:   base,
		outgoing: []*storage.Edge{},
		incoming: []*storage.Edge{{ID: "i1", StartNode: "b", EndNode: "a", Type: "REL"}},
		cancelIn: cancelIn,
	}
	_, err = graphEdgesForNode(ctxIn, inCancel, "a")
	require.ErrorIs(t, err, context.Canceled)
}

func TestGraphHandlers_ResolveAndCollectionErrorBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// handleGraphPath: resolveGraphStorage error branch (missing path value).
	pathReq := httptest.NewRequest(http.MethodPost, "/nornicdb/graph/path", strings.NewReader(`{"source_node_id":"a","target_node_id":"b"}`))
	pathRec := httptest.NewRecorder()
	server.handleGraphPath(pathRec, pathReq)
	require.Equal(t, http.StatusBadRequest, pathRec.Code)
	require.Contains(t, pathRec.Body.String(), "database path parameter is required")

	// handleGraphNeighborhood: collection error branch via canceled context.
	engine := getDefaultStorage(t, server)
	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nbReq := httptest.NewRequest(http.MethodPost, "/nornicdb/graph/neighborhood", strings.NewReader(`{"node_ids":["a"],"depth":1}`)).WithContext(
		context.WithValue(ctx, contextKeyClaims, &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}}),
	)
	nbReq.SetPathValue("database", server.dbManager.DefaultDatabaseName())
	nbRec := httptest.NewRecorder()
	server.handleGraphNeighborhood(nbRec, nbReq)
	require.Equal(t, http.StatusInternalServerError, nbRec.Code)
}

func TestCollectLatestNeighborhood_CanceledInsideEdgeLoop(t *testing.T) {
	server := &Server{}
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	engine := &graphNeighborhoodStubEngine{
		graphEdgesStubEngine: &graphEdgesStubEngine{
			Engine: base,
			outgoing: []*storage.Edge{
				{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"},
			},
			incoming: []*storage.Edge{},
			cancelIn: cancel,
		},
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a", Labels: []string{"Node"}},
			"b": {ID: "b", Labels: []string{"Node"}},
		},
	}

	_, err := server.collectLatestNeighborhood(ctx, engine, []string{"a"}, 1, 10, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, context.Canceled)
}
