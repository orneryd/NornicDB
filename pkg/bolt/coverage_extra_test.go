package bolt

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticDBAccessMode struct {
	visible map[string]bool
	access  map[string]bool
}

func (m staticDBAccessMode) CanSeeDatabase(dbName string) bool {
	return m.visible[dbName]
}

func (m staticDBAccessMode) CanAccessDatabase(dbName string) bool {
	return m.access[dbName]
}

type txExecutorMock struct {
	mockExecutor
	rollbackErr   error
	rollbackCalls int
}

func (m *txExecutorMock) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	return nil
}

func (m *txExecutorMock) CommitTransaction(ctx context.Context) error {
	return nil
}

func (m *txExecutorMock) RollbackTransaction(ctx context.Context) error {
	m.rollbackCalls++
	return m.rollbackErr
}

type overrideDBManager struct {
	store     storage.Engine
	getErr    error
	defaultDB string
	exists    bool
}

func (m *overrideDBManager) GetStorage(name string) (storage.Engine, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.store, nil
}

func (m *overrideDBManager) Exists(name string) bool {
	return m.exists
}

func (m *overrideDBManager) DefaultDatabaseName() string {
	return m.defaultDB
}

func buildRunMessageData(query string, params map[string]any, metadata map[string]any) []byte {
	data := encodePackStreamString(query)
	if params == nil {
		params = map[string]any{}
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	data = append(data, encodePackStreamMap(params)...)
	data = append(data, encodePackStreamMap(metadata)...)
	return data
}

func newBoltCoverageAuthenticator(t *testing.T) *auth.Authenticator {
	t.Helper()

	cfg := auth.DefaultAuthConfig()
	cfg.JWTSecret = []byte("bolt-coverage-secret-key-123456789")
	cfg.SecurityEnabled = true

	authenticator, err := auth.NewAuthenticator(cfg, storage.NewMemoryEngine())
	require.NoError(t, err)

	_, err = authenticator.CreateUser("tester", "secret-pass", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)

	return authenticator
}

func TestBoltCoverage_AuthenticatorPermissionsCallback(t *testing.T) {
	authenticator := newBoltCoverageAuthenticator(t)

	t.Run("fills permissions for anonymous auth", func(t *testing.T) {
		adapter := NewAuthenticatorAdapterWithAnonymous(authenticator)
		adapter.SetGetEffectivePermissions(func(roles []string) []string {
			assert.Equal(t, []string{"viewer"}, roles)
			return []string{"read", "admin"}
		})

		result, err := adapter.Authenticate("none", "", "")
		require.NoError(t, err)
		assert.Equal(t, []string{"read", "admin"}, result.Permissions)
		assert.True(t, result.HasPermission("write"))
	})

	t.Run("fills permissions for cached basic auth", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		callCount := 0
		adapter.SetGetEffectivePermissions(func(roles []string) []string {
			callCount++
			assert.Equal(t, []string{"viewer"}, roles)
			return []string{"read"}
		})

		first, err := adapter.Authenticate("basic", "tester", "secret-pass")
		require.NoError(t, err)
		second, err := adapter.Authenticate("basic", "tester", "secret-pass")
		require.NoError(t, err)

		assert.Equal(t, []string{"read"}, first.Permissions)
		assert.Equal(t, []string{"read"}, second.Permissions)
		assert.GreaterOrEqual(t, callCount, 2)
	})
}

func TestBoltCoverage_ServerSettersAndBookmarkHelpers(t *testing.T) {
	server := New(nil, &mockExecutor{})

	server.SetDatabaseAccessMode(auth.FullDatabaseAccessMode)
	require.NotNil(t, server.databaseAccessMode)
	assert.True(t, server.databaseAccessMode.CanAccessDatabase("any"))

	server.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		assert.Equal(t, []string{"viewer"}, roles)
		return auth.DenyAllDatabaseAccessMode
	})
	require.NotNil(t, server.databaseAccessModeResolver)
	assert.False(t, server.databaseAccessModeResolver([]string{"viewer"}).CanAccessDatabase("db"))

	server.SetResolvedAccessResolver(func(roles []string, dbName string) auth.ResolvedAccess {
		assert.Equal(t, []string{"editor"}, roles)
		assert.Equal(t, "graph", dbName)
		return auth.ResolvedAccess{Read: true, Write: true}
	})
	require.NotNil(t, server.resolvedAccessResolver)
	assert.Equal(t, auth.ResolvedAccess{Read: true, Write: true}, server.resolvedAccessResolver([]string{"editor"}, "graph"))

	session := &Session{server: server}
	session.updateBookmarkSequence(5)
	session.updateBookmarkSequence(3)
	assert.Equal(t, int64(5), server.txSequence)

	session.server = nil
	session.updateBookmarkSequence(10)

	t.Run("receipt variations", func(t *testing.T) {
		cases := []struct {
			name     string
			receipt  any
			expected string
			ok       bool
		}{
			{name: "nil result", receipt: nil, expected: "", ok: false},
			{name: "pointer receipt", receipt: &storage.Receipt{WALSeqEnd: 7}, expected: formatBookmark(7), ok: true},
			{name: "value receipt", receipt: storage.Receipt{WALSeqEnd: 8}, expected: formatBookmark(8), ok: true},
			{name: "map float receipt", receipt: map[string]interface{}{"wal_seq_end": float64(9)}, expected: formatBookmark(9), ok: true},
			{name: "map int receipt", receipt: map[string]interface{}{"wal_seq_end": int64(10)}, expected: formatBookmark(10), ok: true},
			{name: "zero receipt", receipt: map[string]interface{}{"wal_seq_end": uint64(0)}, expected: "", ok: false},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := &Session{server: New(nil, &mockExecutor{})}
				if tc.receipt != nil {
					s.lastResult = &QueryResult{Metadata: map[string]any{"receipt": tc.receipt}}
				}

				bookmark, ok := s.bookmarkFromReceipt()
				assert.Equal(t, tc.ok, ok)
				assert.Equal(t, tc.expected, bookmark)
			})
		}
	})
}

func TestBoltCoverage_ReadMessageAndProcessMessage(t *testing.T) {
	t.Run("reads chunked struct message", func(t *testing.T) {
		conn := &mockConn{
			readData: []byte{
				0x00, 0x01, 0xB0,
				0x00, 0x01, MsgReset,
				0x00, 0x00,
			},
		}
		session := newTestSession(conn, &mockExecutor{})

		msg, err := session.readMessage()
		require.NoError(t, err)
		require.NotNil(t, msg)
		assert.Equal(t, MsgReset, msg.msgType)
		assert.Empty(t, msg.data)
	})

	t.Run("returns nil for empty message", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x00, 0x00}}
		session := newTestSession(conn, &mockExecutor{})

		msg, err := session.readMessage()
		require.NoError(t, err)
		assert.Nil(t, msg)
	})

	t.Run("rejects too short message", func(t *testing.T) {
		conn := &mockConn{
			readData: []byte{
				0x00, 0x01, 0xB0,
				0x00, 0x00,
			},
		}
		session := newTestSession(conn, &mockExecutor{})

		msg, err := session.readMessage()
		assert.Nil(t, msg)
		require.Error(t, err)
		require.ErrorContains(t, err, "message too short")
	})

	t.Run("processes reset messages", func(t *testing.T) {
		conn := &mockConn{}
		session := newTestSession(conn, &mockExecutor{})
		session.inTransaction = true
		session.txMetadata = map[string]any{"tx": "meta"}
		primeTestTransactionLifecycle(t, session)
		session.lastResult = &QueryResult{Rows: [][]any{{"row"}}}
		session.resultIndex = 4

		err := session.processMessage(&boltMessage{msgType: MsgReset})
		require.NoError(t, err)
		assert.False(t, session.inTransaction)
		assert.Nil(t, session.txMetadata)
		assert.Nil(t, session.lastResult)
		assert.Zero(t, session.resultIndex)
		assert.NotEmpty(t, conn.writeData)
	})
}

func TestBoltCoverage_SendRecordsBatched(t *testing.T) {
	conn := &mockConn{}
	session := newTestSession(conn, &mockExecutor{})

	require.NoError(t, session.sendRecordsBatched(nil))

	rows := [][]any{{"alpha", int64(1)}, {"beta", int64(2)}}
	require.NoError(t, session.sendRecordsBatched(rows))
	assert.Greater(t, session.writer.Buffered(), 0)
	require.NoError(t, session.writer.Flush())
	assert.NotEmpty(t, conn.writeData)
	assert.Len(t, session.recordBuf, 0)
}

func TestBoltCoverage_PackStreamPathAndWrapperHelpers(t *testing.T) {
	nodeA := &storage.Node{
		ID:         "node-a",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	}
	nodeB := &storage.Node{
		ID:         "node-b",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Bob"},
	}
	edge := &storage.Edge{
		ID:         "edge-ab",
		StartNode:  nodeA.ID,
		EndNode:    nodeB.ID,
		Type:       "KNOWS",
		Properties: map[string]any{"since": int64(2020)},
	}

	t.Run("map encoding skips internal path sentinel", func(t *testing.T) {
		data := encodePackStreamMapInto(nil, map[string]any{
			"_pathResult": true,
			"name":        "Alice",
		})

		decoded, _, err := decodePackStreamMap(data, 0)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"name": "Alice"}, decoded)
	})

	t.Run("direct wrappers match into variants", func(t *testing.T) {
		nodeMap := map[string]any{"_nodeId": "node-a", "labels": []string{"Person"}, "name": "Alice"}
		assert.Equal(t, encodeNode("node-a", []string{"Person"}, nodeMap), encodeNodeInto(nil, "node-a", []string{"Person"}, nodeMap))
		assert.Equal(t, encodeStorageNode(nodeA), encodeStorageNodeInto(nil, nodeA))
		assert.Equal(t, encodeStorageEdge(edge), encodeStorageEdgeInto(nil, edge))

		rel := &unboundRelationship{id: edge.ID, relType: edge.Type, properties: edge.Properties}
		data := encodeUnboundRelationshipInto(nil, rel)
		require.GreaterOrEqual(t, len(data), 2)
		assert.Equal(t, []byte{0xB3, 0x72}, data[:2])
	})

	t.Run("path helpers preserve node and relationship order", func(t *testing.T) {
		uniqueNodes, nodeIndex := uniquePathNodes([]*storage.Node{nodeA, nil, nodeB, nodeA})
		require.Len(t, uniqueNodes, 2)
		assert.Equal(t, 0, nodeIndex[nodeA.ID])
		assert.Equal(t, 1, nodeIndex[nodeB.ID])

		uniqueRels, relIndex := uniquePathRels([]*storage.Edge{edge, nil, edge})
		require.Len(t, uniqueRels, 1)
		assert.Equal(t, 0, relIndex[edge.ID])

		seq := buildPathSequence([]*storage.Node{nodeA, nodeB}, []*storage.Edge{edge}, nodeIndex, relIndex)
		assert.Equal(t, []int64{1, 1}, seq)

		reverse := buildPathSequence([]*storage.Node{nodeB, nodeA}, []*storage.Edge{edge}, map[storage.NodeID]int{nodeB.ID: 0, nodeA.ID: 1}, relIndex)
		assert.Equal(t, []int64{-1, 1}, reverse)

		assert.Empty(t, buildPathSequence([]*storage.Node{nodeA}, []*storage.Edge{edge}, nodeIndex, relIndex))
		assert.Empty(t, buildPathSequence([]*storage.Node{nodeA, nil}, []*storage.Edge{edge}, nodeIndex, relIndex))

		encoded := encodePathInto(nil, []*storage.Node{nodeA, nodeB}, []*storage.Edge{edge})
		require.GreaterOrEqual(t, len(encoded), 2)
		assert.Equal(t, []byte{0xB3, 0x50}, encoded[:2])

		emptyPath := encodePathInto(nil, nil, nil)
		assert.Equal(t, []byte{0xB3, 0x50, 0x90, 0x90, 0x90}, emptyPath)
	})

	t.Run("extracts and coerces path maps", func(t *testing.T) {
		path, ok := extractPathFromMap(map[string]any{"_pathResult": cypher.PathResult{
			Nodes:         []*storage.Node{nodeA, nodeB},
			Relationships: []*storage.Edge{edge},
			Length:        1,
		}})
		require.True(t, ok)
		require.NotNil(t, path)
		assert.Len(t, path.Nodes, 2)

		path, ok = extractPathFromMap(map[string]any{
			"_pathResult":   true,
			"nodes":         []any{*nodeA, nodeB},
			"relationships": []any{*edge},
		})
		require.True(t, ok)
		require.NotNil(t, path)
		assert.Len(t, path.Nodes, 2)
		assert.Len(t, path.Relationships, 1)

		path, ok = extractPathFromMap(map[string]any{
			"_pathResult": true,
			"nodes":       []storage.Node{*nodeA},
			"rels":        []storage.Edge{*edge},
		})
		require.True(t, ok)
		require.NotNil(t, path)
		assert.Len(t, path.Relationships, 1)

		_, ok = extractPathFromMap(map[string]any{"_pathResult": true})
		assert.False(t, ok)

		assert.Len(t, coercePathNodes([]any{*nodeA, nodeB, "skip"}), 2)
		assert.Len(t, coercePathRels([]any{*edge, edge, "skip"}), 2)
		assert.Nil(t, coercePathNodes("not-a-list"))
		assert.Nil(t, coercePathRels("not-a-list"))
	})
}

func TestBoltCoverage_PackStreamValueIntoSpecializedTypes(t *testing.T) {
	now := time.Unix(1700000000, 123000000).UTC()
	node := storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}}
	edge := storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}}

	cases := []struct {
		name   string
		value  any
		marker byte
	}{
		{name: "bytes", value: []byte{0x01, 0x02}, marker: 0xCC},
		{name: "node id", value: storage.NodeID("node-1"), marker: 0x80 + byte(len("node-1"))},
		{name: "edge id", value: storage.EdgeID("edge-1"), marker: 0x80 + byte(len("edge-1"))},
		{name: "string map", value: map[string]string{"k": "v"}, marker: 0xA1},
		{name: "node slice", value: []storage.Node{node}, marker: 0x91},
		{name: "node ptr slice", value: []*storage.Node{&node}, marker: 0x91},
		{name: "edge slice", value: []storage.Edge{edge}, marker: 0x91},
		{name: "edge ptr slice", value: []*storage.Edge{&edge}, marker: 0x91},
		{name: "string slice", value: []string{"a", "b"}, marker: 0x92},
		{name: "int slice", value: []int{1, 2}, marker: 0x92},
		{name: "int64 slice", value: []int64{1, 2}, marker: 0x92},
		{name: "float64 slice", value: []float64{1.5, 2.5}, marker: 0x92},
		{name: "float32 slice", value: []float32{1.5, 2.5}, marker: 0x92},
		{name: "map slice", value: []map[string]any{{"k": "v"}}, marker: 0x91},
		{name: "time", value: now, marker: 0xB3},
		{name: "duration", value: 1500 * time.Millisecond, marker: 0xC9},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := encodePackStreamValueInto(nil, tc.value)
			require.NotEmpty(t, data)
			assert.Equal(t, tc.marker, data[0])
		})
	}
}

func TestBoltCoverage_PackStreamStringAndMapDecodeErrors(t *testing.T) {
	t.Run("decode string truncation", func(t *testing.T) {
		_, _, err := decodePackStreamString([]byte{0xD0, 0x05, 'h', 'i'}, 0)
		require.Error(t, err)
	})

	t.Run("decode map truncation", func(t *testing.T) {
		_, _, err := decodePackStreamMap([]byte{0xA1, 0x81, 'k'}, 0)
		require.Error(t, err)
	})

	t.Run("encode and decode list round trip", func(t *testing.T) {
		data := encodePackStreamListInto(nil, []any{"x", int64(2), true})
		value, _, err := decodePackStreamValue(data, 0)
		require.NoError(t, err)
		assert.Equal(t, []any{"x", int64(2), true}, value)
	})
}

func TestBoltCoverage_PackStreamBoundaryMarkers(t *testing.T) {
	t.Run("string and map encoders use larger markers", func(t *testing.T) {
		s16 := make([]byte, 300)
		for i := range s16 {
			s16[i] = 'a'
		}
		data := encodePackStreamStringInto(nil, string(s16))
		require.NotEmpty(t, data)
		assert.Equal(t, byte(0xD1), data[0])

		s32 := make([]byte, 70000)
		for i := range s32 {
			s32[i] = 'b'
		}
		data = encodePackStreamStringInto(nil, string(s32))
		require.NotEmpty(t, data)
		assert.Equal(t, byte(0xD2), data[0])

		map8 := make(map[string]any, 16)
		for i := 0; i < 16; i++ {
			map8[string(rune('a'+i))] = int64(i)
		}
		data = encodePackStreamMapInto(nil, map8)
		require.NotEmpty(t, data)
		assert.Equal(t, byte(0xD8), data[0])

		map16 := make(map[string]any, 256)
		for i := 0; i < 256; i++ {
			map16[string([]byte{'k', byte(i)})] = int64(i)
		}
		data = encodePackStreamMapInto(nil, map16)
		require.NotEmpty(t, data)
		assert.Equal(t, byte(0xD9), data[0])
	})

	t.Run("map and list decoders support larger payload markers", func(t *testing.T) {
		map8 := []byte{0xD8, 0x01, 0x81, 'k', 0x2A}
		decodedMap, _, err := decodePackStreamMap(map8, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(42), decodedMap["k"])

		map16 := []byte{0xD9, 0x00, 0x01, 0x81, 'z', 0x2B}
		decodedMap, _, err = decodePackStreamMap(map16, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(43), decodedMap["z"])

		list16 := []byte{0xD5, 0x00, 0x01, 0x2A}
		decodedList, _, err := decodePackStreamList(list16, 0)
		require.NoError(t, err)
		assert.Equal(t, []any{int64(42)}, decodedList)

		list32 := []byte{0xD6, 0x00, 0x00, 0x00, 0x01, 0x2B}
		decodedList, _, err = decodePackStreamList(list32, 0)
		require.NoError(t, err)
		assert.Equal(t, []any{int64(43)}, decodedList)
	})
}

func TestBoltCoverage_PackStreamValueAndDecodeBranches(t *testing.T) {
	t.Run("encodePackStreamValueInto covers specialized and fallback cases", func(t *testing.T) {
		node := storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}}
		edge := storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}}
		nodePtr := &node
		rel := &unboundRelationship{id: edge.ID, relType: edge.Type, properties: edge.Properties}
		largeStrings := make([]string, 16)
		largeInts := make([]int, 16)
		largeMaps := make([]map[string]any, 16)
		for i := range largeStrings {
			largeStrings[i] = "x"
			largeInts[i] = i
			largeMaps[i] = map[string]any{"i": int64(i)}
		}

		prevHook := packstreamFallbackHook
		var fallbackSeen bool
		packstreamFallbackHook = func(v any) { fallbackSeen = true }
		t.Cleanup(func() { packstreamFallbackHook = prevHook })

		cases := []struct {
			name   string
			value  any
			marker byte
		}{
			{name: "bool false", value: false, marker: 0xC2},
			{name: "int8", value: int8(2), marker: 0x02},
			{name: "int16", value: int16(3), marker: 0x03},
			{name: "int32", value: int32(4), marker: 0x04},
			{name: "uint", value: uint(5), marker: 0x05},
			{name: "uint8", value: uint8(6), marker: 0x06},
			{name: "uint16", value: uint16(7), marker: 0x07},
			{name: "uint32", value: uint32(8), marker: 0x08},
			{name: "uint64", value: uint64(9), marker: 0x09},
			{name: "float32", value: float32(1.5), marker: 0xC1},
			{name: "map path result", value: map[string]any{"_pathResult": cypher.PathResult{Nodes: []*storage.Node{nodePtr}, Relationships: []*storage.Edge{}, Length: 0}}, marker: 0xB3},
			{name: "map node structure", value: map[string]any{"_nodeId": "n1", "labels": []string{"Person"}, "name": "Alice"}, marker: 0xB3},
			{name: "string map empty", value: map[string]string{}, marker: 0xA0},
			{name: "string map large", value: func() map[string]string {
				m := map[string]string{}
				for i := 0; i < 16; i++ {
					m[string(rune('a'+i))] = "v"
				}
				return m
			}(), marker: 0xD8},
			{name: "storage node value", value: node, marker: 0xB3},
			{name: "storage node nil ptr", value: (*storage.Node)(nil), marker: 0xC0},
			{name: "storage edge value", value: edge, marker: 0xB5},
			{name: "storage edge nil ptr", value: (*storage.Edge)(nil), marker: 0xC0},
			{name: "unbound relationship value", value: *rel, marker: 0xB3},
			{name: "unbound relationship nil ptr", value: (*unboundRelationship)(nil), marker: 0xC0},
			{name: "storage node slice empty", value: []storage.Node{}, marker: 0x90},
			{name: "storage edge slice empty", value: []storage.Edge{}, marker: 0x90},
			{name: "node ptr slice empty", value: []*storage.Node{}, marker: 0x90},
			{name: "edge ptr slice empty", value: []*storage.Edge{}, marker: 0x90},
			{name: "string slice empty", value: []string{}, marker: 0x90},
			{name: "string slice large", value: largeStrings, marker: 0xD4},
			{name: "int slice empty", value: []int{}, marker: 0x90},
			{name: "int slice large", value: largeInts, marker: 0xD4},
			{name: "int64 slice empty", value: []int64{}, marker: 0x90},
			{name: "int64 slice large", value: make([]int64, 16), marker: 0xD4},
			{name: "float64 slice empty", value: []float64{}, marker: 0x90},
			{name: "float64 slice large", value: make([]float64, 16), marker: 0xD4},
			{name: "float32 slice empty", value: []float32{}, marker: 0x90},
			{name: "float32 slice large", value: make([]float32, 16), marker: 0xD4},
			{name: "map slice empty", value: []map[string]any{}, marker: 0x90},
			{name: "map slice large", value: largeMaps, marker: 0xD4},
			{name: "fallback", value: struct{ X int }{X: 1}, marker: 0xC0},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				data := encodePackStreamValueInto(nil, tc.value)
				require.NotEmpty(t, data)
				assert.Equal(t, tc.marker, data[0])
			})
		}

		assert.True(t, fallbackSeen)
	})

	t.Run("encodePackStreamValue covers legacy scalar and collection branches", func(t *testing.T) {
		node := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}}
		edge := &storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}}
		nodeMap := map[string]any{"_nodeId": "n1", "labels": []string{"Person"}, "name": "Alice"}

		cases := []struct {
			name   string
			value  any
			marker byte
		}{
			{name: "int", value: int(5), marker: 0x05},
			{name: "int8", value: int8(6), marker: 0x06},
			{name: "int16", value: int16(7), marker: 0x07},
			{name: "int32", value: int32(8), marker: 0x08},
			{name: "uint", value: uint(9), marker: 0x09},
			{name: "uint8", value: uint8(10), marker: 0x0A},
			{name: "uint16", value: uint16(11), marker: 0x0B},
			{name: "uint32", value: uint32(12), marker: 0x0C},
			{name: "uint64", value: uint64(13), marker: 0x0D},
			{name: "float32", value: float32(1.25), marker: 0xC1},
			{name: "string slice", value: []string{"a", "b"}, marker: 0x92},
			{name: "any slice", value: []any{"a", int64(1)}, marker: 0x92},
			{name: "int slice", value: []int{1, 2}, marker: 0x92},
			{name: "int64 slice", value: []int64{1, 2}, marker: 0x92},
			{name: "float64 slice", value: []float64{1.2, 2.3}, marker: 0x92},
			{name: "float32 slice", value: []float32{1.2, 2.3}, marker: 0x92},
			{name: "map slice", value: []map[string]any{{"k": "v"}}, marker: 0x91},
			{name: "plain map", value: map[string]any{"k": "v"}, marker: 0xA1},
			{name: "node map", value: nodeMap, marker: 0xB3},
			{name: "node", value: node, marker: 0xB3},
			{name: "edge", value: edge, marker: 0xB5},
			{name: "unknown", value: struct{}{}, marker: 0xC0},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				data := encodePackStreamValue(tc.value)
				require.NotEmpty(t, data)
				assert.Equal(t, tc.marker, data[0])
			})
		}

		assert.Equal(t, []byte{0xC0}, encodePackStreamValue((*storage.Node)(nil)))
		assert.Equal(t, []byte{0xC0}, encodePackStreamValue((*storage.Edge)(nil)))
	})

	t.Run("node and relationship into helpers cover remaining branches", func(t *testing.T) {
		largeLabels := make([]string, 16)
		for i := range largeLabels {
			largeLabels[i] = "L"
		}

		data := encodeNodeInto(nil, int64(5), []string{}, map[string]any{})
		require.GreaterOrEqual(t, len(data), 4)
		assert.Equal(t, []byte{0xB3, 0x4E}, data[:2])
		assert.Equal(t, byte(0x90), data[3]) // empty labels

		data = encodeNodeInto(nil, 7, []any{"A", "B"}, map[string]any{"name": "Alice"})
		assert.Equal(t, []byte{0xB3, 0x4E}, data[:2])

		data = encodeNodeInto(nil, "node-x", largeLabels, map[string]any{"name": "Alice"})
		assert.Equal(t, []byte{0xB3, 0x4E}, data[:2])

		data = encodeNodeInto(nil, 3.14, "bad-labels", map[string]any{"_nodeId": "skip", "labels": "skip"})
		assert.Equal(t, []byte{0xB3, 0x4E}, data[:2])

		node := &storage.Node{ID: "n1", Labels: largeLabels, Properties: map[string]any{}}
		data = encodeStorageNodeInto(nil, node)
		assert.Equal(t, []byte{0xB3, 0x4E}, data[:2])

		node.Properties = map[string]any{"name": "Alice"}
		data = encodeStorageNodeInto(nil, node)
		assert.Equal(t, []byte{0xB3, 0x4E}, data[:2])

		edge := &storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]any{}}
		data = encodeStorageEdgeInto(nil, edge)
		assert.Equal(t, []byte{0xB5, 0x52}, data[:2])
	})

	t.Run("decode helpers cover remaining error and structure branches", func(t *testing.T) {
		_, _, err := decodePackStreamString(nil, 0)
		require.ErrorContains(t, err, "offset out of bounds")
		_, _, err = decodePackStreamString([]byte{0xD1, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete STRING16")
		_, _, err = decodePackStreamString([]byte{0xD2, 0x00, 0x00, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete STRING32")
		_, _, err = decodePackStreamString([]byte{0x01}, 0)
		require.ErrorContains(t, err, "not a string marker")

		_, _, err = decodePackStreamMap(nil, 0)
		require.ErrorContains(t, err, "offset out of bounds")
		_, _, err = decodePackStreamMap([]byte{0xD8}, 0)
		require.ErrorContains(t, err, "incomplete MAP8")
		_, _, err = decodePackStreamMap([]byte{0xD9, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete MAP16")
		_, _, err = decodePackStreamMap([]byte{0x90}, 0)
		require.ErrorContains(t, err, "not a map marker")
		_, _, err = decodePackStreamMap([]byte{0xA1, 0x81, 'k', 0xCC}, 0)
		require.ErrorContains(t, err, "failed to decode map value")

		_, _, err = decodePackStreamValue(nil, 0)
		require.ErrorContains(t, err, "offset out of bounds")
		_, _, err = decodePackStreamValue([]byte{0xC8}, 0)
		require.ErrorContains(t, err, "incomplete INT8")
		_, _, err = decodePackStreamValue([]byte{0xC9, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete INT16")
		_, _, err = decodePackStreamValue([]byte{0xCA, 0x00, 0x00, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete INT32")
		_, _, err = decodePackStreamValue([]byte{0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete INT64")
		_, _, err = decodePackStreamValue([]byte{0xC1, 0x00, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete Float64")
		_, _, err = decodePackStreamValue([]byte{0xCC}, 0)
		require.ErrorContains(t, err, "incomplete BYTES8")
		_, _, err = decodePackStreamValue([]byte{0xCD, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete BYTES16")
		_, _, err = decodePackStreamValue([]byte{0xCE, 0x00, 0x00, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete BYTES32")

		struct8 := []byte{0xDC, 0x01, 0x01, 0x81, 'x'}
		val, _, err := decodePackStreamValue(struct8, 0)
		require.NoError(t, err)
		generic, ok := val.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Structure_0x01", generic["_type"])

		struct16 := []byte{0xDD, 0x00, 0x01, 0x01, 0x81, 'y'}
		val, _, err = decodePackStreamValue(struct16, 0)
		require.NoError(t, err)
		generic, ok = val.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, byte(0x01), generic["signature"])

		_, _, err = decodePackStreamValue([]byte{0xFF - 0x10}, 0)
		require.ErrorContains(t, err, "unknown marker")

		_, _, err = decodePackStreamStructure(nil, 0)
		require.ErrorContains(t, err, "offset out of bounds")
		_, _, err = decodePackStreamStructure([]byte{0xB1}, 0)
		require.ErrorContains(t, err, "missing signature")

		relVal, consumed, err := decodeStructureFields([]byte{0x01, 0x02}, 0, 2, 0x52)
		require.NoError(t, err)
		assert.Equal(t, 2, consumed)
		relMap := relVal.(map[string]any)
		assert.Equal(t, "Relationship", relMap["_type"])
		assert.Contains(t, relMap, "fields")

		pathVal, consumed, err := decodeStructureFields([]byte{0x01, 0x02}, 0, 2, 0x50)
		require.NoError(t, err)
		assert.Equal(t, 2, consumed)
		pathMap := pathVal.(map[string]any)
		assert.Equal(t, "Path", pathMap["_type"])
		assert.Contains(t, pathMap, "fields")

		_, _, err = decodeStructureFields([]byte{0xCC}, 0, 1, 0x4E)
		require.ErrorContains(t, err, "failed to decode structure field 0")

		_, _, err = decodePackStreamList(nil, 0)
		require.ErrorContains(t, err, "offset out of bounds")
		_, _, err = decodePackStreamList([]byte{0xD4}, 0)
		require.ErrorContains(t, err, "incomplete LIST8")
		_, _, err = decodePackStreamList([]byte{0xD5, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete LIST16")
		_, _, err = decodePackStreamList([]byte{0xD6, 0x00, 0x00, 0x00}, 0)
		require.ErrorContains(t, err, "incomplete LIST32")
		_, _, err = decodePackStreamList([]byte{0xA0}, 0)
		require.ErrorContains(t, err, "not a list marker")
		_, _, err = decodePackStreamList([]byte{0x91, 0xCC}, 0)
		require.ErrorContains(t, err, "failed to decode list item 0")
	})
}

func TestBoltCoverage_ServerMessageAndRunHelpers(t *testing.T) {
	t.Run("handleHello and parseHelloAuth cover auth variants", func(t *testing.T) {
		authenticator := newBoltCoverageAuthenticator(t)
		token, err := authenticator.GenerateClusterToken("node-2", auth.RoleAdmin)
		require.NoError(t, err)

		t.Run("parse hello auth edge cases", func(t *testing.T) {
			session := &Session{}
			params, err := session.parseHelloAuth(nil)
			require.NoError(t, err)
			assert.Empty(t, params["scheme"])

			params, err = session.parseHelloAuth([]byte{0xB1})
			require.NoError(t, err)
			assert.Empty(t, params["database"])

			payload := encodePackStreamMap(map[string]any{
				"scheme":      "basic",
				"principal":   "tester",
				"credentials": "secret-pass",
				"database":    "graph",
			})
			params, err = session.parseHelloAuth(append([]byte{0xB1, MsgHello}, payload...))
			require.NoError(t, err)
			assert.Equal(t, "graph", params["database"])

			payload = encodePackStreamMap(map[string]any{"db": "graph"})
			params, err = session.parseHelloAuth(payload)
			require.NoError(t, err)
			assert.Equal(t, "graph", params["database"])
		})

		makeHelloSession := func() (*Session, net.Conn, net.Conn) {
			serverConn, clientConn := net.Pipe()
			session := newTestSession(serverConn, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			return session, serverConn, clientConn
		}

		t.Run("parse failure and require auth without authenticator", func(t *testing.T) {
			session, serverConn, clientConn := makeHelloSession()
			defer serverConn.Close()
			defer clientConn.Close()

			done := make(chan error, 1)
			go func() { done <- session.handleHello([]byte{0xD8}) }()
			code, _, err := AssertFailure(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Request.Invalid", code)

			serverConn2, clientConn2 := net.Pipe()
			defer serverConn2.Close()
			defer clientConn2.Close()
			session = newTestSession(serverConn2, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.server.config.RequireAuth = true

			done = make(chan error, 1)
			go func() { done <- session.handleHello(encodePackStreamMap(map[string]any{})) }()
			code, _, err = AssertFailure(t, clientConn2)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Security.Unauthorized", code)
		})

		t.Run("anonymous basic bearer and unsupported auth", func(t *testing.T) {
			adapter := NewAuthenticatorAdapterWithAnonymous(authenticator)

			session, serverConn, clientConn := makeHelloSession()
			defer serverConn.Close()
			defer clientConn.Close()
			session.server.config.Authenticator = adapter
			session.server.config.AllowAnonymous = true
			session.server.dbManager = &overrideDBManager{store: storage.NewMemoryEngine(), defaultDB: "graph", exists: true}

			done := make(chan error, 1)
			go func() { done <- session.handleHello(encodePackStreamMap(map[string]any{"scheme": "none"})) }()
			meta, err := AssertSuccess(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "anonymous", session.authResult.Username)
			assert.Equal(t, "graph", session.database)
			assert.Equal(t, buildinfo.ServerAnnouncement(), meta["server"])

			serverConn2, clientConn2 := net.Pipe()
			defer serverConn2.Close()
			defer clientConn2.Close()
			session = newTestSession(serverConn2, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.server.config.Authenticator = NewAuthenticatorAdapter(authenticator)
			session.server.config.LogQueries = true
			session.server.dbManager = &overrideDBManager{store: storage.NewMemoryEngine(), defaultDB: "graph", exists: true}

			done = make(chan error, 1)
			go func() {
				done <- session.handleHello(encodePackStreamMap(map[string]any{
					"scheme":      "basic",
					"principal":   "tester",
					"credentials": "secret-pass",
				}))
			}()
			_, err = AssertSuccess(t, clientConn2)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.True(t, session.authenticated)

			serverConn3, clientConn3 := net.Pipe()
			defer serverConn3.Close()
			defer clientConn3.Close()
			session = newTestSession(serverConn3, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.server.config.Authenticator = NewAuthenticatorAdapter(authenticator)

			done = make(chan error, 1)
			go func() {
				done <- session.handleHello(encodePackStreamMap(map[string]any{
					"scheme":      "bearer",
					"credentials": token,
					"db":          "graph",
				}))
			}()
			_, err = AssertSuccess(t, clientConn3)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.True(t, session.authenticated)
			assert.Equal(t, "graph", session.database)

			serverConn4, clientConn4 := net.Pipe()
			defer serverConn4.Close()
			defer clientConn4.Close()
			session = newTestSession(serverConn4, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.server.config.Authenticator = adapter

			done = make(chan error, 1)
			go func() { done <- session.handleHello(encodePackStreamMap(map[string]any{"scheme": "kerberos"})) }()
			code, _, err := AssertFailure(t, clientConn4)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Security.Unauthorized", code)
		})

		t.Run("custom server announcement override", func(t *testing.T) {
			session, serverConn, clientConn := makeHelloSession()
			defer serverConn.Close()
			defer clientConn.Close()
			session.server.config.ServerAnnouncement = "Neo4j/5.26.0"

			done := make(chan error, 1)
			go func() { done <- session.handleHello(encodePackStreamMap(map[string]any{"scheme": "none"})) }()
			meta, err := AssertSuccess(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo4j/5.26.0", meta["server"])
		})

		t.Run("database not found and no-auth fallback", func(t *testing.T) {
			session, serverConn, clientConn := makeHelloSession()
			defer serverConn.Close()
			defer clientConn.Close()
			session.server.config.Authenticator = NewAuthenticatorAdapter(authenticator)
			session.server.dbManager = &overrideDBManager{store: storage.NewMemoryEngine(), defaultDB: "graph", exists: false}

			done := make(chan error, 1)
			go func() {
				done <- session.handleHello(encodePackStreamMap(map[string]any{
					"scheme":      "basic",
					"principal":   "tester",
					"credentials": "secret-pass",
					"db":          "missing",
				}))
			}()
			code, _, err := AssertFailure(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", code)

			serverConn2, clientConn2 := net.Pipe()
			defer serverConn2.Close()
			defer clientConn2.Close()
			session = newTestSession(serverConn2, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}

			done = make(chan error, 1)
			go func() { done <- session.handleHello(encodePackStreamMap(map[string]any{})) }()
			_, err = AssertSuccess(t, clientConn2)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, string(auth.RoleAdmin), session.authResult.Roles[0])
		})
	})

	t.Run("database metadata parsing and bookmarks", func(t *testing.T) {
		db, ok := databaseFromMetadata(nil)
		assert.False(t, ok)
		assert.Empty(t, db)

		db, ok = databaseFromMetadata(map[string]any{"db": "  graph  "})
		assert.True(t, ok)
		assert.Equal(t, "graph", db)

		db, ok = databaseFromMetadata(map[string]any{"db": " ", "database": "fallback"})
		assert.True(t, ok)
		assert.Equal(t, "fallback", db)

		db, ok = databaseFromMetadata(map[string]any{"db": 7})
		assert.False(t, ok)
		assert.Empty(t, db)

		s := &Session{server: &Server{config: DefaultConfig()}}
		assert.Equal(t, formatBookmark(1), s.generateBookmark())
		assert.Equal(t, int64(1), s.lastTxSequence)
		s.server.txSequence = -1
		assert.Equal(t, formatBookmark(0), s.currentBookmark())
		assert.Equal(t, formatBookmark(0), (&Session{}).currentBookmark())
		assert.Panics(t, func() { (&Session{}).generateBookmark() })
	})

	t.Run("handleMessage fallback and message routes", func(t *testing.T) {
		conn := &mockConn{readData: []byte{0x00, 0x02, MsgReset, 0x00, 0x00, 0x00}}
		session := newTestSession(conn, &mockExecutor{})
		require.NoError(t, session.handleMessage())

		conn = &mockConn{readData: []byte{0x00, 0x01, 0xB0, 0x00, 0x00}}
		session = newTestSession(conn, &mockExecutor{})
		require.ErrorContains(t, session.handleMessage(), "message too short")

		require.Equal(t, io.EOF, session.dispatchMessage(MsgGoodbye, nil))
	})

	t.Run("discard route and rollback handlers", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		session := newTestSession(serverConn, &mockExecutor{})
		session.lastResult = &QueryResult{Rows: [][]any{{"row"}}}
		session.resultIndex = 3

		done := make(chan error, 1)
		go func() { done <- session.handleDiscard(nil) }()
		meta, err := AssertSuccess(t, clientConn)
		require.NoError(t, err)
		require.NoError(t, <-done)
		// DISCARD now emits Neo4j-compatible completion metadata (type, bookmark, db).
		assert.Contains(t, meta, "type")
		assert.Contains(t, meta, "bookmark")
		assert.Nil(t, session.lastResult)
		assert.Zero(t, session.resultIndex)

		done = make(chan error, 1)
		go func() { done <- session.handleRoute(nil) }()
		meta, err = AssertSuccess(t, clientConn)
		require.NoError(t, err)
		require.NoError(t, <-done)
		assert.Contains(t, meta, "rt")
		rt, ok := meta["rt"].(map[string]any)
		require.True(t, ok, "expected rt metadata map")
		ttl, ok := rt["ttl"].(int64)
		require.True(t, ok, "expected int64 ttl in routing table")
		assert.Greater(t, ttl, int64(0))
		rawServers, ok := rt["servers"].([]any)
		require.True(t, ok, "expected servers list in routing table")
		require.NotEmpty(t, rawServers)
		roles := map[string]bool{}
		for _, entry := range rawServers {
			serverEntry, ok := entry.(map[string]any)
			require.True(t, ok, "expected routing server entry map")
			role, ok := serverEntry["role"].(string)
			require.True(t, ok, "expected routing server role")
			roles[role] = true
			addresses, ok := serverEntry["addresses"].([]any)
			require.True(t, ok, "expected routing server addresses list")
			require.NotEmpty(t, addresses, "expected non-empty addresses for role %s", role)
			for _, addr := range addresses {
				addrStr, ok := addr.(string)
				require.True(t, ok, "expected address string for role %s", role)
				assert.NotEmpty(t, addrStr)
			}
		}
		assert.True(t, roles["ROUTE"], "expected ROUTE role in routing table")
		assert.True(t, roles["READ"], "expected READ role in routing table")
		assert.True(t, roles["WRITE"], "expected WRITE role in routing table")
	})

	t.Run("rollback without transaction succeeds", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		session := newTestSession(serverConn, &mockExecutor{})
		done := make(chan error, 1)
		go func() { done <- session.handleRollback(nil) }()
		_, err := AssertSuccess(t, clientConn)
		require.NoError(t, err)
		require.NoError(t, <-done)
	})

	t.Run("rollback with transactional executor success and failure", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			rollbackErr error
			expectCode  string
		}{
			{name: "success", rollbackErr: nil, expectCode: ""},
			{name: "failure", rollbackErr: io.ErrClosedPipe, expectCode: "Neo.ClientError.Transaction.TransactionRollbackFailed"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				serverConn, clientConn := net.Pipe()
				defer serverConn.Close()
				defer clientConn.Close()

				txExec := &txExecutorMock{rollbackErr: tc.rollbackErr}
				session := newTestSession(serverConn, txExec)
				session.inTransaction = true
				session.txMetadata = map[string]any{"db": "graph"}
				primeTestTransactionLifecycle(t, session)

				done := make(chan error, 1)
				go func() { done <- session.handleRollback(nil) }()

				if tc.expectCode == "" {
					_, err := AssertSuccess(t, clientConn)
					require.NoError(t, err)
				} else {
					code, _, err := AssertFailure(t, clientConn)
					require.NoError(t, err)
					assert.Equal(t, tc.expectCode, code)
				}
				handlerErr := <-done
				if tc.rollbackErr == nil {
					require.NoError(t, handlerErr)
					assert.False(t, session.inTransaction)
					assert.Nil(t, session.txMetadata)
				} else {
					require.Error(t, handlerErr)
					assert.True(t, session.inTransaction)
					assert.NotNil(t, session.txMetadata)
				}
				assert.Equal(t, 1, txExec.rollbackCalls)
			})
		}
	})

	t.Run("flush and chunk framing helpers", func(t *testing.T) {
		conn := &mockConn{}
		session := newTestSession(conn, &mockExecutor{})
		require.NoError(t, session.flushIfPending())

		session.flushPending = true
		require.NoError(t, session.flushIfPending())
		assert.False(t, session.flushPending)

		large := make([]byte, 70000)
		for i := range large {
			large[i] = byte(i)
		}
		require.NoError(t, session.writeMessageNoFlush(large))
		require.NoError(t, session.writer.Flush())
		require.Greater(t, len(conn.writeData), len(large))
		assert.Equal(t, byte(0xFF), conn.writeData[0])
		assert.Equal(t, byte(0xFF), conn.writeData[1])
		assert.Equal(t, []byte{0x00, 0x00}, conn.writeData[len(conn.writeData)-2:])
	})

	t.Run("database executor lookup and adapter execute", func(t *testing.T) {
		session := &Session{}
		_, err := session.getExecutorForDatabase("graph")
		require.ErrorContains(t, err, "database manager not available")

		session = &Session{server: &Server{dbManager: &mockDBManager{stores: map[string]storage.Engine{}, defaultDB: "graph"}}}
		_, err = session.getExecutorForDatabase("missing")
		require.Error(t, err)

		store := storage.NewMemoryEngine()
		session = &Session{server: &Server{dbManager: &mockDBManager{stores: map[string]storage.Engine{"graph": store}, defaultDB: "graph"}}}
		exec, err := session.getExecutorForDatabase("graph")
		require.NoError(t, err)
		res, err := exec.Execute(context.Background(), "RETURN 1", map[string]any{"x": 1})
		require.NoError(t, err)
		require.NotNil(t, res)
	})

	t.Run("handleRun covers auth, permissions, routing, filtering, and executor errors", func(t *testing.T) {
		makeSession := func(executor QueryExecutor) (*Session, net.Conn, net.Conn) {
			serverConn, clientConn := net.Pipe()
			s := newTestSession(serverConn, executor)
			s.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}, executor: executor}
			return s, serverConn, clientConn
		}

		t.Run("requires authentication", func(t *testing.T) {
			session, serverConn, clientConn := makeSession(&mockExecutor{})
			defer serverConn.Close()
			defer clientConn.Close()
			session.server.config.RequireAuth = true

			done := make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("RETURN 1", nil, nil)) }()
			code, _, err := AssertFailure(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Security.Unauthorized", code)
		})

		t.Run("denies write and schema without permissions", func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				query   string
				message string
			}{
				{name: "write", query: "CREATE (n)", message: "Write operations require write permission"},
				{name: "schema", query: "CREATE INDEX ON :Person(name)", message: "Schema operations require schema permission"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					session, serverConn, clientConn := makeSession(&mockExecutor{})
					defer serverConn.Close()
					defer clientConn.Close()
					session.authenticated = true
					session.authResult = &BoltAuthResult{Authenticated: true, Username: "viewer", Permissions: []string{"read"}}

					done := make(chan error, 1)
					go func() { done <- session.handleRun(buildRunMessageData(tc.query, nil, nil)) }()
					code, msg, err := AssertFailure(t, clientConn)
					require.NoError(t, err)
					require.NoError(t, <-done)
					assert.Equal(t, "Neo.ClientError.Security.Forbidden", code)
					assert.Contains(t, msg, tc.message)
				})
			}
		})

		t.Run("denies database access and missing database", func(t *testing.T) {
			session, serverConn, clientConn := makeSession(&mockExecutor{})
			defer serverConn.Close()
			defer clientConn.Close()
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Permissions: []string{"read"}}
			session.server.config.RequireAuth = true
			session.server.databaseAccessMode = staticDBAccessMode{access: map[string]bool{"allowed": false}}

			done := make(chan error, 1)
			go func() {
				done <- session.handleRun(buildRunMessageData("RETURN 1", nil, map[string]any{"db": "allowed"}))
			}()
			code, msg, err := AssertFailure(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Security.Forbidden", code)
			assert.Contains(t, msg, "Access to database 'allowed' is not allowed")

			serverConn2, clientConn2 := net.Pipe()
			defer serverConn2.Close()
			defer clientConn2.Close()
			session = newTestSession(serverConn2, &mockExecutor{})
			session.server = &Server{
				config:   DefaultConfig(),
				sessions: map[string]*Session{},
				dbManager: &mockDBManager{
					stores:    map[string]storage.Engine{"graph": storage.NewMemoryEngine()},
					defaultDB: "graph",
				},
			}
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Permissions: []string{"read"}}

			done = make(chan error, 1)
			go func() {
				done <- session.handleRun(buildRunMessageData("RETURN 1", nil, map[string]any{"db": "missing"}))
			}()
			code, _, err = AssertFailure(t, clientConn2)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", code)
		})

		t.Run("denies resolved write and filters show databases", func(t *testing.T) {
			session, serverConn, clientConn := makeSession(&mockExecutor{
				executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
					if query == "SHOW DATABASES" {
						return &QueryResult{
							Columns: []string{"name"},
							Rows:    [][]any{{"allowed"}, {"hidden"}},
						}, nil
					}
					return &QueryResult{Columns: []string{"n"}, Rows: [][]any{{"ok"}}}, nil
				},
			})
			defer serverConn.Close()
			defer clientConn.Close()
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "editor", Roles: []string{"editor"}, Permissions: []string{"read", "write"}}
			session.server.databaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
				return staticDBAccessMode{
					visible: map[string]bool{"allowed": true},
					access:  map[string]bool{"nornic": true, "allowed": true},
				}
			}

			done := make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("SHOW DATABASES", nil, nil)) }()
			meta, err := AssertSuccess(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, []any{"name"}, meta["fields"])
			require.NotNil(t, session.lastResult)
			require.Len(t, session.lastResult.Rows, 1)
			assert.Equal(t, "allowed", session.lastResult.Rows[0][0])

			serverConn2, clientConn2 := net.Pipe()
			defer serverConn2.Close()
			defer clientConn2.Close()
			session = newTestSession(serverConn2, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "editor", Roles: []string{"editor"}, Permissions: []string{"read", "write"}}
			session.server.resolvedAccessResolver = func(roles []string, dbName string) auth.ResolvedAccess {
				return auth.ResolvedAccess{Read: true, Write: false}
			}

			done = make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("CREATE (n)", nil, nil)) }()
			code, msg, err := AssertFailure(t, clientConn2)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Security.Forbidden", code)
			assert.Contains(t, msg, "Write on database 'nornic' is not allowed")
		})

		t.Run("returns executor error and success metadata", func(t *testing.T) {
			session, serverConn, clientConn := makeSession(&mockExecutor{
				executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
					if query == "RETURN fail" {
						return nil, io.ErrUnexpectedEOF
					}
					return &QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}, nil
				},
			})
			defer serverConn.Close()
			defer clientConn.Close()
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Permissions: []string{"read", "write", "schema"}}

			done := make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("RETURN fail", nil, nil)) }()
			code, _, err := AssertFailure(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)

			serverConnWrapped, clientConnWrapped := net.Pipe()
			defer serverConnWrapped.Close()
			defer clientConnWrapped.Close()
			session = newTestSession(serverConnWrapped, &mockExecutor{
				executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
					return nil, fmt.Errorf("apply input failed: Neo.ClientError.Transaction.ForbiddenDueToTransactionType: Writing to more than one database per transaction is not allowed")
				},
			})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Permissions: []string{"read", "write", "schema"}}

			done = make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("RETURN wrapped", nil, nil)) }()
			code, msg, err := AssertFailure(t, clientConnWrapped)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType", code)
			assert.Contains(t, msg, "Writing to more than one database per transaction is not allowed")

			serverConn2, clientConn2 := net.Pipe()
			defer serverConn2.Close()
			defer clientConn2.Close()
			session = newTestSession(serverConn2, &mockExecutor{
				executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
					return &QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}, nil
				},
			})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.authenticated = true
			session.inTransaction = true
			primeTestTransactionLifecycle(t, session)
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Permissions: []string{"read", "write", "schema"}}

			done = make(chan error, 1)
			go func() {
				done <- session.handleRun(buildRunMessageData("RETURN 1", map[string]any{"x": int64(1)}, map[string]any{"tx_timeout": int64(5)}))
			}()
			meta, err := AssertSuccess(t, clientConn2)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, []any{"value"}, meta["fields"])
			assert.Contains(t, meta, "qid")
			assert.Equal(t, int64(0), meta["t_first"])
		})

		t.Run("read permission secure default and db executor errors", func(t *testing.T) {
			session, serverConn, clientConn := makeSession(&mockExecutor{})
			defer serverConn.Close()
			defer clientConn.Close()
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "writer", Permissions: []string{"write"}}

			done := make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("RETURN 1", nil, nil)) }()
			code, msg, err := AssertFailure(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Security.Forbidden", code)
			assert.Contains(t, msg, "Read operations require read permission")

			serverConn2, clientConn2 := net.Pipe()
			defer serverConn2.Close()
			defer clientConn2.Close()
			session = newTestSession(serverConn2, &mockExecutor{})
			session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
			session.server.config.RequireAuth = true
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Permissions: []string{"read"}}

			done = make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("RETURN 1", nil, nil)) }()
			code, msg, err = AssertFailure(t, clientConn2)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Security.Forbidden", code)
			assert.Contains(t, msg, "Access to database 'nornic' is not allowed")

			serverConn3, clientConn3 := net.Pipe()
			defer serverConn3.Close()
			defer clientConn3.Close()
			session = newTestSession(serverConn3, &mockExecutor{})
			session.server = &Server{
				config:   DefaultConfig(),
				sessions: map[string]*Session{},
				dbManager: &overrideDBManager{
					getErr:    io.ErrClosedPipe,
					defaultDB: "graph",
					exists:    true,
				},
			}
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Permissions: []string{"read"}}

			done = make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData("RETURN 1", nil, nil)) }()
			code, _, err = AssertFailure(t, clientConn3)
			require.NoError(t, err)
			require.NoError(t, <-done)
			assert.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", code)
		})
	})
}

func TestBoltCoverage_SendFailureAndChunkErrors(t *testing.T) {
	conn := &errorConn{writeErr: io.ErrClosedPipe}
	session := newTestSession(conn, &mockExecutor{
		executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
			return nil, io.ErrClosedPipe
		},
	})

	require.Error(t, session.sendFailure("Neo.ClientError.General.UnknownError", "boom"))
	require.Error(t, session.sendChunk([]byte{0x01}))
}
