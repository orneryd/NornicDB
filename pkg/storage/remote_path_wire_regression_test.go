package storage

import (
	"encoding/json"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRemoteBoltPathNormalizesToEntitySequence(t *testing.T) {
	a := dbtype.Node{Id: 11, ElementId: "4:source:a", Labels: []string{"Doc"}, Props: map[string]interface{}{}}
	b := dbtype.Node{Id: 22, ElementId: "4:source:b", Labels: []string{"Doc"}, Props: map[string]interface{}{}}
	r := dbtype.Relationship{Id: 33, ElementId: "5:source:r", StartId: 11, EndId: 22, StartElementId: a.ElementId, EndElementId: b.ElementId, Type: "LINKS", Props: map[string]interface{}{}}
	value := normalizeBoltValue(dbtype.Path{Nodes: []dbtype.Node{a, b}, Relationships: []dbtype.Relationship{r}})
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `[{"elementId":"4:source:a","labels":["Doc"],"properties":{}},{"elementId":"5:source:r","type":"LINKS","startNodeElementId":"4:source:a","endNodeElementId":"4:source:b","properties":{}},{"elementId":"4:source:b","labels":["Doc"],"properties":{}}]`, string(data))
}

func TestRemoteNumericIdentityStaysPrivate(t *testing.T) {
	node := normalizeBoltValue(dbtype.Node{Id: 71, ElementId: "4:source:n", Labels: []string{}, Props: map[string]interface{}{"id": "domain", "\x00remoteIdentity": "ordinary"}}).(RemoteEntityValue)
	id, start, end := node.LegacyIDs()
	require.Equal(t, int64(71), id)
	require.Equal(t, int64(-1), start)
	require.Equal(t, int64(-1), end)
	wire, err := json.Marshal(node)
	require.NoError(t, err)
	require.JSONEq(t, `{"elementId":"4:source:n","id":"domain","labels":[],"properties":{"id":"domain","\u0000remoteIdentity":"ordinary"}}`, string(wire))
	edge := normalizeBoltValue(dbtype.Relationship{Id: 81, StartId: 71, EndId: 72, ElementId: "5:source:r", StartElementId: "4:source:n", EndElementId: "4:source:m", Type: "LINKS", Props: map[string]interface{}{}}).(RemoteEntityValue)
	id, start, end = edge.LegacyIDs()
	require.Equal(t, int64(81), id)
	require.Equal(t, int64(71), start)
	require.Equal(t, int64(72), end)
	var empty RemoteEntityValue
	wire, err = json.Marshal(empty)
	require.NoError(t, err)
	require.Equal(t, "null", string(wire))
	_, err = json.Marshal(RemoteEntityValue{"properties": make(chan int)})
	require.Error(t, err)
}

func TestRemotePathPartsValidateIdentityAndShape(t *testing.T) {
	a := RemoteEntityValue{"elementId": "a", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	b := RemoteEntityValue{"elementId": "b", "labels": []interface{}{}, "properties": map[string]interface{}{}}
	r := RemoteEntityValue{"elementId": "r", "type": "R", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{}}
	for _, p := range []RemotePathValue{nil, {}, {a, r}, {RemoteEntityValue{}}, {r}, {a, a, b}, {a, r, a}} {
		nodes, rels, ok := p.Parts()
		require.False(t, ok)
		require.Nil(t, nodes)
		require.Nil(t, rels)
	}
	for _, p := range []RemotePathValue{{a}, {a, r, b}, {b, r, a}, {a, r, b, r, a}} {
		nodes, rels, ok := p.Parts()
		require.True(t, ok)
		require.Equal(t, (len(p)+1)/2, len(nodes))
		require.Equal(t, len(p)/2, len(rels))
	}
}

func TestRemoteHTTPRESTPathRejectsInconsistentCounts(t *testing.T) {
	for _, raw := range []string{`{"nodes":["a","b"],"relationships":["r"],"length":2}`, `{"nodes":["a"],"relationships":[],"length":-1}`, `{"nodes":["a"],"relationships":[],"length":0}`} {
		require.False(t, remoteHTTPRESTPath(json.RawMessage(raw), []interface{}{nil, nil, nil}), raw)
	}
	require.True(t, remoteHTTPRESTPath(json.RawMessage(`{"nodes":["a","b"],"relationships":["r"],"length":1}`), []interface{}{nil, nil, nil}))
}

func TestRemoteHTTPMalformedPathEntitiesAreRejected(t *testing.T) {
	node := map[string]interface{}{"elementId": "n", "labels": []interface{}{}, "properties": map[string]interface{}{"a": int64(1)}}
	meta := map[string]interface{}{"elementId": "n", "type": "node"}
	for _, tc := range []struct {
		raw      string
		metadata []interface{}
	}{{`1`, []interface{}{}}, {`[1]`, []interface{}{nil}}, {`[]`, []interface{}{}}, {`[{"a":1},{"a":1},{"a":1}]`, []interface{}{meta, meta, meta}}} {
		index := 0
		_, err := decodeRemoteHTTPValue(json.RawMessage(tc.raw), []interface{}{tc.metadata}, &index, map[string]interface{}{"n": node})
		require.Error(t, err, tc.raw)
	}
	require.Equal(t, int64(-1), remoteHTTPNumericID("not-numeric"))
	require.Equal(t, int64(4), remoteHTTPEndpointID(map[string]interface{}{"startNodeElementId": "n"}, "startNode", map[string]interface{}{"n": map[string]interface{}{remoteIdentityKey: remoteNumericIdentity{id: 4}}}))
	require.Equal(t, int64(-1), remoteHTTPEndpointID(map[string]interface{}{}, "startNode", nil))
}

func TestPublicRemoteMapRemovesOnlySealedMetadata(t *testing.T) {
	ordinary := map[string]interface{}{remoteIdentityKey: "user", "value": int64(1)}
	public := PublicRemoteMap(ordinary)
	public["value"] = int64(2)
	require.Equal(t, int64(2), ordinary["value"])
	require.Equal(t, "user", public[remoteIdentityKey])
	marker := normalizeBoltValue(dbtype.Node{Id: 7, ElementId: "n", Labels: []string{}, Props: map[string]interface{}{}}).(RemoteEntityValue)
	converted := map[string]interface{}(marker)
	public = PublicRemoteMap(converted)
	require.Len(t, public, 3)
	require.Len(t, converted, 4)
	require.NotContains(t, public, remoteIdentityKey)
	data, err := json.Marshal(public)
	require.NoError(t, err)
	require.NotContains(t, string(data), "remoteIdentity")
	require.Nil(t, PublicRemoteMap(nil))
}
