package storage

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeRemoteDatabaseNamesRetainPhysicalSource(t *testing.T) {
	sources := map[string]Engine{}
	for _, alias := range []string{"left", "right"} {
		sources[alias] = makeEngineWithTransport(t, "http://"+alias+".example", func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{map[string]interface{}{
				"id": "shared", "labels": []interface{}{"N"}, "properties": map[string]interface{}{"id": "shared", "source": alias},
			}}}, nil)}, nil
		})
	}
	composite := NewCompositeEngine(sources, map[string]string{"left": "remote_db", "right": "remote_db"}, map[string]string{"left": "read_write", "right": "read_write"})
	composite.SetCompositeName("comp")
	t.Cleanup(func() { require.NoError(t, composite.Close()) })
	nodes := composite.GetAllNodes()
	require.Len(t, nodes, 2)
	identities := map[string]bool{}
	for _, node := range nodes {
		identities[node.IdentityKey()] = true
	}
	require.Len(t, identities, 2, "independent remotes may have the same database name and stored ID")
	for _, node := range nodes {
		reloaded, err := composite.GetNode(NodeID(node.ElementID("")))
		require.NoError(t, err)
		require.Equal(t, node.Properties["source"], reloaded.Properties["source"])
		lookup, ok := EntityIdentityFromKey(node.IdentityKey()).NodeLookupID(composite)
		require.True(t, ok)
		require.Equal(t, NodeID(node.ElementID("")), lookup)
		require.Equal(t, node.IdentityKey(), CopyNode(node).IdentityKey())
		wire, err := json.Marshal(node)
		require.NoError(t, err)
		require.NotContains(t, string(wire), ".example")
		require.NotContains(t, fmt.Sprint(node.Identity()), ".example")
		encoded, err := encodeValue(node)
		require.NoError(t, err)
		var stored Node
		require.NoError(t, decodeValue(encoded, &stored))
		require.Empty(t, stored.Identity().Source)
		require.Empty(t, stored.Database)
	}
	_, err := composite.GetNode("4:remote_db:shared")
	require.ErrorIs(t, err, ErrNotFound, "an ambiguous old qualifier must not choose a random source")
	require.Error(t, BindRelationshipEndpoints(&Edge{ID: "cross", Type: "R"}, nodes[0], nodes[1]))
}

func TestCompositeRemoteAliasesSharePhysicalIdentity(t *testing.T) {
	sources := map[string]Engine{}
	requests := 0
	for _, alias := range []string{"left", "mirror"} {
		sources[alias] = makeEngineWithTransport(t, "http://same.example", func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{map[string]interface{}{"id": "shared", "labels": []interface{}{"N"}, "properties": map[string]interface{}{"source": "same"}}}}, nil)}, nil
		})
	}
	composite := NewCompositeEngine(sources, map[string]string{"left": "remote_db", "mirror": "remote_db"}, map[string]string{"left": "read_write", "mirror": "read_write"})
	composite.SetCompositeName("comp")
	t.Cleanup(func() { require.NoError(t, composite.Close()) })
	nodes, err := composite.AllNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 1, "separate transport instances for one configured location are one source")
	require.Equal(t, 1, requests, "alias dedup occurs before scanning, without retaining graph-sized state")
	require.Equal(t, "4:remote_db:shared", nodes[0].ElementID(""), "preserve the unambiguous shipped qualifier")
	for _, qualifier := range []string{"remote_db", "comp.left", "comp.mirror"} {
		node, err := composite.GetNode(NodeID("4:" + qualifier + ":shared"))
		require.NoError(t, err)
		require.Equal(t, nodes[0].IdentityKey(), node.IdentityKey())
	}
}
