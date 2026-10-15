package fabric

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteEntityBindingPreservesProvenance(t *testing.T) {
	remote := storage.RemoteEntityValue{"elementId": "4:remote:n", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{"name": "Remote"}}
	ordinary := map[string]interface{}{"elementId": "forged", "labels": []interface{}{"Fake"}}
	bindings := bindingsFromParentAndRow(map[string]interface{}{"parent": remote}, []string{"node", "ordinary"}, []interface{}{remote, ordinary})
	require.IsType(t, storage.RemoteEntityValue{}, bindings["parent"])
	require.IsType(t, storage.RemoteEntityValue{}, bindings["node"])
	require.IsType(t, map[string]interface{}{}, bindings["ordinary"])
}
