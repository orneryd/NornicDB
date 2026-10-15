package models

import (
	"bytes"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteEntityGraphQLJSONShape(t *testing.T) {
	remote := storage.RemoteEntityValue{"elementId": "4:remote:n", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{"name": "Remote"}}
	var wire bytes.Buffer
	JSON{"row": []interface{}{remote}}.MarshalGQL(&wire)
	require.JSONEq(t, `{"row":[{"elementId":"4:remote:n","labels":["Doc"],"properties":{"name":"Remote"}}]}`, wire.String())
}
