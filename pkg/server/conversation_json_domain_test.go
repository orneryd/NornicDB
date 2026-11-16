package server

import (
	"encoding/json"
	"strings"
	"testing"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/require"
)

// One matrix compares the canonical input owner with the previous decoder for
// every workspace/config request shape, including malformed and mistyped input.
func TestAssistantRequestBodiesKeepDecodeContract(t *testing.T) {
	type workspaceRequest struct {
		Database string `json:"database"`
	}
	type configRequest struct {
		Overrides map[string]string `json:"overrides"`
	}
	for _, tc := range []struct {
		name           string
		target         func() any
		valid, invalid []string
	}{
		{"workspace", func() any { return &workspaceRequest{} }, []string{`{"database":"reader-chats"}`, `{}`, `{"database":""}`}, []string{`{"database":`, `{"database":5}`, ``}},
		{"conversation", func() any { return &heimdall.ConversationRequest{} }, []string{`{"operation":"create","database":"reader-chats","title":"File review"}`, `{"id":"thread-1","archived":true}`, `{"operation":"search","query":"needle"}`}, []string{`{"archived":`, `{"archived":"yes"}`, ``}},
		{"database config", func() any { return &configRequest{} }, []string{`{"overrides":{"db.nornic.search.bm25.enabled":"true"}}`, `{"overrides":{}}`, `{"overrides":null}`, `{}`}, []string{`{"overrides":`, `{"overrides":{"key":5}}`, ``}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, body := range tc.valid {
				previous, current := tc.target(), tc.target()
				require.NoError(t, json.NewDecoder(strings.NewReader(body)).Decode(previous))
				require.NoError(t, valueconvert.CypherInputJSON.NewDecoder(strings.NewReader(body)).Decode(current))
				require.Equal(t, previous, current)
			}
			for _, body := range tc.invalid {
				previous, current := tc.target(), tc.target()
				oldErr := json.NewDecoder(strings.NewReader(body)).Decode(previous)
				newErr := valueconvert.CypherInputJSON.NewDecoder(strings.NewReader(body)).Decode(current)
				require.Error(t, oldErr)
				require.Error(t, newErr)
				require.Equal(t, oldErr.Error(), newErr.Error())
			}
		})
	}
}
