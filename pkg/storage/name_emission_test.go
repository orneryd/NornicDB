package storage

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/stretchr/testify/require"
)

func TestNameEmissionRemoteParameterKeys(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			text, params := remoteHTTPRequest(t, "CREATE (n:L $props)", map[string]any{"props": map[string]any{name: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}})
			// The actual HTTP writer emits BOTH a literal map key and a
			// property lookup. Re-read those exact tokens without repairing them.
			for _, marker := range []string{"{", "$props."} {
				at := strings.Index(text, marker)
				require.GreaterOrEqual(t, at, 0, text)
				written := strings.TrimLeft(text[at+len(marker):], " ")
				token, ok := cyphertext.ScanIdentifier(written, 0)
				got, end := token.Value, token.End
				require.True(t, ok, text)
				require.Equal(t, name, got)
				require.Contains(t, ":)", written[end:end+1])
			}
			var sent map[string]map[string]any
			require.NoError(t, json.Unmarshal(params, &sent))
			require.Contains(t, sent["props"], name)
		})
	}
}
