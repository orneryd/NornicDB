package antlr

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/contextslot"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestANTLRCredentialRoleProduct(t *testing.T) {
	for _, raw := range []string{"'probe741928'", `"probe741928"`, "`probe741928`", "probe741928", `'probe74\u0031928'`} {
		query := "CREATE COMPOSITE DATABASE grammar_product ALIAS r FOR DATABASE nornic AT 'http://fixture:probe741928@example.invalid' USER u PASSWORD " + raw
		_, err := ParseSyntax(context.Background(), query, StatementEntry, false, RawSource)
		require.NoError(t, err)
		// Replay the retained product into this statement's set on a cache hit too.
		ctx := contextslot.Ensure[cyphertext.CredentialSet](t.Context())
		_, err = ParseSyntax(ctx, query, StatementEntry, false, RawSource)
		require.NoError(t, err)
		foundPassword, foundURL := false, false
		for _, span := range contextslot.Get[cyphertext.CredentialSet](ctx).Spans {
			require.Equal(t, span.Raw, query[span.Start:span.End])
			if span.Password && span.Raw == raw && span.Value == "probe741928" {
				foundPassword = true
			}
			if !span.Password && span.Value == "http://fixture:probe741928@example.invalid" {
				foundURL = true
			}
		}
		require.True(t, foundPassword && foundURL, "grammar credential roles must reach the shared set; details withheld")
	}
}

func TestANTLRCredentialSourceMapReplayPopulation(t *testing.T) {
	data, err := os.ReadFile("../../../internal/cyphertext/testdata/normalization.json")
	require.NoError(t, err)
	var classes []struct{ Character, Kind string }
	require.NoError(t, json.Unmarshal(data, &classes))
	observations := 0
	for _, raw := range []string{"'probe741928'", `"probe741928"`, "`probe741928`", "probe741928", `'probe74\u0031928'`} {
		base := "CREATE COMPOSITE DATABASE grammar_source_map ALIAS r FOR DATABASE nornic AT 'http://fixture:probe741928@example.invalid' USER u PASSWORD " + raw
		observe := func(original string) {
			ctx, source := cyphertext.WithSource(t.Context(), original)
			// Warm the exact accepted product without this request's original
			// association. Replay must use the new request's source map.
			warm, err := ParseSyntax(t.Context(), source.Text, StatementEntry, false, AcceptedSource)
			require.NoError(t, err)
			ctx = contextslot.Ensure[cyphertext.CredentialSet](ctx)
			replay, err := ParseSyntax(ctx, source.Text, StatementEntry, false, AcceptedSource)
			require.NoError(t, err)
			require.Same(t, warm.data, replay.data)
			foundPassword, foundURL := false, false
			for _, span := range contextslot.Get[cyphertext.CredentialSet](ctx).Spans {
				require.Equal(t, span.Raw, original[span.Start:span.End])
				if span.Password && span.Raw == raw && span.Value == "probe741928" {
					foundPassword = true
				}
				if !span.Password && span.Value == "http://fixture:probe741928@example.invalid" {
					foundURL = true
				}
			}
			require.True(t, foundPassword && foundURL, "replayed grammar roles lost original coordinates; details withheld")
			observations++
		}
		for _, class := range classes {
			if class.Kind == "whitespace" {
				observe(strings.ReplaceAll(base, " ", class.Character))
			} else if class.Kind == "deletion" {
				// Every boundary and interior in every administration keyword,
				// not only PASSWORD or the positions in the original finding.
				cursor := 0
				for _, word := range []string{"CREATE", "COMPOSITE", "DATABASE", "ALIAS", "FOR", "DATABASE", "AT", "USER", "PASSWORD"} {
					start := cursor + strings.Index(base[cursor:], word)
					for position := 0; position <= len(word); position++ {
						observe("\ufeff" + base[:start+position] + class.Character + base[start+position:])
					}
					cursor = start + len(word)
				}
			}
		}
	}
	require.Greater(t, observations, 1000)
	t.Logf("checked %d successful original-source role replays", observations)
}
