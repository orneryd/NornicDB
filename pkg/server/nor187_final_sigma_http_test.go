package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nor187HTTPQuery is the exact historical reproduction shape over the HTTP
// transaction API: the input is a parameter, the complete lowercase value and
// the final character are both returned. The recorded "FIXED" result had been
// truncated, so neither projection may be inferred from the other.
const nor187HTTPQuery = "RETURN toLower($s) AS lowered, right(toLower($s), 1) AS last"

type nor187HTTPRow struct {
	name  string
	input string
	want  string
}

// TestHTTPToLowerFinalSigmaBoundary reproduces NOR-187 through the in-process
// HTTP transaction route (server.buildRouter) on both parser paths, using the
// exact same parameter-carrying statement the historical evidence used.
// Expected values follow the pinned Neo4j/JDK profile; the exhaustive matrix
// lives with the casing owner (pkg/cypher/fn) and the public execute path
// (pkg/cypher). This subset keeps every discriminating class and control on
// the wire path. No external network, Docker, or SSH is used: httptest drives
// the real router in process.
func TestHTTPToLowerFinalSigmaBoundary(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })

	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			for _, row := range nor187HTTPRows() {
				t.Run(row.name, func(t *testing.T) {
					body := map[string]any{
						"statements": []any{map[string]any{
							"statement":  nor187HTTPQuery,
							"parameters": map[string]any{"s": row.input},
						}},
					}
					encoded, err := jsontext.Marshal(body)
					require.NoError(t, err)
					record := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", json.RawMessage(encoded), token)
					require.Equal(t, http.StatusOK, record.Code)
					var response TransactionResponse
					require.NoError(t, jsontext.Unmarshal(record.Body.Bytes(), &response))
					require.Empty(t, response.Errors)
					require.Len(t, response.Results, 1)
					require.Len(t, response.Results[0].Data, 1)
					values := response.Results[0].Data[0].Row
					require.Len(t, values, 2)
					lowered, ok := values[0].(string)
					require.True(t, ok, "lowered projection is a string")
					last, ok := values[1].(string)
					require.True(t, ok, "last projection is a string")

					assert.Equal(t, row.want, lowered,
						"complete HTTP toLower output: input %d bytes/%d runes, got %d bytes/%d runes",
						len(row.input), utf8.RuneCountInString(row.input), len(lowered), utf8.RuneCountInString(lowered))
					assert.Equal(t, nor187HTTPWantLastRune(row.want), last, "HTTP final rune")
					assert.Equal(t, utf8.RuneCountInString(row.want), utf8.RuneCountInString(lowered),
						"rune count: truncation is the historical misreading, not the contract")
				})
			}
		})
	}
}

// nor187HTTPWantLastRune is the expected right(value, 1) for a contract row.
func nor187HTTPWantLastRune(want string) string {
	if want == "" {
		return ""
	}
	last, _ := utf8.DecodeLastRuneInString(want)
	return string(last)
}

// nor187HTTPRows keeps the recorded boundary, its adjacent lengths, a
// combining-acute run across the 128-byte chunk, the forward-lookahead limit,
// U+0345 precedence, short cased/uncased controls, and empty/non-Greek
// preservation on the HTTP path.
func nor187HTTPRows() []nor187HTTPRow {
	const (
		sigma         = "\u03A3"
		finalSigma    = "\u03C2"
		medialSigma   = "\u03C3"
		acute         = "\u0301"
		ypogegrammeni = "\u0345"
	)
	reps := func(text string, count int) string { return strings.Repeat(text, count) }

	return []nor187HTTPRow{
		{name: "ascii126+Sigma", input: reps("a", 126) + sigma, want: reps("a", 126) + finalSigma},
		{name: "ascii127+Sigma_historical", input: reps("a", 127) + sigma, want: reps("a", 127) + finalSigma},
		{name: "ascii128+Sigma", input: reps("a", 128) + sigma, want: reps("a", 128) + finalSigma},
		{name: "ascii129+Sigma", input: reps("a", 129) + sigma, want: reps("a", 129) + finalSigma},
		{name: "A+acute127+Sigma", input: "A" + reps(acute, 127) + sigma, want: "a" + reps(acute, 127) + finalSigma},
		{name: "A+acute257+Sigma", input: "A" + reps(acute, 257) + sigma, want: "a" + reps(acute, 257) + finalSigma},

		{name: "A+Sigma+acute30+B", input: "A" + sigma + reps(acute, 30) + "B", want: "a" + medialSigma + reps(acute, 30) + "b"},
		{name: "A+Sigma+acute31+B", input: "A" + sigma + reps(acute, 31) + "B", want: "a" + medialSigma + reps(acute, 31) + "b"},
		{name: "A+Sigma+acute32+B", input: "A" + sigma + reps(acute, 32) + "B", want: "a" + medialSigma + reps(acute, 32) + "b"},

		{name: "U0345+Sigma", input: ypogegrammeni + sigma, want: ypogegrammeni + "\u03C3"},
		{name: "supplementary_letter_after_sigma", input: "AΣ\U00011F02B", want: "aς\U00011F02b"},
		{name: "supplementary_letter_before_sigma", input: "A\U00011F02Σ", want: "a\U00011F02σ"},
		{name: "supplementary_cased_at_start", input: "\U00010400Σ", want: "\U00010428ς"},
		{name: "supplementary_cased_after_done", input: "\uFFFF\U00010400Σ", want: "\uFFFF\U00010428ς"},
		{name: "supplementary_cased_after_unpaired_surrogate", input: "\xed\xa0\x80\U00010400Σ", want: "\xed\xa0\x80\U00010428σ"},
		{name: "java_dash_word", input: "AΣ-B", want: "aσ-b"},
		{name: "java_colon_boundary", input: "AΣ:B", want: "aς:b"},
		{name: "A+U0345+Sigma", input: "A" + ypogegrammeni + sigma, want: "a" + ypogegrammeni + finalSigma},

		{name: "a+Sigma", input: "a" + sigma, want: "a" + finalSigma},
		{name: "Sigma+a", input: sigma + "a", want: medialSigma + "a"},
		{name: "Sigma_alone", input: sigma, want: medialSigma},
		{name: "A+Sigma+space", input: "A" + sigma + " ", want: "a" + finalSigma + " "},

		{name: "empty", input: "", want: ""},
		{name: "german_sharp_s", input: "Stra\u00DFe", want: "stra\u00DFe"},
		{name: "dotted_capital_I", input: "\u0130", want: "i\u0307"},
		{name: "greek_caps", input: "\u0391\u0392\u0393\u0394", want: "\u03B1\u03B2\u03B3\u03B4"},
	}
}
