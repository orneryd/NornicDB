package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/statementdiagtest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestStatementDiagnosticHTTPContract(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	baseline := map[string][]byte{}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		t.Run(parser, func(t *testing.T) {
			server, users := setupTestServerWithConfig(t, func(cfg *Config) { cfg.Localizer = manager })
			token := "Bearer " + getAuthToken(t, users, "admin")
			router := server.buildRouter()
			for _, locale := range []string{"en-US", "es-ES", "en-XA"} {
				for _, item := range statementdiagtest.TransportCases() {
					for _, query := range statementdiagtest.RoutedForms(item, "nornic") {
						t.Run(locale+"/"+item.Name+"/"+query, func(t *testing.T) {
							body, err := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": query}}})
							require.NoError(t, err)
							request := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", bytes.NewReader(body))
							request.Header.Set("Content-Type", "application/json")
							request.Header.Set("Authorization", token)
							request.Header.Set("Accept-Language", locale)
							response := httptest.NewRecorder()
							router.ServeHTTP(response, request)
							require.Equal(t, http.StatusOK, response.Code, response.Body.String())
							var decoded TransactionResponse
							require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded), response.Body.String())
							if item.ExpectedStatus != "" {
								if assert.Len(t, decoded.Errors, 1, response.Body.String()) {
									assert.Equal(t, item.ExpectedStatus, decoded.Errors[0].Code)
								}
							} else {
								assert.Empty(t, decoded.Errors, response.Body.String())
							}
							if item.Name == "grammar-token" {
								position := localization.CypherANTLRUnexpectedInput(1, strings.IndexByte(query, '@'))
								message := localization.CypherANTLRParseSyntaxError(position.Fallback, position)
								want, _, err := manager.Render(localization.WithPreferences(t.Context(), language.MustParse(locale)), localization.MessageRequest(message))
								require.NoError(t, err)
								if len(decoded.Errors) == 1 {
									assert.Equal(t, want, decoded.Errors[0].Message)
								}
							}
							// Compare decoded JSON contracts, excluding plans/statistics with
							// measured durations. Error encoding and locale are retained whole.
							payload, err := statementdiagtest.Encode(struct {
								Errors []QueryError
								Locale string
							}{decoded.Errors, response.Header().Get("Content-Language")})
							require.NoError(t, err)
							key := locale + "/" + query
							if prior := baseline[key]; prior != nil {
								require.JSONEq(t, string(prior), string(payload))
							} else {
								baseline[key] = payload
							}
						})
					}
				}
			}
		})
	}
}
