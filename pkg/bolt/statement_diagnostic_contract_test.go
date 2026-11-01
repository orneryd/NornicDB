package bolt

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/statementdiagtest"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestStatementDiagnosticBoltContract(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	baseline := map[string][]byte{}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		t.Run(parser, func(t *testing.T) {
			store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			// Configure localization before accepting a connection, as in production.
			server := New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192, Localizer: manager}, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic"))})
			port := serveBoltIntegrationServer(t, server)
			conn := openBoltTestConn(t, port)
			for _, locale := range []string{"en-US", "es-ES", "en-XA"} {
				for _, item := range statementdiagtest.TransportCases() {
					for _, query := range statementdiagtest.RoutedForms(item, "nornic") {
						t.Run(locale+"/"+item.Name+"/"+query, func(t *testing.T) {
							failure := diagnosticBoltResponse(t, conn, query, locale)
							if item.ExpectedStatus != "" {
								assert.NotNil(t, failure)
								assert.Equal(t, item.ExpectedStatus, failure["code"])
							} else {
								assert.Nil(t, failure)
							}
							if item.Name == "grammar-token" {
								position := localization.CypherANTLRUnexpectedInput(1, strings.IndexByte(query, '@'))
								message := localization.CypherANTLRParseSyntaxError(position.Fallback, position)
								want, _, err := manager.Render(localization.WithPreferences(t.Context(), language.MustParse(locale)), localization.MessageRequest(message))
								require.NoError(t, err)
								assert.Equal(t, want, failure["message"])
							}
							// This is the complete decoded FAILURE map from real PackStream,
							// including any protocol diagnostic metadata.
							payload, err := statementdiagtest.Encode(failure)
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

func diagnosticBoltResponse(t *testing.T, conn net.Conn, query, locale string) map[string]any {
	t.Helper()
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
	require.NoError(t, SendRun(t, conn, query, nil, map[string]any{"locale": locale}))
	kind, data, err := ReadMessage(conn)
	require.NoError(t, err)
	if kind == MsgSuccess {
		require.NoError(t, SendPull(t, conn, nil))
		for count := 0; count < 100; count++ {
			kind, data, err = ReadMessage(conn)
			require.NoError(t, err)
			if kind != MsgRecord {
				break
			}
		}
	}
	if kind == MsgSuccess {
		return nil
	}
	require.Equal(t, byte(MsgFailure), kind)
	decoded, end, err := decodePackStreamMap(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), end, "FAILURE payload must contain exactly one map")
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
	return decoded
}
