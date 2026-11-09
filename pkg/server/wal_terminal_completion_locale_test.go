// Author: Codex, NOR-180 localized public HTTP completion boundary, 2026-11-01.
package server

import (
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"net/http"
	"strings"
	"testing"
)

func TestHTTPWALTerminalCompletionLocales(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, tag := range manager.SupportedTags() {
		for _, operation := range []string{"commit", "rollback", "malformed"} {
			t.Run(tag.String()+"/"+operation, func(t *testing.T) {
				_, engine, request := walHTTPServer(t, tag)
				_, opened := request(t, http.MethodPost, "/db/nornic/tx", walHTTPStatements("CREATE (:WalWire)"))
				require.Empty(t, opened.Errors)
				require.NotEmpty(t, opened.Commit)
				require.NoError(t, engine.WAL.Close())
				method, path, body := http.MethodPost, opened.Commit, `{"statements":[]}`
				if operation == "rollback" {
					method, path, body = http.MethodDelete, opened.Commit[:len(opened.Commit)-len("/commit")], ""
				}
				if operation == "malformed" {
					body = "{"
				}
				_, response := request(t, method, path, body)
				require.NotEmpty(t, response.Errors)
				descriptor := localization.CypherTransactionsWALAbortFailed("TRANSACTION_ID", storage.ErrWALClosed)
				if operation == "commit" {
					descriptor = localization.CypherTransactionsWALCommitFailed("TRANSACTION_ID", storage.ErrWALClosed)
				}
				// Transaction identity is dynamic; the catalog's surrounding text is stable.
				expected, _, renderErr := manager.Render(localization.WithPreferences(t.Context(), tag), localization.MessageRequest(descriptor))
				require.NoError(t, renderErr)
				require.NotEmpty(t, expected)
				for _, part := range strings.Split(expected, "TRANSACTION_ID") {
					require.Contains(t, walHTTPMessages(response), part)
				}
				if operation == "commit" {
					require.Equal(t, int64(1), walterminal.Count(t, engine))
				}
			})
		}
	}
}
