package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// The manager reflects only a value, without its URL/key. Execute must retain
// the original command's URL credential context across both parser routes.
func TestCredentialURLAdminErrorGuard(t *testing.T) {
	previous := observability.DefaultRedactKeys
	observability.DefaultRedactKeys = append(append([]string(nil), previous...), "url_query_fixture_seal")
	t.Cleanup(func() { observability.DefaultRedactKeys = previous })
	const secret = "synthetic-admin-url-credential-738129"
	for _, endpoint := range []string{
		"http://fixture:" + secret + "@example.invalid/",
		"http://example.invalid/?api_key=" + secret,
		"http://example.invalid/?fixture_seal=" + secret,
		"http://example.invalid/#fixture_seal=" + secret,
	} {
		cause := errors.New("manager rejected " + secret)
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		executor.SetDatabaseManager(newCypherAdminErrorManager(t, testCatalogCreateComposite, cause))
		_, err := executor.Execute(context.Background(), "CREATE COMPOSITE DATABASE rejected ALIAS r FOR DATABASE nornic AT '"+endpoint+"'", nil)
		if !errors.Is(err, cause) {
			t.Fatal("public alias error lost the manager cause; capture withheld")
		}
		outputs := []string{err.Error()}
		var localized *localization.LocalizedError
		if errors.As(err, &localized) {
			outputs = append(outputs, localized.Message.Fallback, fmt.Sprint(localized.Message.Data))
		}
		for _, output := range outputs {
			if strings.Contains(output, secret) {
				t.Error("DDL error bypassed canonical URL credential context; capture withheld")
			}
		}
	}
}
