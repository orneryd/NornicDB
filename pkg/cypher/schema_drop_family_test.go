package cypher_test

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every DROP INDEX / DROP CONSTRAINT form the default parser has accepted keeps
// its result on either parser: IF EXISTS after or before a plain or backtick
// name, on a present or missing object. The public API alone is used, so the
// same file runs against a release tree for comparison.
func TestSchemaDropFamilyKeepsDefaultParserResults(t *testing.T) {
	// The planted pre-name form is outside the shared grammar alone.
	const planted = "DROP CONSTRAINT IF EXISTS `uq order id`"
	_, grammarErr := cypherantlr.ParseSyntax(t.Context(), planted, cypherantlr.StatementEntry, false, cyphertext.RawSource)
	require.Error(t, grammarErr, "the planted control must be rejected by the grammar alone")

	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	kinds := []struct{ keyword, create string }{
		{"INDEX", "CREATE INDEX %s IF NOT EXISTS FOR (o:%s) ON (o.createdAt)"},
		{"CONSTRAINT", "CREATE CONSTRAINT %s IF NOT EXISTS FOR (o:%s) REQUIRE o.id IS UNIQUE"},
	}
	names := []string{"drop_family_plain", "`drop family quoted`"}
	forms := []string{"DROP %s %s", "DROP %s %s IF EXISTS", "DROP %s IF EXISTS %s"}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		for _, kind := range kinds {
			for n, name := range names {
				for f, form := range forms {
					t.Run(parser+"/"+kind.keyword+"/"+name+"/"+strings.ReplaceAll(form, "%s", "_"), func(t *testing.T) {
						exec := newDropFamilyExecutor(t)
						label := "DropFamily" + string(rune('A'+n)) + string(rune('A'+f))
						drop := sprintf(form, kind.keyword, name)
						_, err := exec.Execute(t.Context(), sprintf(kind.create, name, label), nil)
						require.NoError(t, err)
						_, err = exec.Execute(t.Context(), drop, nil)
						require.NoError(t, err, drop)
						// The object is gone: a plain drop now fails and IF EXISTS
						// forms on the missing object still succeed.
						_, err = exec.Execute(t.Context(), sprintf("DROP %s %s", kind.keyword, name), nil)
						require.Error(t, err, "dropped object remained")
						for _, missing := range forms[1:] {
							_, err = exec.Execute(t.Context(), sprintf(missing, kind.keyword, name), nil)
							require.NoError(t, err, missing)
						}
					})
				}
			}
		}
		_, err := newDropFamilyExecutor(t).Execute(t.Context(), planted, nil)
		require.NoError(t, err, parser+": "+planted)
	}
}

// newDropFamilyExecutor owns a fresh store and closes it with the test.
func newDropFamilyExecutor(t *testing.T) *cypher.StorageExecutor {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	return cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
}

// sprintf fills %s verbs in order; the forms carry no other verbs.
func sprintf(format string, values ...string) string {
	for _, value := range values {
		format = strings.Replace(format, "%s", value, 1)
	}
	return format
}
