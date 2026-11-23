package cypher

// Author: pr1826-base3 test lane (Claude Opus 5.5), 23 Nov 2026.
// The deciding values of statement admission for SHOW USER[S] WITH AUTH in a
// fine-grained request: whether the statement is treated as a NornicDB
// extension (validated by the Nornic validator) or handed to the ANTLR grammar.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestPR1826B3ShowUsersWithAuthAdmission(t *testing.T) {
	ctx := auth.WithFineGrainedRequest(t.Context(), &auth.FineGrainedRequest{})
	for _, statement := range []string{
		"SHOW USERS WITH AUTH YIELD user, provider, auth WHERE user = 'analyst'",
		"SHOW USER WITH AUTH YIELD user, provider WHERE user = 'analyst_new'",
		"SHOW USERS YIELD user WHERE user = 'analyst'",
		"CREATE USER analyst SET AUTH 'oidc-work' { SET ID 'employee-12' }",
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "system"))
		err := exec.validateSyntax(ctx, statement, false, syntaxStatement)
		t.Logf("PR1826B3ADMIT parser=%s statement=%q isShowUsersWithAuth=%t isNornicExtensionStatement=%t validateSyntax=%v",
			config.GetParserType(), statement, isShowUsersWithAuth(ctx, statement), isNornicExtensionStatement(ctx, statement), err)
	}
}
