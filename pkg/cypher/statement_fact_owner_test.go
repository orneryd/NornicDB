package cypher

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStatementFactsIgnoreCommentsAndSpacing pins each owner fact that compares
// against the text after a word.
func TestStatementFactsIgnoreCommentsAndSpacing(t *testing.T) {
	for statement, want := range map[string]statementCommand{
		"BEGIN":                         commandBegin,
		"BEGIN /* c */":                 commandBegin,
		"BEGIN TRANSACTION // c":        commandBegin,
		"COMMIT /* c */":                commandCommit,
		"ROLLBACK /* a */ /* b */\n":    commandRollback,
		"BEGIN /* c */ RETURN 1 COMMIT": commandBeginScript,
	} {
		require.Equal(t, want, classifyStatement(statement).command, statement)
	}
	for _, statement := range []string{
		"CREATE p = (n)",
		"CREATE /* a */ p /* b */ = (n)",
		"CREATE p // a\n= (n)",
		"CREATE /* a */ (n)",
	} {
		require.True(t, classifyStatement(statement).graphQuery, statement)
	}
}

// TestExecutionPrefixIsTheOwnersFact pins each consumer of the prefix to the
// owner's answer, whatever precedes the prefix.
func TestExecutionPrefixIsTheOwnersFact(t *testing.T) {
	ctx := context.Background()
	require.True(t, compileExecutionPolicy(ctx, "RETURN 1", 0).cacheable)
	for _, statement := range []string{
		"PROFILE RETURN 1",
		"profile RETURN 1",
		"/* c */ PROFILE RETURN 1",
		"CYPHER 25 PROFILE RETURN 1",
		"CYPHER 5 /* c */ PROFILE RETURN 1",
	} {
		require.Equal(t, prefixProfile, classifyStatement(statement).prefixMode, statement)
		require.False(t, compileExecutionPolicy(ctx, statement, 0).cacheable, statement)
		info := analyzeQuery(statement)
		require.True(t, info.HasProfile, statement)
		require.False(t, info.HasExplain, statement)
	}
	for _, statement := range []string{"EXPLAIN RETURN 1", "/* c */ EXPLAIN RETURN 1", "CYPHER 5 EXPLAIN RETURN 1"} {
		require.Equal(t, prefixExplain, classifyStatement(statement).prefixMode, statement)
		require.True(t, analyzeQuery(statement).HasExplain, statement)
		require.NoError(t, authorizeReadOnlyConstituent(ctx, statement, false), statement)
	}
}

// TestPermissionEntitlementsAccumulate pins that requirements are the union of
// what the statement needs: an administrative head does not hide a write in
// its continuation, and an administration CREATE keeps its own mapping.
func TestPermissionEntitlementsAccumulate(t *testing.T) {
	ctx := context.Background()
	allowExceptWrite := func(permission string) bool { return permission != "write" }
	noBoost := func(string) bool { return false }
	for _, test := range []struct {
		statement                  string
		admin, write, changesState bool
	}{
		{"CYPHER 25 SHOW FUNCTIONS YIELD name WITH name CREATE (:ReviewWrite) RETURN name", false, true, true},
		{"CYPHER 25 SHOW TRANSACTIONS YIELD transactionId WITH transactionId CREATE (:ReviewWrite) RETURN transactionId", true, true, true},
		{"CYPHER 25 SHOW TRANSACTIONS /* c */ YIELD transactionId WITH transactionId SET transactionId.x = 1 RETURN transactionId", true, true, true},
		{"SHOW TRANSACTIONS", true, false, false},
		{"SHOW TRANSACTIONS YIELD transactionId RETURN transactionId", true, false, false},
		{"CREATE DATABASE reviewdb", true, false, true},
		{"CREATE USER reviewuser SET PASSWORD 'review-secret'", false, true, true},
		{"CREATE COMPOSITE DATABASE reviewdb", true, false, true},
		{"CREATE ALIAS reviewalias FOR DATABASE nornic", true, false, true},
		{"DROP DATABASE reviewdb", true, false, true},
		{"CREATE (:ReviewWrite)", false, true, true},
	} {
		requirements := QueryPermissionRequirements(ctx, test.statement, AcceptedSource)
		require.Equal(t, test.admin, requirements.Admin, test.statement)
		require.Equal(t, test.write, requirements.Write, test.statement)
		require.Equal(t, test.changesState, requirements.ChangesState, test.statement)
		if test.write {
			require.Equal(t, "write", MissingPermission(requirements, allowExceptWrite, noBoost), test.statement)
		} else {
			require.Empty(t, MissingPermission(requirements, allowExceptWrite, noBoost), test.statement)
		}
	}
	// EXPLAIN runs nothing, and needs the WRITE privilege of what it plans.
	explained := QueryPermissionRequirements(ctx, "EXPLAIN CYPHER 25 SHOW TRANSACTIONS YIELD transactionId WITH transactionId CREATE (:ReviewWrite) RETURN transactionId", AcceptedSource)
	require.True(t, explained.Write)
	require.False(t, explained.ChangesState)
}

// TestExecutionPrefixHasOneReader keeps EXPLAIN and PROFILE recognition in the
// statement text owner: a consumer reads statementClass.prefixMode. The command
// table and the knowledge-policy files read PROFILE as a word of DECAY PROFILE.
func TestExecutionPrefixHasOneReader(t *testing.T) {
	files := token.NewFileSet()
	packages, err := parser.ParseDir(files, ".", func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go") &&
			info.Name() != "statement_text_admission.go" &&
			info.Name() != "statement_command.go" &&
			!strings.HasPrefix(info.Name(), "knowledgepolicy_")
	}, 0)
	require.NoError(t, err)
	require.NotEmpty(t, packages)
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				for _, argument := range call.Args {
					if literal, ok := argument.(*ast.BasicLit); ok && (literal.Value == `"EXPLAIN"` || literal.Value == `"PROFILE"`) {
						t.Errorf("%s reads an execution prefix outside its owner", files.Position(literal.Pos()))
					}
				}
				return true
			})
		}
	}
}
