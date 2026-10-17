package cypher

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOwnUserInformationNeedsNoGraphRead(t *testing.T) {
	ctx := WithPermissionChecker(context.Background(), func(string) bool { return false })
	for _, query := range []string{
		"SHOW CURRENT USER YIELD collectionLimited RETURN collectionLimited",
		"SHOW USER PRIVILEGES",
		"/* self */ show user privileges yield action return action",
	} {
		require.Equal(t, PermissionRequirements{}, QueryPermissionRequirements(query), query)
		require.NoError(t, AuthorizeQuery(ctx, query), query)
	}
	require.Error(t, AuthorizeQuery(ctx, "SHOW USER other PRIVILEGES"), "other-user inspection still needs its authorization")
}

func TestEveryAdministrationCommandHasExpectedPrivilegeClass(t *testing.T) {
	assert.Len(t, administrationCommandClasses, len(expectedCommandClasses))
	for shape, expected := range expectedCommandClasses {
		t.Run(shape, func(t *testing.T) {
			for _, query := range []string{shape, "/* request */ " + strings.ToLower(strings.ReplaceAll(shape, " ", "\t"))} {
				class, found := administrationCommandClass(query)
				require.True(t, found, query)
				require.Equal(t, expected, class, query)
				want := requirementsForClass(expected)
				if want.Schema {
					// A schema refusal names Neo4j's schema action.
					want.SchemaAction = schemaCommandAction(query)
				}
				require.Equal(t, want, QueryPermissionRequirements(query), query)
			}
		})
	}
}

func TestAdministrationDispatchShapesHavePrivilegeFixtures(t *testing.T) {
	for _, filename := range []string{"executor_query_routing.go", "executor_show.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 3 {
				return true
			}
			function, ok := call.Fun.(*ast.Ident)
			if !ok || function.Name != "startsWithKeywords" {
				return true
			}
			first, firstOK := call.Args[1].(*ast.BasicLit)
			rest, restOK := call.Args[2].(*ast.BasicLit)
			if !firstOK || !restOK {
				return true
			}
			verb, err := strconv.Unquote(first.Value)
			if err != nil {
				return true
			}
			object, err := strconv.Unquote(rest.Value)
			if err != nil {
				return true
			}
			switch verb {
			case "SHOW", "CREATE", "DROP", "ALTER", "TERMINATE":
				if verb == "CREATE" && object == "OR" {
					return true
				} // CREATE OR REPLACE DATABASE is the complete fixture.
				shape := verb + " " + object
				_, found := expectedCommandClasses[shape]
				assert.True(t, found, "%s dispatches %q without a reviewed contract", filename, shape)
			}
			return true
		})
	}
}
