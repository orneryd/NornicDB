package cypher

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func commentedFunctionScanInput(identifiers int, line bool) string {
	var query strings.Builder
	if line {
		query.WriteString("// don't scan this prefix again\nRETURN ")
	} else {
		query.WriteString("/* don't scan this prefix again */ RETURN ")
	}
	for i := 0; i < identifiers; i++ {
		query.WriteString("identifier")
		query.WriteString(strconv.Itoa(i))
		query.WriteString(" + ")
	}
	query.WriteString("coalesce(null,1)")
	return query.String()
}

// The forward owner retains its previous live position. Forbid the exact
// growing-prefix dependency that made commented name scans quadratic, rather
// than making a correctness suite depend on a noisy wall-clock ratio.
func TestFunctionSyntaxLinearScanGuard(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "function_syntax.go", nil, 0)
	require.NoError(t, err)
	forward := map[string]bool{"readFunctionSyntaxFacts": true, "readFunctionIdentity": true, "dottedNameParts": true, "callableIdentifierToken": true, "nextCall": true}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !forward[function.Name.Name] {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if name, ok := node.(*ast.Ident); ok {
				require.NotContains(t, []string{"queryGapStart", "functionNameStartsAt", "colonIsMapKeySeparator"}, name.Name, function.Name.Name)
			}
			return true
		})
	}
}

func TestFunctionSyntaxCommentedLongNameScan(t *testing.T) {
	for _, identifiers := range []int{2000, 4000} {
		for _, line := range []bool{false, true} {
			query := commentedFunctionScanInput(identifiers, line)
			require.Greater(t, len(query), 16384)
			var names []string
			require.NoError(t, forEachFunctionCall(query, func(name, _ string) error { names = append(names, name); return nil }))
			require.Equal(t, []string{"coalesce"}, names)
		}
	}
}

// Paired 2k/4k measurements establish scan scaling beyond the lexical cache's
// retention bound. This uses the same reader API on the prior and fixed trees.
func BenchmarkFunctionSyntaxCommentedNameScan(b *testing.B) {
	for _, identifiers := range []int{2000, 4000} {
		for _, line := range []bool{false, true} {
			query := commentedFunctionScanInput(identifiers, line)
			b.Run(fmt.Sprintf("identifiers=%d/line=%t", identifiers, line), func(b *testing.B) {
				count := 0
				if err := forEachFunctionCall(query, func(_, _ string) error { count++; return nil }); err != nil || count != 1 {
					b.Fatalf("calls=%d err=%v", count, err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := forEachFunctionCall(query, func(_, _ string) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
