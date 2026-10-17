package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// internalFunctionCall is an internal function call a rewrite writes into a
// statement's text.
var internalFunctionCall = regexp.MustCompile(`__nornic_[a-z0-9_]+\(`)

// emittedInternalFunctions extracts, from this package's source, every
// internal __nornic_* function its rewrites emit: a call written in a string
// literal, or a name a rewrite builds the call from (a constant whose name
// ends in Function).
func emittedInternalFunctions(t *testing.T) []string {
	t.Helper()
	files := token.NewFileSet()
	packages, err := parser.ParseDir(files, ".", func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)
	found := map[string]bool{}
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.BasicLit:
					if node.Kind != token.STRING {
						return true
					}
					value, err := strconv.Unquote(node.Value)
					if err != nil {
						return true
					}
					for _, call := range internalFunctionCall.FindAllString(value, -1) {
						found[strings.TrimSuffix(call, "(")] = true
					}
				case *ast.ValueSpec:
					for i, name := range node.Names {
						if !strings.HasSuffix(name.Name, "Function") || i >= len(node.Values) {
							continue
						}
						literal, ok := node.Values[i].(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING {
							continue
						}
						if value, err := strconv.Unquote(literal.Value); err == nil && strings.HasPrefix(value, "__nornic_") {
							found[value] = true
						}
					}
				}
				return true
			})
		}
	}
	names := make([]string, 0, len(found))
	for name := range found {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Every internal function a rewrite emits has a privilege contract: the
// fail-closed classification must never see a statement NornicDB itself
// rewrote as calling an unclassified function. A new rewrite function fails
// here until it is classified at its owner (the catalog or the registry).
func TestEmittedInternalFunctionsAreClassified(t *testing.T) {
	names := emittedInternalFunctions(t)
	require.Subset(t, names, []string{"__nornic_path_selector", "__nornic_interpolate", "__nornic_haslabels"}, "the extractor finds the rewrites")
	for _, name := range names {
		class, found := functionPrivilegeClass(name)
		require.True(t, found, "%s is emitted by a rewrite but unclassified", name)
		require.True(t, class.Valid(), "%s: invalid privilege class %v", name, class)
	}
}
