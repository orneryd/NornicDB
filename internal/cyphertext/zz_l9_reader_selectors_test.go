package cyphertext

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestL9LexicalClientUsesReaderSelectors(t *testing.T) {
	names, calls := l9ReaderClientBindings(t, "scanner_population_test.go", nil)
	selectorData, err := os.ReadFile("source_reader_selectors.json")
	require.NoError(t, err)
	require.True(t, json.Valid(selectorData))
	t.Logf("production lexical client: selector file bound=%v shared extractor called=%v", names, calls)
	require.True(t, names && calls, "source reader selectors must be consumed through the workcensus extractor")
}

func TestL9ReaderSelectorBindingControls(t *testing.T) {
	full := `package fixture
import wc "github.com/orneryd/nornicdb/internal/workcensus"
func run() { read("source_reader_selectors.json"); wc.Extract(cfg) }`
	for _, fixture := range []struct {
		name string
		source string
		file, extract bool
	}{
		{"shared_client", full, true, true},
		{"frozen_client", strings.ReplaceAll(full, "wc.Extract(cfg)", "walk()"), true, false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			name, call := l9ReaderClientBindings(t, "control.go", fixture.source)
			require.Equal(t, fixture.file, name)
			require.Equal(t, fixture.extract, call)
		})
	}
}

func l9ReaderClientBindings(t *testing.T, name string, source any) (usesSelectors, callsExtractor bool) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	require.NoError(t, err)
	sharedName := ""
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		require.NoError(t, err)
		if strings.HasSuffix(path, "/workcensus") {
			sharedName = "workcensus"
			if imported.Name != nil {
				sharedName = imported.Name.Name
			}
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.BasicLit:
			usesSelectors = usesSelectors || n.Value == `"source_reader_selectors.json"`
		case *ast.CallExpr:
			if call, ok := n.Fun.(*ast.SelectorExpr); ok && call.Sel.Name == "Extract" {
				pkg, ok := call.X.(*ast.Ident)
				callsExtractor = callsExtractor || (ok && pkg.Name == sharedName)
			}
		}
		return true
	})
	return usesSelectors, callsExtractor
}
