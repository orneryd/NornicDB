package cypher

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

// This guard only checks constant USE/GRAPH token needles in known text-search
// primitives. It makes no claim about variable needles or arbitrary wrappers:
// the generated consumer property protects behavior across those boundaries.
// No call-site allowlist, source fingerprints or reaching-value analysis.
var useOwnerSearchImporter = importer.Default()
var useOwnerConstantToken = regexp.MustCompile(`(?i)\b(?:USE|GRAPH)\b`)

func useOwnerSearchViolations(path string, source []byte) ([]string, error) {
	set := token.NewFileSet()
	tree, err := parser.ParseFile(set, path, source, 0)
	if err != nil {
		return nil, err
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}
	config := types.Config{Importer: useOwnerSearchImporter, Error: func(error) {}}
	pkg, _ := config.Check(tree.Name.Name, set, []*ast.File{tree}, info)
	return useOwnerConstantViolations(set, tree, info, pkg), nil
}

func useOwnerConstantViolations(set *token.FileSet, tree *ast.File, info *types.Info, pkg *types.Package) []string {
	imports := map[string]string{}
	for _, spec := range tree.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		alias := filepath.Base(path)
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		imports[alias] = path
	}
	value := func(expr ast.Expr) string {
		// A []byte conversion of a constant string still has a constant needle.
		if call, ok := ast.Unparen(expr).(*ast.CallExpr); ok && len(call.Args) == 1 {
			if array, ok := call.Fun.(*ast.ArrayType); ok && array.Len == nil {
				expr = call.Args[0]
			}
		}
		v := info.Types[expr].Value
		if v == nil {
			_ = types.CheckExpr(set, pkg, expr.Pos(), expr, info)
			v = info.Types[expr].Value
		}
		if v != nil && v.Kind() == constant.String {
			return constant.StringVal(v)
		}
		return ""
	}
	var violations []string
	ast.Inspect(tree, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, library := "", ""
		switch fn := ast.Unparen(call.Fun).(type) {
		case *ast.SelectorExpr:
			name = fn.Sel.Name
			if id, ok := fn.X.(*ast.Ident); ok {
				library = imports[id.Name]
			}
		case *ast.Ident:
			name = fn.Name
			if object := info.Uses[fn]; object != nil && object.Pkg() != nil {
				library = object.Pkg().Path()
			}
		}
		index := -1
		switch library {
		case "regexp":
			switch name {
			case "Compile", "MustCompile", "CompilePOSIX", "MustCompilePOSIX", "Match", "MatchString", "MatchReader":
				index = 0
			}
		case "strings", "bytes":
			switch name {
			case "Contains", "ContainsAny", "Index", "IndexAny", "LastIndex", "LastIndexAny", "HasPrefix", "HasSuffix", "EqualFold", "Cut", "CutPrefix", "CutSuffix", "Split", "SplitN", "SplitAfter", "SplitAfterN", "Count", "Replace", "ReplaceAll":
				index = 1
			}
		default:
			switch name {
			case "GetCachedRegex":
				index, library = 0, "regexp"
			case "matchKeywordAt", "hasKeywordAt", "keywordAt", "clauseKeywordAt", "keywordIndexFrom":
				index = 2
			case "containsFold", "indexFold", "startsWithFold", "endsWithFold", "hasPrefixFold", "hasSuffixFold", "startsWithWordFold", "indexCaseInsensitive", "indexASCIIFold", "containsKeyword", "ContainsKeyword", "countKeywordOccurrences", "extractIntAfterKeyword", "extractStringAfterKeyword", "findAllKeywordPositions", "findTopLevelKeyword", "startsWithKeywordFold", "endsWithKeywordFold", "topLevelKeywordIndex", "FindKeywordIndex":
				index = 1
			}
		}
		// keywordIndexFrom's needle precedes its start offset.
		if name == "keywordIndexFrom" {
			index = 1
		}
		if index < 0 || index >= len(call.Args) {
			return true
		}
		needles := []string{value(call.Args[index])}
		if name == "EqualFold" {
			needles = append(needles, value(call.Args[0]))
		}
		for _, needle := range needles {
			selector := useOwnerConstantToken.MatchString(strings.NewReplacer(`\b`, " ", `\s`, " ").Replace(needle))
			if library == "regexp" {
				// Decode regex literal runs, including escaped letters and
				// singleton character classes; no regex data-flow tracking.
				if pattern, err := syntax.Parse(needle, syntax.Perl); err == nil {
					var literal func(*syntax.Regexp) string
					literal = func(re *syntax.Regexp) string {
						switch re.Op {
						case syntax.OpLiteral:
							return string(re.Rune)
						case syntax.OpCharClass:
							var folded rune
							for i := 0; i < len(re.Rune); i += 2 {
								if re.Rune[i] != re.Rune[i+1] || i > 0 && unicode.ToUpper(re.Rune[i]) != folded {
									return " "
								}
								folded = unicode.ToUpper(re.Rune[i])
							}
							return string(folded)
						case syntax.OpConcat, syntax.OpCapture:
							var text strings.Builder
							for _, child := range re.Sub {
								text.WriteString(literal(child))
							}
							return text.String()
						}
						return " "
					}
					var visit func(*syntax.Regexp)
					visit = func(re *syntax.Regexp) {
						if useOwnerConstantToken.MatchString(literal(re)) {
							selector = true
						}
						for _, child := range re.Sub {
							visit(child)
						}
					}
					visit(pattern)
				}
			}
			if selector {
				violations = append(violations, fmt.Sprintf("%s: %s has constant USE/GRAPH needle", set.Position(call.Pos()), name))
				break
			}
		}
		return true
	})
	return violations
}

func TestUseOwnerSearchGuard(t *testing.T) {
	type file struct {
		path string
		tree *ast.File
	}
	set := token.NewFileSet()
	groups := map[string][]file{}
	// These existing files own USE recognition and policy. This is an owner
	// boundary, not an exemption for arbitrary consumer searches.
	owners := map[string]bool{"executor_use.go": true, "antlr/use_reference.go": true, "../fabric/use_clause.go": true}
	for _, root := range []string{".", "../fabric"} {
		require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			path = filepath.ToSlash(path)
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || owners[path] {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			tree, err := parser.ParseFile(set, path, source, 0)
			if err != nil {
				return err
			}
			group := filepath.Dir(path) + ":" + tree.Name.Name
			groups[group] = append(groups[group], file{path, tree})
			return nil
		}))
	}
	for group, files := range groups {
		trees := make([]*ast.File, 0, len(files))
		for _, file := range files {
			trees = append(trees, file.tree)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}
		config := types.Config{Importer: useOwnerSearchImporter, Error: func(error) {}}
		pkg, _ := config.Check(group, set, trees, info)
		for _, file := range files {
			for _, violation := range useOwnerConstantViolations(set, file.tree, info, pkg) {
				t.Errorf("%s must consume the USE owner", violation)
			}
		}
	}
}

func TestUseOwnerSearchGuardConstants(t *testing.T) {
	for _, test := range []struct {
		name, source string
		violations   int
	}{
		{"literal", `func read(q string) bool { return strings.Contains(q, "MATCH") }`, 0},
		{"constant", `const needle = "US" + "E"; func read(q string) bool { return strings.Contains(q, needle) }`, 1},
		{"case", `func read(q string) bool { return strings.EqualFold(q, "gRaPh") }`, 1},
		{"symmetric", `func read(q string) bool { return strings.EqualFold("UsE", q) }`, 1},
		{"shadow", `const needle = "USE"; func read(q string) bool { const needle = "MATCH"; return strings.Contains(q, needle) }`, 0},
		{"variable outside static contract", `func read(q string) bool { needle := "USE"; return strings.Contains(q, needle) }`, 0},
		{"parameter outside static contract", `func read(q, needle string) bool { return strings.HasPrefix(q, needle) }`, 0},
		{"loop outside static contract", `func read(q string) { for _, needle := range []string{"USE", "GRAPH"} { _ = strings.Contains(q, needle) } }`, 0},
		{"bytes", `func read(q []byte) bool { return bytes.Contains(q, []byte("USE")) }`, 1},
		{"parentheses", `func read(q string) bool { return (strings.Contains)(q, "USE") }`, 1},
		{"regex", `var re = regexp.MustCompile("(?i)\\bUSE\\s+GRAPH")`, 1},
		{"regex escape", `var re = regexp.MustCompile("(?i)\\x55SE")`, 1},
		{"regex class", `var re = regexp.MustCompile("(?i)U[S]E")`, 1},
		{"regex wrapper", `func read() { _ = GetCachedRegex("(?i)GRAPH") }`, 1},
		{"keyword helper", `func read(q string) { _ = findTopLevelKeyword(q, "USE") }`, 1},
		{"owner consumer", `func read(q string) { fabric.ReadUseClause(q, true) }`, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			violations, err := useOwnerSearchViolations("consumer.go", []byte("package cypher\nimport (\"strings\"; \"bytes\"; \"regexp\")\n"+test.source))
			require.NoError(t, err)
			require.Len(t, violations, test.violations)
		})
	}
	for _, source := range []string{
		`package cypher; import s "strings"; func read(q string) bool { return s.Contains(q, "USE") }`,
		`package cypher; import . "strings"; func read(q string) bool { return Contains(q, "GRAPH") }`,
	} {
		violations, err := useOwnerSearchViolations("consumer.go", []byte(source))
		require.NoError(t, err)
		require.Len(t, violations, 1)
	}
}
