package cypher

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestMapPreflightCorpusVerdicts prints, for every complete statement this
// package's tests execute and every openCypher TCK query, the verdict of the
// statement's admission as Execute performs it: the client text goes through
// the production preprocessing (normalization, dialect, Cypher 25 and query
// structure rewrites, label expressions) and then validateSyntax. Run on two
// revisions with NORNICDB_PREFLIGHT_CORPUS=1, the verdict lines are compared
// statement by statement; the harness is identical on both, so any difference
// comes from the product code.
//
// Statements are read from the tests' Go source: a call's query argument is
// folded from string literals, constants, concatenation, fmt.Sprintf and the
// values a range over a literal table gives its variables.
func TestMapPreflightCorpusVerdicts(t *testing.T) {
	if os.Getenv("NORNICDB_PREFLIGHT_CORPUS") != "1" {
		t.Skip("set NORNICDB_PREFLIGHT_CORPUS=1 to print the admission corpus verdicts")
	}
	corpus := map[string]bool{}
	stats := corpusExtractGoQueries(t, ".", corpus)
	features := corpusExtractFeatureQueries(t, filepath.Join("..", "..", "testing", "cypher", "tck", "testdata", "opencypher"), corpus)
	queries := make([]string, 0, len(corpus))
	for query := range corpus {
		queries = append(queries, query)
	}
	sort.Strings(queries)
	fmt.Printf("PREFLIGHT_STATS\t%s\n", mustJSON(map[string]int{"callSites": stats.calls, "resolvedCalls": stats.resolved, "featureQueries": features, "statements": len(queries)}))
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "preflight_corpus"))
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	for _, query := range queries {
		fmt.Fprintf(writer, "PREFLIGHT_VERDICT\t%s\n", mustJSON(map[string]string{"q": query, "v": corpusAdmissionVerdict(executor, query)}))
	}
}

func mustJSON(value interface{}) string {
	data, _ := json.Marshal(value)
	return string(data)
}

// corpusAdmissionVerdict replays Execute's admission steps up to and including
// validateSyntax and describes the first failure (stage, status, detail and
// message), or "ok".
func corpusAdmissionVerdict(e *StorageExecutor, cypher string) (verdict string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			verdict = fmt.Sprintf("panic: %v", recovered)
		}
	}()
	describe := func(stage string, err error) string {
		code, message := nornicerrors.Neo4jStatus(err)
		detail := ""
		var semantic *SemanticError
		if errors.As(err, &semantic) {
			detail = semantic.Detail
		}
		return fmt.Sprintf("%s|%s|%s|%s", stage, code, detail, message)
	}
	cypher = normalizeCypherSyntaxConfusables(trimBOM(cypher))
	if err := internalFunctionCallError(cypher); err != nil {
		return describe("internal", err)
	}
	ctx := context.Background()
	canonical, rewrite, err := normalizeQueryText(ctx, cypher)
	if err != nil {
		return describe("normalize", err)
	}
	if rewrite != nil {
		cypher = canonical
	}
	if grouped, rewrite := canonicalizeNumericLiterals(cypher); rewrite != nil {
		cypher = grouped
	}
	if aliased, rewrite := canonicalizeFunctionAliases(cypher); rewrite != nil {
		cypher = aliased
	}
	cypher = trimTrailingStatementDelimiters(strings.TrimSpace(cypher))
	ctx = withCypherVersion(ctx, cypher)
	if err := validateCypherPreamble(cypher); err != nil {
		return describe("preamble", err)
	}
	cypher, _ = stripCypherPreamble(cypher)
	cypher = strings.TrimSpace(cypher)
	cypher25 := cypherVersionFromContext(ctx) == "25"
	if cypher25 {
		if separated, rewrite := separateCypher25Parameters(cypher); rewrite != nil {
			cypher = separated
		}
	}
	expanded, _, err := desugarCypher25Expressions(cypher, cypher25)
	if err != nil {
		return describe("cypher25", err)
	}
	cypher = expanded
	composed, _, err := desugarQueryStructure(ctx, cypher, e)
	if err != nil {
		return describe("structure", err)
	}
	cypher = composed
	if err := e.validateStatementFraming(ctx, cypher); err != nil {
		return describe("framing", err)
	}
	if stripped, ok := stripUnionBranchFinishes(ctx, originalBraceView(cypher)); ok {
		cypher = strings.TrimSpace(stripped.text())
	}
	if cypher == "" {
		return "empty"
	}
	if isKnowledgePolicyDDLStatement(cypher) {
		if _, _, err := ParseKnowledgePolicyDDL(ctx, cypher, AcceptedSource); err != nil {
			return describe("policy", err)
		}
	}
	if canonical, names := canonicalizeQuotedVariablesWithBindings(ctx, cypher, nil); names != nil {
		cypher = canonical
	}
	if desugared, rewrite := desugarStandaloneOrderClauses(cypher); rewrite != nil {
		cypher = desugared
	}
	if mode, inner := parseExecutionMode(cypher); mode != ModeNormal {
		cypher = inner
	}
	desugared, _, err := desugarLabelExpressions(cypher, nil, cypher25)
	if err != nil {
		return describe("labels", err)
	}
	if err := e.validateSyntax(ctx, originalBraceView(desugared), cypher25, syntaxStatement); err != nil {
		return describe("syntax", err)
	}
	return "ok"
}

type corpusExtractStats struct{ calls, resolved int }

// corpusValue is what an identifier may hold: strings, or table rows whose
// fields hold strings.
type corpusValue struct {
	strings []string
	rows    []map[string][]string
}

type corpusScope struct {
	parent *corpusScope
	values map[string]corpusValue
}

func (scope *corpusScope) lookup(name string) (corpusValue, bool) {
	for current := scope; current != nil; current = current.parent {
		if value, ok := current.values[name]; ok {
			return value, true
		}
	}
	return corpusValue{}, false
}

const corpusFoldLimit = 64

func corpusExtractGoQueries(t *testing.T, directory string, corpus map[string]bool) corpusExtractStats {
	fileSet := token.NewFileSet()
	matches, err := filepath.Glob(filepath.Join(directory, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, path := range matches {
		file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		files = append(files, file)
	}
	structs := map[string][]string{}
	global := &corpusScope{values: map[string]corpusValue{}}
	for _, file := range files {
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range general.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					if structure, ok := spec.Type.(*ast.StructType); ok {
						structs[spec.Name.Name] = corpusStructFields(structure)
					}
				case *ast.ValueSpec:
					for index, name := range spec.Names {
						if index < len(spec.Values) {
							global.values[name.Name] = corpusFoldValue(spec.Values[index], global, structs)
						}
					}
				}
			}
		}
	}
	var stats corpusExtractStats
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			corpusWalk(function.Body, &corpusScope{parent: global, values: map[string]corpusValue{}}, structs, corpus, &stats)
		}
	}
	return stats
}

func corpusStructFields(structure *ast.StructType) []string {
	var fields []string
	for _, field := range structure.Fields.List {
		if len(field.Names) == 0 {
			fields = append(fields, "")
		}
		for _, name := range field.Names {
			fields = append(fields, name.Name)
		}
	}
	return fields
}

// corpusWalk visits statements in order, binding assignments and range
// variables before the calls that use them.
func corpusWalk(node ast.Node, scope *corpusScope, structs map[string][]string, corpus map[string]bool, stats *corpusExtractStats) {
	ast.Inspect(node, func(child ast.Node) bool {
		switch child := child.(type) {
		case *ast.AssignStmt:
			for index, target := range child.Lhs {
				identifier, ok := target.(*ast.Ident)
				if !ok || index >= len(child.Rhs) || len(child.Rhs) != len(child.Lhs) {
					continue
				}
				value := corpusFoldValue(child.Rhs[index], scope, structs)
				if len(value.strings) > 0 || len(value.rows) > 0 {
					scope.values[identifier.Name] = value
				}
			}
		case *ast.RangeStmt:
			inner := &corpusScope{parent: scope, values: map[string]corpusValue{}}
			var source corpusValue
			if literal, ok := child.X.(*ast.CompositeLit); ok {
				source = corpusFoldElements(literal, scope, structs)
			} else if identifier, ok := child.X.(*ast.Ident); ok {
				source, _ = scope.lookup(identifier.Name)
			}
			if literal, ok := child.X.(*ast.CompositeLit); ok {
				if _, isMap := literal.Type.(*ast.MapType); isMap {
					var keys, values []string
					for _, element := range literal.Elts {
						if pair, ok := element.(*ast.KeyValueExpr); ok {
							keys = append(keys, corpusFold(pair.Key, scope, structs)...)
							values = append(values, corpusFold(pair.Value, scope, structs)...)
						}
					}
					if key, ok := child.Key.(*ast.Ident); ok {
						inner.values[key.Name] = corpusValue{strings: keys}
					}
					if value, ok := child.Value.(*ast.Ident); ok {
						inner.values[value.Name] = corpusValue{strings: values}
					}
					corpusWalk(child.Body, inner, structs, corpus, stats)
					return false
				}
			}
			if value, ok := child.Value.(*ast.Ident); ok {
				inner.values[value.Name] = source
			}
			corpusWalk(child.Body, inner, structs, corpus, stats)
			return false
		case *ast.FuncLit:
			corpusWalk(child.Body, &corpusScope{parent: scope, values: map[string]corpusValue{}}, structs, corpus, stats)
			return false
		case *ast.CallExpr:
			selector, ok := child.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Execute" || len(child.Args) < 2 {
				return true
			}
			stats.calls++
			queries := corpusFold(child.Args[1], scope, structs)
			if len(queries) > 0 {
				stats.resolved++
			}
			for _, query := range queries {
				if strings.TrimSpace(query) != "" {
					corpus[query] = true
				}
			}
		}
		return true
	})
}

func corpusFoldValue(expression ast.Expr, scope *corpusScope, structs map[string][]string) corpusValue {
	if literal, ok := expression.(*ast.CompositeLit); ok {
		return corpusFoldElements(literal, scope, structs)
	}
	return corpusValue{strings: corpusFold(expression, scope, structs)}
}

// corpusFoldElements reads a slice literal of strings or of table rows.
func corpusFoldElements(literal *ast.CompositeLit, scope *corpusScope, structs map[string][]string) corpusValue {
	var fields []string
	if array, ok := literal.Type.(*ast.ArrayType); ok {
		switch element := array.Elt.(type) {
		case *ast.StructType:
			fields = corpusStructFields(element)
		case *ast.Ident:
			fields = structs[element.Name]
		case *ast.StarExpr:
			if identifier, ok := element.X.(*ast.Ident); ok {
				fields = structs[identifier.Name]
			}
		}
	}
	var value corpusValue
	for _, element := range literal.Elts {
		if unary, ok := element.(*ast.UnaryExpr); ok {
			element = unary.X
		}
		row, ok := element.(*ast.CompositeLit)
		if !ok {
			value.strings = append(value.strings, corpusFold(element, scope, structs)...)
			continue
		}
		fieldsOfRow := fields
		if identifier, ok := row.Type.(*ast.Ident); ok {
			fieldsOfRow = structs[identifier.Name]
		}
		values := map[string][]string{}
		for index, item := range row.Elts {
			if pair, ok := item.(*ast.KeyValueExpr); ok {
				if key, ok := pair.Key.(*ast.Ident); ok {
					values[key.Name] = corpusFold(pair.Value, scope, structs)
				}
				continue
			}
			if index < len(fieldsOfRow) {
				values[fieldsOfRow[index]] = corpusFold(item, scope, structs)
			}
		}
		value.rows = append(value.rows, values)
	}
	return value
}

// corpusFold returns the strings expression may be, or nil when it can't be
// folded. Concatenations and Sprintf calls fold every combination (bounded).
func corpusFold(expression ast.Expr, scope *corpusScope, structs map[string][]string) []string {
	switch expression := expression.(type) {
	case *ast.BasicLit:
		if expression.Kind == token.STRING {
			if value, err := strconv.Unquote(expression.Value); err == nil {
				return []string{value}
			}
		}
		if expression.Kind == token.INT || expression.Kind == token.FLOAT {
			return []string{expression.Value}
		}
	case *ast.ParenExpr:
		return corpusFold(expression.X, scope, structs)
	case *ast.Ident:
		if value, ok := scope.lookup(expression.Name); ok {
			return value.strings
		}
	case *ast.SelectorExpr:
		if identifier, ok := expression.X.(*ast.Ident); ok {
			if value, ok := scope.lookup(identifier.Name); ok {
				var folded []string
				for _, row := range value.rows {
					folded = append(folded, row[expression.Sel.Name]...)
				}
				return folded
			}
		}
	case *ast.BinaryExpr:
		if expression.Op != token.ADD {
			return nil
		}
		left, right := corpusFold(expression.X, scope, structs), corpusFold(expression.Y, scope, structs)
		var folded []string
		for _, l := range left {
			for _, r := range right {
				if len(folded) < corpusFoldLimit {
					folded = append(folded, l+r)
				}
			}
		}
		return folded
	case *ast.CallExpr:
		selector, ok := expression.Fun.(*ast.SelectorExpr)
		if !ok || len(expression.Args) == 0 {
			return nil
		}
		if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "fmt" || selector.Sel.Name != "Sprintf" {
			return nil
		}
		combinations := [][]string{nil}
		for _, argument := range expression.Args {
			values := corpusFold(argument, scope, structs)
			if len(values) == 0 {
				return nil
			}
			var next [][]string
			for _, prefix := range combinations {
				for _, value := range values {
					if len(next) < corpusFoldLimit {
						next = append(next, append(append([]string{}, prefix...), value))
					}
				}
			}
			combinations = next
		}
		var folded []string
		for _, combination := range combinations {
			arguments := make([]interface{}, len(combination)-1)
			for index, value := range combination[1:] {
				if number, err := strconv.ParseInt(value, 10, 64); err == nil && strings.Contains(combination[0], "%d") {
					arguments[index] = number
				} else {
					arguments[index] = value
				}
			}
			folded = append(folded, fmt.Sprintf(combination[0], arguments...))
		}
		return folded
	}
	return nil
}

// corpusExtractFeatureQueries adds each query of a TCK scenario step
// ("When executing query:", "And having executed:", …) to corpus.
func corpusExtractFeatureQueries(t *testing.T, root string, corpus map[string]bool) int {
	count := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".feature") {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
		for index := 0; index < len(lines); index++ {
			step := strings.TrimSpace(lines[index])
			if !strings.HasSuffix(step, "executing query:") && !strings.HasSuffix(step, "executed:") {
				continue
			}
			if index+1 >= len(lines) || strings.TrimSpace(lines[index+1]) != `"""` {
				continue
			}
			indent := strings.Index(lines[index+1], `"""`)
			var query []string
			for index += 2; index < len(lines) && strings.TrimSpace(lines[index]) != `"""`; index++ {
				line := lines[index]
				if len(line) >= indent && strings.TrimSpace(line[:indent]) == "" {
					line = line[indent:]
				}
				query = append(query, line)
			}
			if text := strings.Join(query, "\n"); strings.TrimSpace(text) != "" {
				corpus[text] = true
				count++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}
