package cyphertext

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This is an architectural guard, not a second parser: structural grammar may
// inspect owned token spans, but only cyphertext may scan quoted/comment spans.
func lexicalOwnershipViolations(file *ast.File, fset *token.FileSet, path string) []string {
	var violations []string
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if _, otherGrammar := nonCypherGrammarOwners[path+":"+fn.Name.Name]; otherGrammar {
			continue
		}
		if delegate, facade := braceSyntaxFacades[fn.Name.Name]; facade {
			valid := false
			if len(fn.Body.List) == 1 {
				if ret, ok := fn.Body.List[0].(*ast.ReturnStmt); ok && len(ret.Results) == 1 {
					if call, ok := ret.Results[0].(*ast.CallExpr); ok {
						if target, ok := call.Fun.(*ast.Ident); ok && target.Name == delegate {
							valid = true
						}
					}
				}
			}
			if !valid {
				violations = append(violations, fmt.Sprintf("%s:%d %s must delegate brace ownership to %s", path, fset.Position(fn.Pos()).Line, fn.Name.Name, delegate))
			}
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch loop := node.(type) {
			case *ast.ForStmt:
				if rawLexicalProbe(loop) {
					violations = append(violations, fmt.Sprintf("%s:%d %s scans raw lexical delimiters", path, fset.Position(loop.Pos()).Line, fn.Name.Name))
				}
				return false
			case *ast.RangeStmt:
				if rawLexicalProbe(loop) {
					violations = append(violations, fmt.Sprintf("%s:%d %s scans raw lexical delimiters", path, fset.Position(loop.Pos()).Line, fn.Name.Name))
				}
				return false
			}
			return true
		})
		if path != "pkg/cypher/brace_syntax.go" {
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				ret, ok := node.(*ast.ReturnStmt)
				if !ok {
					return true
				}
				for _, value := range ret.Results {
					name, ok := value.(*ast.Ident)
					if ok && (name.Name == "literalBrace" || name.Name == "projectionBrace" || name.Name == "subqueryBrace" || name.Name == "quantifiedBrace") {
						violations = append(violations, fmt.Sprintf("%s:%d %s decides brace ownership outside its owner", path, fset.Position(ret.Pos()).Line, fn.Name.Name))
					}
				}
				return true
			})
		}
	}
	return violations
}

// These APIs consume other languages as data, not Cypher query source. Their
// lexical rules deliberately differ (for example Java pattern quotes), so the
// Cypher tokenizer cannot replace them. Exemptions are per owner, never files.
var nonCypherGrammarOwners = map[string]string{
	"internal/rerankwire/decode.go:validateEnvelopeProse":      "JSON/Markdown response-envelope prose, not query text",
	"pkg/cypher/fulltext_query.go:ftTokenize":                  "Lucene search-string grammar",
	"pkg/cypher/functions_cypher25.go:javaReplacementTemplate": "Java regular-expression replacement template",
	"pkg/cypher/json_values.go:unescapeLineSeparators":         "JSON output escape encoding",
	"pkg/cypher/temporal_format.go:formatDurationPattern":      "Java duration format-string grammar",
	"pkg/cypher/temporal_format.go:stripDurationQuotes":        "Java duration format-string quoting",
	"pkg/cypher/temporal_pattern.go:compileTemporalPattern":    "Java date/time pattern grammar",
}
var braceSyntaxFacades = map[string]string{
	"rowMapProjectionSplit": "completeProjection",
	"mapProjectionBrace":    "enclosingProjectionBrace",
	"opensSubquery":         "precedingSubqueryExpressionKeyword",
}

func rawLexicalProbe(node ast.Node) bool {
	found := false
	ownedComments := hasOwnedCommentSpan(node)
	ast.Inspect(node, func(node ast.Node) bool {
		switch probe := node.(type) {
		case *ast.BinaryExpr:
			if probe.Op != token.EQL && probe.Op != token.NEQ && probe.Op != token.LAND && probe.Op != token.LOR {
				break
			}
			if lexicalLiteral(probe.X) || lexicalLiteral(probe.Y) {
				found = true
			}
			// Comment lexing probes adjacent source bytes. Testing the same
			// operator against slash/star is arithmetic grammar, not a lexer.
			if adjacentCommentBytes(probe) && !ownedComments {
				found = true
			}
		case *ast.CaseClause:
			for _, value := range probe.List {
				if lexicalLiteral(value) {
					found = true
				}
			}
		case *ast.CallExpr:
			call, ok := probe.Fun.(*ast.SelectorExpr)
			if !ok {
				break
			}
			switch call.Sel.Name {
			case "Index", "IndexByte", "IndexAny", "LastIndex", "LastIndexByte", "HasPrefix":
				for _, argument := range probe.Args {
					if lexicalLiteral(argument) {
						found = true
					}
				}
			}
		}
		return !found
	})
	return found
}

func adjacentCommentBytes(expression ast.Expr) bool {
	type probe struct {
		source, index string
		offset        int
		mark          string
	}
	var probes []probe
	ast.Inspect(expression, func(node ast.Node) bool {
		comparison, ok := node.(*ast.BinaryExpr)
		if !ok || comparison.Op != token.EQL && comparison.Op != token.NEQ {
			return true
		}
		for _, pair := range [][2]ast.Expr{{comparison.X, comparison.Y}, {comparison.Y, comparison.X}} {
			literal, ok := pair[1].(*ast.BasicLit)
			if !ok {
				continue
			}
			mark, err := strconv.Unquote(literal.Value)
			if err != nil || mark != "/" && mark != "*" {
				continue
			}
			read, ok := pair[0].(*ast.IndexExpr)
			if !ok {
				continue
			}
			source, ok := read.X.(*ast.Ident)
			if !ok {
				continue
			}
			index := read.Index
			offset := 0
			if sum, ok := index.(*ast.BinaryExpr); ok && sum.Op == token.ADD {
				amount, ok := sum.Y.(*ast.BasicLit)
				if !ok {
					continue
				}
				offset, err = strconv.Atoi(amount.Value)
				if err != nil {
					continue
				}
				index = sum.X
			}
			name, ok := index.(*ast.Ident)
			if !ok {
				continue
			}
			probes = append(probes, probe{source.Name, name.Name, offset, mark})
		}
		return true
	})
	for _, left := range probes {
		for _, right := range probes {
			if left.source == right.source && left.index == right.index && right.offset == left.offset+1 && left.mark == "/" && (right.mark == "/" || right.mark == "*") {
				return true
			}
		}
	}
	return false
}

func hasOwnedCommentSpan(node ast.Node) bool {
	owned := false
	ast.Inspect(node, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			switch target := call.Fun.(type) {
			case *ast.Ident:
				owned = owned || target.Name == "queryCommentEnd" || target.Name == "skipCypherLexicalToken"
			case *ast.SelectorExpr:
				owned = owned || target.Sel.Name == "TokenAt" || target.Sel.Name == "CommentEnd"
			}
		}
		return !owned
	})
	return owned
}

func lexicalLiteral(expression ast.Expr) bool {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.CHAR && literal.Kind != token.STRING {
		return false
	}
	value, err := strconv.Unquote(literal.Value)
	return err == nil && (value == "'" || value == "\"" || value == "`" || value == "\\" || value == "//" || value == "/*" || value == "*/")
}

func TestLexicalOwnershipGuard(t *testing.T) {
	root := filepath.Join("..", "..")
	var violations []string
	for _, directory := range []string{"pkg/cypher", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(source[:min(len(source), 300)]), "Code generated") {
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, source, 0)
			if err != nil {
				return err
			}
			// Other encodings inside the canonical owner (UTF-8/runes and
			// string escape decoding) are representations of this one lexer.
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			if strings.HasPrefix(filepath.ToSlash(relative), "internal/cyphertext/") {
				return nil
			}
			violations = append(violations, lexicalOwnershipViolations(file, fset, filepath.ToSlash(relative))...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(violations)
	for _, violation := range violations {
		t.Error(violation)
	}
}

func TestLexicalOwnershipGuardMutations(t *testing.T) {
	for _, source := range []string{
		"package cypher; func legacy(q string){for i:=0;i<len(q);i++ {if q[i]=='`' {i++}}}",
		"package cypher; func legacy(q string){for i:=0;i<len(q);i++ {if q[i]=='\\\\' {i++}}}",
		"package cypher; func legacy(q string){for i:=0;i<len(q);i++ {if strings.HasPrefix(q[i:],\"/*\") {i++}}}",
		"package cypher; func legacy(q string){for i:=0;i+1<len(q);i++ {if q[i]=='/'&&q[i+1]=='*' {i++}}}",
		"package cypher; func legacy(q string){for i:=0;i+1<len(q);i++ {if '/'==q[i]&&'*'==q[i+1] {i++}}}",
		"package cypher; func guessed(q string) mapBraceKind {if strings.Contains(q,\":\") {return literalBrace}; return projectionBrace}",
		"package cypher; func rowMapProjectionSplit(q string) bool {return strings.Contains(q,\":\")}",
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "mutation.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(lexicalOwnershipViolations(file, fset, "pkg/cypher/mutation.go")) == 0 {
			t.Fatalf("guard accepted private lexer/brace owner: %s", source)
		}
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "owned.go", "package cypher;func owned(q string){r:=cyphertext.NewReader(q,0);for s,ok:=r.Next();ok;s,ok=r.Next(){_ = s}}", 0)
	if err != nil {
		t.Fatal(err)
	}
	if problems := lexicalOwnershipViolations(file, fset, "pkg/cypher/owned.go"); len(problems) > 0 {
		t.Fatalf("owned token consumer rejected: %v", problems)
	}
	file, err = parser.ParseFile(fset, "operators.go", "package cypher;func operators(q string){for _,c:=range q{if c=='/'||c=='*'{continue}}}", 0)
	if err != nil {
		t.Fatal(err)
	}
	if problems := lexicalOwnershipViolations(file, fset, "pkg/cypher/operators.go"); len(problems) > 0 {
		t.Fatalf("operator grammar rejected: %v", problems)
	}
}
