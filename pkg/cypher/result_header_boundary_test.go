package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// This inventory scans every production package, including Fabric and transports.
// It checks the derivation mechanism, not a hand-maintained list of inferrer names.
func TestResultHeaderConsumerBoundary(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	fset := token.NewFileSet()
	ownedCalls := map[string]bool{}
	err := filepath.WalkDir(filepath.Join(root, "pkg"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasPrefix(filepath.Base(path), "result_columns") || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				ownedCalls[fn.Name.Name] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(filepath.Join(root, "pkg"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		owned := strings.HasPrefix(filepath.Base(path), "result_columns")
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			stringsOut, projection, parses, columnSink := false, false, false, false
			if fn.Type.Results != nil {
				for _, field := range fn.Type.Results.List {
					if a, ok := field.Type.(*ast.ArrayType); ok {
						if name, ok := a.Elt.(*ast.Ident); ok && name.Name == "string" {
							stringsOut = true
						}
					}
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.BasicLit:
					if n.Kind == token.STRING {
						v, _ := strconv.Unquote(n.Value)
						if strings.EqualFold(strings.TrimSpace(v), "RETURN") || strings.EqualFold(strings.TrimSpace(v), "YIELD") {
							projection = true
						}
					}
				case *ast.SelectorExpr:
					if n.Sel.Name == "columns" || n.Sel.Name == "Columns" {
						columnSink = true
					}
				case *ast.CallExpr:
					name := ""
					switch call := n.Fun.(type) {
					case *ast.Ident:
						name = call.Name
					case *ast.SelectorExpr:
						name = call.Sel.Name
					}
					lower := strings.ToLower(name)
					if name == "canonicalizeQueryText" && !owned && fn.Name.Name != "classificationText" {
						t.Errorf("execution normalization bypasses source-column preservation: %s (%s)", fn.Name.Name, fset.Position(n.Pos()))
					}
					if owned && (name == "semanticScopeWalk" || name == "validateMatchSemanticScopes" || name == "validateSemanticScopes") {
						t.Errorf("projection metadata invoked validation: %s", name)
					}
					if strings.Contains(lower, "keyword") || strings.HasPrefix(lower, "parse") || strings.Contains(lower, "split") || name == "LastIndex" || name == "Index" {
						parses = true
					}
					if !owned && (name == "resultHeader" || name == "resultHeaderFor" || name == "resultHeaderFromColumns") && fn.Name.Name != "executeExplain" && fn.Name.Name != "StatementColumns" {
						t.Errorf("ordinary consumer %s may not invoke metadata analysis %s (%s)", fn.Name.Name, name, fset.Position(n.Pos()))
					}
				}
				return true
			})
			if !owned && executionSourceMutation(fn) {
				t.Errorf("destructive lexical rewrite reaches statement execution: %s (%s)", fn.Name.Name, path)
			}
			if (stringsOut || columnSink) && projection && parses {
				rel, _ := filepath.Rel(root, path)
				t.Logf("AST header consumer: %s:%d %s", rel, fset.Position(fn.Pos()).Line, fn.Name.Name)
				if !owned && headerTextDerivation(fn, ownedCalls) {
					t.Errorf("query-derived columns outside result_columns owner: %s (%s)", fn.Name.Name, rel)
				}
			}
			if !owned && (columnSink || (stringsOut && (strings.Contains(strings.ToLower(fn.Name.Name), "column") || strings.Contains(strings.ToLower(fn.Name.Name), "header")))) && !(projection && parses && (stringsOut || columnSink)) && headerTextDerivation(fn, ownedCalls) {
				t.Errorf("query text reaches result columns outside owner: %s (%s)", fn.Name.Name, path)
			}
			if fn.Name.Name == "ProjectedScopeAfterClause" && (fn.Type.Results == nil || len(fn.Type.Results.List) != 1) {
				t.Error("projected scope must return an opaque known/unknown value, not names plus an ignorable bool")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Execution receives the original statement, or the shared owner's rewrite.
// A trivia-only admission check may inspect stripped text without replacing it.
func executionSourceMutation(fn *ast.FuncDecl) bool {
	tainted := map[string]bool{}
	var rewritten func(ast.Expr) bool
	rewritten = func(expression ast.Expr) bool {
		found := false
		ast.Inspect(expression, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.Ident:
				found = found || tainted[node.Name]
			case *ast.CallExpr:
				found = found || headerCallName(node) == "StripComments" || headerCallName(node) == "canonicalizeQueryText"
			}
			return true
		})
		return found
	}
	violation := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.AssignStmt:
			for index, left := range node.Lhs {
				name, ok := left.(*ast.Ident)
				if !ok || name.Name == "_" || len(node.Rhs) == 0 {
					continue
				}
				right := min(index, len(node.Rhs)-1)
				tainted[name.Name] = rewritten(node.Rhs[right])
			}
		case *ast.CallExpr:
			switch headerCallName(node) {
			case "Execute", "ExecuteQuery", "executeInternal", "executeWithImplicitTransaction", "Run", "run":
				for _, argument := range node.Args {
					violation = violation || rewritten(argument)
				}
			}
		}
		return true
	})
	return violation
}

func TestExecutionSourceBoundaryDetector(t *testing.T) {
	for _, sample := range []struct {
		body string
		bad  bool
	}{
		{`func f(query string) { query=StripComments(query); Execute(ctx,query,nil) }`, true},
		{`func f(query string) { query=strings.TrimSpace(cypher.StripComments(query)); run(ctx,db,query,nil) }`, true},
		{`func f(query string) { other:=StripComments(query); alias:=other; Execute(ctx,alias,nil) }`, true},
		{`func f(query string) { query,_=canonicalizeQueryText(query); Execute(ctx,query,nil) }`, true},
		{`func f(query string) { if strings.TrimSpace(StripComments(query))=="" { return }; Execute(ctx,query,nil) }`, false},
		{`func f(query string) { query,_=canonicalizeStatementText(query); Execute(ctx,query,nil) }`, false},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package fixture;"+sample.body, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := executionSourceMutation(file.Decls[0].(*ast.FuncDecl)); got != sample.bad {
			t.Errorf("source mutation=%v want %v: %s", got, sample.bad, sample.body)
		}
	}
}

// headerTextDerivation follows query-string values into header sinks. Parser
// names alone are not a violation: row execution may parse a clause and forward
// actual stream columns. Calls into the projection owner end the text flow.
func headerTextDerivation(fn *ast.FuncDecl, ownedCalls map[string]bool) bool {
	raw := map[string]bool{}
	if fn.Type.Params != nil {
		for _, field := range fn.Type.Params.List {
			if typ, ok := field.Type.(*ast.Ident); ok && typ.Name == "string" {
				for _, name := range field.Names {
					if headerQueryParameter(name.Name) {
						raw[name.Name] = true
					}
				}
			}
		}
	}
	var queryText func(ast.Expr) bool
	queryText = func(expr ast.Expr) bool {
		switch node := expr.(type) {
		case *ast.Ident:
			return raw[node.Name]
		case *ast.BasicLit:
			if node.Kind == token.STRING {
				value, _ := strconv.Unquote(node.Value)
				return value == "*"
			}
		case *ast.SelectorExpr:
			return queryText(node.X)
		case *ast.ParenExpr:
			return queryText(node.X)
		case *ast.SliceExpr:
			return queryText(node.X)
		case *ast.IndexExpr:
			return queryText(node.X)
		case *ast.BinaryExpr:
			return queryText(node.X) || queryText(node.Y)
		case *ast.CallExpr:
			name := headerCallName(node)
			if ownedCalls[name] || name == "make" || name == "new" || name == "len" || name == "cap" {
				return false
			}
			if !headerLexicalCall(name) {
				return false
			}
			for _, arg := range node.Args {
				if queryText(arg) {
					return true
				}
			}
		case *ast.CompositeLit:
			for _, value := range node.Elts {
				if pair, ok := value.(*ast.KeyValueExpr); ok {
					if queryText(pair.Value) {
						return true
					}
				} else if expression, ok := value.(ast.Expr); ok && queryText(expression) {
					return true
				}
			}
		}
		return false
	}
	// Loops and assignments are a small monotone dataflow problem; the fixed
	// point includes aliases built inside a projection-item loop.
	for changed := true; changed; {
		changed = false
		mark := func(expr ast.Expr) {
			if name, ok := expr.(*ast.Ident); ok && name.Name != "_" && !raw[name.Name] {
				raw[name.Name] = true
				changed = true
			}
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch stmt := node.(type) {
			case *ast.AssignStmt:
				for i, value := range stmt.Rhs {
					if queryText(value) {
						if len(stmt.Rhs) == 1 {
							for _, left := range stmt.Lhs {
								mark(left)
							}
						} else if i < len(stmt.Lhs) {
							mark(stmt.Lhs[i])
						}
					}
				}
			case *ast.ValueSpec:
				for i, value := range stmt.Values {
					if queryText(value) && i < len(stmt.Names) {
						mark(stmt.Names[i])
					}
				}
			case *ast.RangeStmt:
				if queryText(stmt.X) {
					mark(stmt.Value)
				}
			}
			return true
		})
	}
	stringsOut := false
	if fn.Type.Results != nil {
		for _, field := range fn.Type.Results.List {
			if typ, ok := field.Type.(*ast.ArrayType); ok {
				if element, ok := typ.Elt.(*ast.Ident); ok && element.Name == "string" {
					stringsOut = true
				}
			}
		}
	}
	violation := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch stmt := node.(type) {
		case *ast.ReturnStmt:
			if stringsOut {
				for i, value := range stmt.Results {
					if headerStringResult(fn, i) && queryText(value) {
						violation = true
					}
				}
			}
		case *ast.AssignStmt:
			for i, left := range stmt.Lhs {
				if field, ok := left.(*ast.SelectorExpr); ok && (field.Sel.Name == "Columns" || field.Sel.Name == "columns") && i < len(stmt.Rhs) && queryText(stmt.Rhs[i]) {
					violation = true
				}
			}
		case *ast.KeyValueExpr:
			if key, ok := stmt.Key.(*ast.Ident); ok && (key.Name == "Columns" || key.Name == "columns") && queryText(stmt.Value) {
				violation = true
			}
		}
		return true
	})
	return violation
}

func headerCallName(call *ast.CallExpr) string {
	switch function := call.Fun.(type) {
	case *ast.Ident:
		return function.Name
	case *ast.SelectorExpr:
		return function.Sel.Name
	}
	return ""
}

func TestResultHeaderConsumerBoundaryDetector(t *testing.T) {
	owner := map[string]bool{"returnProjectionPlanFor": true}
	for _, tc := range []struct {
		source    string
		violation bool
	}{
		{"func bad(query string) []string {part:=query[7:];return []string{part}}", true},
		{"func bad(query string) *Result {part:=strings.TrimSpace(query[7:]);return &Result{Columns:[]string{part}}}", true},
		{`func bad(query string) *Result {columns:=[]string{};for _,item:=range strings.Split(query,","){columns=append(columns,item)};return &Result{Columns:columns}}`, true},
		{`func bad(query string) []string {return []string{"*"}}`, true},
		{"func actual(query string) *Result {result:=execute(query);return &Result{Columns:result.Columns}}", false},
		{"func owner(query string) []string {plan:=returnProjectionPlanFor(query);return plan.columns}", false},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package fixture;"+tc.source, 0)
		if err != nil {
			t.Fatal(err)
		}
		got := headerTextDerivation(file.Decls[0].(*ast.FuncDecl), owner)
		if got != tc.violation {
			t.Errorf("derivation=%v want %v: %s", got, tc.violation, tc.source)
		}
	}
}

func headerQueryParameter(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, "query") || strings.Contains(lower, "cypher") || strings.Contains(lower, "clause") || strings.Contains(lower, "projection") || strings.Contains(lower, "returnpart") || lower == "q"
}
func headerLexicalCall(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasPrefix(lower, "parse") || strings.HasPrefix(lower, "aliases") || name == "append" || name == "string" || strings.HasPrefix(lower, "trim") || strings.HasPrefix(lower, "split") || strings.HasPrefix(lower, "replace") || strings.Contains(lower, "substring") || strings.Contains(lower, "submatch") || strings.Contains(lower, "tolower") || strings.Contains(lower, "toupper")
}
func headerStringResult(fn *ast.FuncDecl, index int) bool {
	position := 0
	for _, field := range fn.Type.Results.List {
		count := len(field.Names)
		if count == 0 {
			count = 1
		}
		if index >= position && index < position+count {
			if array, ok := field.Type.(*ast.ArrayType); ok {
				if element, ok := array.Elt.(*ast.Ident); ok {
					return element.Name == "string"
				}
			}
			return false
		}
		position += count
	}
	return false
}
