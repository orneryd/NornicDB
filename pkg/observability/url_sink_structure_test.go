package observability_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// This inventory starts with configured endpoint reads, not redactor callers.
// It follows local assignments and string-returning endpoint helpers, and lists
// each raw endpoint egress expression. Approved runtime-only edges are explicit
// in the reviewed manifest; a new raw diagnostic/metadata sink cannot inherit a
// function-wide exemption merely because that function also calls a redactor.
// The companion non-egress manifest classifies only exact expression hashes.
// After editing a URL or credential flow, run this focused test through tnrun on
// TrueNAS, inspect every changed function, then update the hash and its verdict
// in one of the manifests and rerun the test. Drift output contains keys only:
// endpoint expressions can contain credentials and must never appear in logs.
var endpointName = regexp.MustCompile(`(?i)(url|uri|endpoint|apikey)$|^(APIPath|EmbeddingSpace|Issuer|Effective|Overrides|Constituents|databases|DefValue)$`)

type endpointFunction struct {
	path, name string
	decl       *ast.FuncDecl
	fileset    *token.FileSet
}
type endpointSink struct {
	Function   string `json:"function"`
	Expression string `json:"expression"`
	SHA256     string `json:"sha256"`
	Line       int    `json:"line,omitempty"`
}

func sourceText(node ast.Node) string {
	var b bytes.Buffer
	_ = format.Node(&b, token.NewFileSet(), node)
	return b.String()
}

func endpointInventoryKeys(actual map[string]string) []string {
	keys := make([]string, 0, len(actual))
	for key := range actual {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func endpointCallName(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

func endpointFunctionCalls(fn *ast.FuncDecl, name string) bool {
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && endpointCallName(call) == name {
			found = true
			return false
		}
		return !found
	})
	return found
}

func safeEndpointCall(name string) bool {
	switch name {
	case "RedactURL", "RedactText", "RedactError", "RedactErrorDetails", "RedactClientError", "DisplayValue", "DisplayArgs", "DisplayFlagDefault", "redactOwnedCredentials", "redactAdminCommandError", "redactAdminCommandErrorSource", "RemoteUserForDisplay":
		return true
	}
	return false
}

func credentialOwnerCall(name string) bool {
	return safeEndpointCall(name) || name == "ClientCredentials" || name == "ContextCredentials" || name == "MapURLCredentials" || name == "ComposeRequestURL" || name == "NormalizeRequestBaseURL" || name == "mapURLCredentialQuery" || name == "IsSensitiveURLQueryName" || name == "credentialPercentProjection"
}

func endpointTainted(expr ast.Expr, locals, returns map[string]bool) bool {
	if expr == nil {
		return false
	}
	switch x := expr.(type) {
	case *ast.CallExpr:
		name := endpointCallName(x)
		if safeEndpointCall(name) {
			return false
		}
		if name == "GetOverrides" {
			return true
		}
		if returns[name] {
			return true
		}
		if selector, ok := x.Fun.(*ast.SelectorExpr); ok && endpointTainted(selector.X, locals, returns) {
			return true
		}
		for _, arg := range x.Args {
			if endpointTainted(arg, locals, returns) {
				return true
			}
		}
		return false
	case *ast.SelectorExpr:
		if x.Sel.Name == "Args" && sourceText(x.X) == "os" {
			return true
		}
		return endpointName.MatchString(x.Sel.Name) || endpointTainted(x.X, locals, returns)
	case *ast.Ident:
		return locals[x.Name]
	}
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if n == nil || n == expr {
			return true
		}
		if child, ok := n.(ast.Expr); ok {
			if endpointTainted(child, locals, returns) {
				found = true
			}
			return false
		}
		return true
	})
	return found
}

func endpointLocals(fn *ast.FuncDecl, returns map[string]bool) map[string]bool {
	locals := map[string]bool{}
	if fn.Type.Params != nil {
		for _, field := range fn.Type.Params.List {
			kind := sourceText(field.Type)
			// Normalization moves URL userinfo into these runtime records. Their
			// fields remain in the population after the original URI loses User.
			if strings.Contains(kind, "RemoteEngineConfig") || strings.Contains(kind, "ConstituentRef") {
				for _, name := range field.Names {
					locals[name.Name] = true
				}
			}
			if kind == "string" || kind == "map[string]string" || kind == "*url.URL" || (credentialOwnerCall(fn.Name.Name) && (kind == "[]string" || kind == "...string")) {
				for _, name := range field.Names {
					if credentialOwnerCall(fn.Name.Name) || endpointName.MatchString(name.Name) {
						locals[name.Name] = true
					}
				}
			}
		}
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			var left []ast.Expr
			var right []ast.Expr
			switch x := n.(type) {
			case *ast.AssignStmt:
				left, right = x.Lhs, x.Rhs
			case *ast.ValueSpec:
				for _, id := range x.Names {
					left = append(left, id)
				}
				right = x.Values
			case *ast.RangeStmt:
				if endpointTainted(x.X, locals, returns) {
					if x.Key != nil {
						left = append(left, x.Key)
					}
					if x.Value != nil {
						left = append(left, x.Value)
					}
					right = []ast.Expr{x.X}
				}
			}
			// Configured flags/environment reads can originate in a constructor
			// whose name carries no endpoint type (flag.String, os.Getenv).
			// Seed their semantic destination names as well as RHS field reads.
			for _, lhs := range left {
				if id, ok := lhs.(*ast.Ident); ok && endpointName.MatchString(id.Name) && !locals[id.Name] {
					locals[id.Name] = true
					changed = true
				}
			}
			for i, rhs := range right {
				if !endpointTainted(rhs, locals, returns) {
					continue
				}
				for j, lhs := range left {
					if len(right) == len(left) && j != i {
						continue
					}
					if id, ok := lhs.(*ast.Ident); ok && id.Name != "_" && !locals[id.Name] {
						locals[id.Name] = true
						changed = true
					}
				}
			}
			return true
		})
	}
	return locals
}

func TestCredentialURLSinkStructure(t *testing.T) {
	root := filepath.Join("..", "..")
	var functions []endpointFunction
	for _, dir := range []string{"pkg", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(data, []byte("Code generated")) {
				return nil
			}
			fileset := token.NewFileSet()
			file, err := parser.ParseFile(fileset, path, data, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
					name := fn.Name.Name
					if fn.Recv != nil {
						name = sourceText(fn.Recv.List[0].Type) + "." + name
					}
					functions = append(functions, endpointFunction{filepath.ToSlash(rel), name, fn, fileset})
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	returns := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, fn := range functions {
			if fn.decl.Name.Name == "String" || safeEndpointCall(fn.decl.Name.Name) || returns[fn.decl.Name.Name] || fn.decl.Type.Results == nil {
				continue
			}
			stringResult := false
			for _, field := range fn.decl.Type.Results.List {
				if sourceText(field.Type) == "string" {
					stringResult = true
				}
			}
			if !stringResult {
				continue
			}
			locals := endpointLocals(fn.decl, returns)
			ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
				if ret, ok := n.(*ast.ReturnStmt); ok {
					for _, value := range ret.Results {
						if endpointTainted(value, locals, returns) {
							returns[fn.decl.Name.Name] = true
							changed = true
						}
					}
				}
				return true
			})
		}
	}
	var sinks []endpointSink
	hasInternalExecute := false
	for _, fn := range functions {
		if fn.path == "pkg/cypher/executor_entry.go" && fn.decl.Name.Name == "execute" {
			hasInternalExecute = true
		}
	}
	for _, fn := range functions {
		locals := endpointLocals(fn.decl, returns)
		clientErrorBoundary := endpointFunctionCalls(fn.decl, "RedactClientError")
		add := func(node ast.Node) {
			expression := sourceText(node)
			// Critical projection and owner functions are inventoried as whole
			// declarations below. Other approvals follow the egress expression,
			// so an unrelated base change in the same function does not silently
			// turn reviewed runtime plumbing into an unknown credential sink.
			hash := sha256.Sum256([]byte(expression))
			line := 0
			if clientErrorBoundary {
				// Identical raw exits in a credentialed client boundary are
				// independently reviewed: removing the owner from only one must
				// not collapse into an already-approved sibling expression.
				line = fn.fileset.Position(node.Pos()).Line
			}
			sinks = append(sinks, endpointSink{fn.path + ":" + fn.name, expression, hex.EncodeToString(hash[:]), line})
		}
		sharedExit := fn.path == "pkg/cypher/executor_entry.go" && fn.decl.Name.Name == "Execute"
		sharedExecuteOwner := fn.decl.Name.Name == "Execute" && !hasInternalExecute || fn.decl.Name.Name == "execute" && hasInternalExecute
		generationBoundary := fn.path == "pkg/heimdall/scheduler.go" && (fn.decl.Name.Name == "Generate" || fn.decl.Name.Name == "GenerateStream" || fn.decl.Name.Name == "GenerateWithTools")
		if fn.path == "pkg/cypher/executor_entry.go" && sharedExecuteOwner && len(fn.decl.Body.List) > 0 {
			// Public Execute and transparent routed execution share this owner.
			// It must bind the original statement before any normalization or
			// routing can reject it; a same-function dummy call is insufficient.
			if deferStmt, ok := fn.decl.Body.List[0].(*ast.DeferStmt); ok {
				call := deferStmt.Call
				if endpointCallName(call) == "redactAdminCommandError" {
					add(deferStmt)
				}
				// The inline form captures the original source by value while reading
				// the final statement credential set after the consuming DDL reader.
				if inline, ok := call.Fun.(*ast.FuncLit); ok && len(call.Args) == 1 && sourceText(call.Args[0]) == "cypher" && len(inline.Body.List) == 1 && len(inline.Type.Params.List) == 1 && len(inline.Type.Params.List[0].Names) == 1 {
					if expression, ok := inline.Body.List[0].(*ast.ExprStmt); ok {
						if project, ok := expression.X.(*ast.CallExpr); ok && endpointCallName(project) == "redactAdminCommandError" && len(project.Args) >= 2 && sourceText(project.Args[0]) == inline.Type.Params.List[0].Names[0].Name && sourceText(project.Args[1]) == "&retErr" {
							add(deferStmt)
						}
					}
				}
			}
		}
		configProjection := (fn.path == "pkg/config/display_errors.go" && fn.decl.Name.Name == "configDisplayError") ||
			(fn.path == "pkg/server/server_dbconfig.go" && fn.decl.Name.Name == "redactDatabaseSettings") ||
			(fn.path == "pkg/cypher/executor_show.go" && fn.decl.Name.Name == "executeShowSettings")
		bootstrapProjection := fn.path == "pkg/observability/log_events.go" &&
			(fn.decl.Name.Name == "logBootstrapEvent" || fn.decl.Name.Name == "logSlogBootstrapEvent")
		if credentialOwnerCall(fn.decl.Name.Name) || sharedExit || configProjection || bootstrapProjection || generationBoundary {
			add(fn.decl)
		}
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.ReturnStmt:
				for _, value := range x.Results {
					if endpointTainted(value, locals, returns) {
						add(x)
						break
					}
				}
			case *ast.CallExpr:
				name := endpointCallName(x)
				if credentialOwnerCall(name) {
					// Inventory the complete owned callback boundary as well. A
					// new raw sink inside a credential mapper must not disappear
					// merely because the outer call is the canonical owner.
					if name == "MapURLCredentials" || name == "mapURLCredentialQuery" {
						add(x)
					}
					// Extraction returns raw credential context, so it is a source,
					// never a sanitizer for a future metadata/log consumer.
					if safeEndpointCall(name) {
						return false
					}
				}
				sink := false
				for _, word := range []string{"Print", "Errorf", "Sprintf", "Log", "Info", "Warn", "Debug", "String", "SetAttributes", "WithLabel", "Marshal", "Encode", "Write", "CreateNode", "UpdateNode"} {
					sink = sink || strings.HasPrefix(strings.ToLower(name), strings.ToLower(word))
				}
				if sink {
					for _, arg := range x.Args {
						if endpointTainted(arg, locals, returns) {
							add(x)
							break
						}
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range x.Lhs {
					if _, local := lhs.(*ast.Ident); local {
						continue
					}
					for _, rhs := range x.Rhs {
						if endpointTainted(rhs, locals, returns) {
							add(x)
							return true
						}
					}
				}
			}
			return true
		})
	}
	sort.Slice(sinks, func(i, j int) bool {
		a, b := sinks[i], sinks[j]
		return fmt.Sprintf("%s%s%08d", a.Function, a.SHA256, a.Line) < fmt.Sprintf("%s%s%08d", b.Function, b.SHA256, b.Line)
	})
	duplicateCounts := map[string]int{}
	for _, sink := range sinks {
		duplicateCounts[sink.Function+" "+sink.SHA256]++
	}
	actual := map[string]string{}
	occurrences := map[string]int{}
	for _, sink := range sinks {
		baseKey := sink.Function + " " + sink.SHA256
		key := baseKey
		if duplicateCounts[baseKey] > 1 && sink.Line > 0 {
			// Use the AST-order occurrence rather than a source line. Line
			// numbers churn when unrelated code moves, while a removed or
			// inserted identical exit still changes the reviewed population.
			occurrences[baseKey]++
			key = fmt.Sprintf("%s#N%d %s", sink.Function, occurrences[baseKey], sink.SHA256)
		}
		actual[key] = sink.Expression
	}
	data, err := os.ReadFile("testdata/url_sink_inventory.json")
	if err != nil {
		encoded, _ := json.MarshalIndent(endpointInventoryKeys(actual), "", "  ")
		t.Log("URL_SINK_KEYS_JSON " + string(encoded))
		t.Fatal(err)
	}
	var reviewed map[string]string
	if err := json.Unmarshal(data, &reviewed); err != nil {
		t.Fatal(err)
	}
	nonEgressData, err := os.ReadFile("testdata/url_sink_non_egress.json")
	if err != nil {
		t.Fatal(err)
	}
	var reviewedNonEgress map[string]string
	if err := json.Unmarshal(nonEgressData, &reviewedNonEgress); err != nil {
		t.Fatal(err)
	}
	classifiedNonEgress := 0
	for key, reason := range reviewedNonEgress {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("non-egress classification has no reason: %s", key)
		}
		if _, exists := actual[key]; exists {
			delete(actual, key)
			classifiedNonEgress++
		} else {
			t.Errorf("stale endpoint non-egress classification %s", key)
		}
		delete(reviewed, key)
	}
	for key := range actual {
		if reviewed[key] == "" {
			t.Errorf("unreviewed raw endpoint egress %s", key)
		}
	}
	for key := range reviewed {
		if _, ok := actual[key]; !ok {
			t.Errorf("stale endpoint inventory entry %s", key)
		}
	}
	if t.Failed() {
		encoded, _ := json.MarshalIndent(endpointInventoryKeys(actual), "", "  ")
		t.Log("URL_SINK_KEYS_JSON " + string(encoded))
	}
	fmt.Printf("ENDPOINT_INVENTORY functions=%d raw_egresses=%d string_helpers=%d classified_non_egress=%d\n", len(functions), len(actual), len(returns), classifiedNonEgress)
}
