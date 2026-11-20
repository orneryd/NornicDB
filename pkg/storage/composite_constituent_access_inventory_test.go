// NOR-312 acceptance criterion 4: the complete constituent-access call-site
// population must be reclassified by actual contract rather than assumed
// equivalent. The historical inventory reported 86 sites; a later previous
// search reported 62. Neither number proves the population, so this file
// derives it from source on every run and fails when a site is added, removed
// or changes how it handles the access error.
package storage

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// constituentAccessMethods are the selector names that resolve a constituent
// engine, including the write-mode wrapper and the exported alias resolver.
var constituentAccessMethods = map[string]bool{
	"getConstituent":         true,
	"getConstituentForWrite": true,
	"GetConstituentByAlias":  true,
}

// constituentAccessClass is how a call site handles the access error.
type constituentAccessClass string

const (
	// accessPropagates returns the error to the caller.
	accessPropagates constituentAccessClass = "propagate"
	// accessSkips drops the failed constituent and keeps going.
	accessSkips constituentAccessClass = "skip"
	// accessDiscards drops the error result or returns a different error.
	accessDiscards constituentAccessClass = "discard"
	// accessFallsThrough leaves this block without handling the access error.
	accessFallsThrough constituentAccessClass = "fallthrough"
	// accessConditional has different handling on different failure paths.
	accessConditional constituentAccessClass = "conditional"
	// accessUnclassified is a call site the extractor could not classify; it
	// must never appear in a recorded inventory.
	accessUnclassified constituentAccessClass = "unclassified"
)

// constituentAccessSite is one call site, keyed by file and enclosing function
// so the inventory survives line movement.
type constituentAccessSite struct {
	file, function string
	line           int
	class          constituentAccessClass
	handling       ast.Stmt
}

func (s constituentAccessSite) key() string { return s.file + "#" + s.function }

func constituentAccessSourceFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		names = append(names, filepath.Join(directory, entry.Name()))
	}
	sort.Strings(names)
	return names, nil
}

// constituentAccessCalls returns every access call site and the number of
// calls the AST contains, so an unclassified site cannot go unnoticed.
func constituentAccessCalls(t *testing.T, directory string, methods map[string]bool) ([]constituentAccessSite, int) {
	t.Helper()
	paths, err := constituentAccessSourceFiles(directory)
	if err != nil {
		t.Fatalf("read %s: %v", directory, err)
	}
	fset := token.NewFileSet()
	var sites []constituentAccessSite
	classified := 0
	total := 0
	for _, path := range paths {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		name := filepath.Base(path)
		ast.Inspect(file, func(node ast.Node) bool {
			decl, ok := node.(*ast.FuncDecl)
			if !ok || decl.Body == nil {
				return true
			}
			found, seen := scanConstituentAccessBlock(fset, decl.Body.List, name, decl.Name.Name, methods)
			sites = append(sites, found...)
			classified += seen
			return false
		})
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && methods[selector.Sel.Name] {
				total++
			}
			return true
		})
	}
	if total != classified {
		t.Fatalf("%s: %d constituent-access call sites in the AST, %d classified", directory, total, classified)
	}
	sort.SliceStable(sites, func(i, j int) bool {
		if sites[i].file != sites[j].file {
			return sites[i].file < sites[j].file
		}
		return sites[i].line < sites[j].line
	})
	return sites, total
}

func scanConstituentAccessBlock(fset *token.FileSet, statements []ast.Stmt, file, function string, methods map[string]bool) ([]constituentAccessSite, int) {
	var sites []constituentAccessSite
	total := 0
	record := func(node ast.Node, class constituentAccessClass) {
		total++
		sites = append(sites, constituentAccessSite{file: file, function: function, line: fset.Position(node.Pos()).Line, class: class})
	}
	for index, statement := range statements {
		switch current := statement.(type) {
		case *ast.AssignStmt:
			for _, value := range current.Rhs {
				if !isConstituentAccessCall(value, methods) {
					continue
				}
				class := accessUnclassified
				if len(current.Rhs) == 1 && len(current.Lhs) == 2 {
					if binding, ok := current.Lhs[1].(*ast.Ident); ok {
						if binding.Name == "_" {
							class = accessDiscards
						} else {
							class = classifyAccessFailure(statements[index+1:], binding)
						}
					}
				}
				record(current, class)
				if class == accessConditional {
					sites[len(sites)-1].handling = statements[index+1]
				}
			}
		case *ast.ReturnStmt:
			for _, result := range current.Results {
				if isConstituentAccessCall(result, methods) {
					record(current, accessPropagates)
				}
			}
		case *ast.IfStmt:
			found, seen := scanConstituentAccessBlock(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
			if block, ok := current.Else.(*ast.BlockStmt); ok {
				found, seen = scanConstituentAccessBlock(fset, block.List, file, function, methods)
				sites, total = append(sites, found...), total+seen
			}
		case *ast.ForStmt:
			found, seen := scanConstituentAccessBlock(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.RangeStmt:
			found, seen := scanConstituentAccessBlock(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.BlockStmt:
			found, seen := scanConstituentAccessBlock(fset, current.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.SwitchStmt:
			found, seen := scanConstituentAccessBlock(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.TypeSwitchStmt:
			found, seen := scanConstituentAccessBlock(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.CaseClause:
			found, seen := scanConstituentAccessBlock(fset, current.Body, file, function, methods)
			sites, total = append(sites, found...), total+seen
		}
	}
	return sites, total
}

func isConstituentAccessCall(expression ast.Expr, methods map[string]bool) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && methods[selector.Sel.Name]
}

// This is a deliberately bounded syntactic audit, not Go data-flow analysis.
// Interpret the local tail with the assigned access error known to be nonnil.
// Unsupported effects/control flow fail closed instead of becoming evidence
// of propagation. Object identity prevents a different or shadowed err binding
// from standing in for the access error.
func classifyAccessFailure(statements []ast.Stmt, binding *ast.Ident) constituentAccessClass {
	if len(statements) > 0 {
		if guard, ok := statements[0].(*ast.IfStmt); ok && !accessExpressionUses(guard.Cond, binding) {
			return accessUnclassified
		}
	}
	paths := accessFailurePaths(statements, binding)
	switch paths {
	case accessPathPropagate:
		return accessPropagates
	case accessPathSkip:
		return accessSkips
	case accessPathDiscard:
		return accessDiscards
	case accessPathFallthrough:
		return accessFallsThrough
	default:
		if paths&accessPathUnknown != 0 {
			return accessUnclassified
		}
		return accessConditional
	}
}

type accessFailurePath uint8

const (
	accessPathPropagate accessFailurePath = 1 << iota
	accessPathSkip
	accessPathDiscard
	accessPathFallthrough
	accessPathUnknown
)

func accessFailurePaths(statements []ast.Stmt, binding *ast.Ident) accessFailurePath {
	if len(statements) == 0 {
		return accessPathFallthrough
	}
	tail := statements[1:]
	switch current := statements[0].(type) {
	case *ast.ReturnStmt:
		if len(current.Results) == 0 {
			return accessPathUnknown // Named return values require data-flow analysis.
		}
		result := current.Results[len(current.Results)-1]
		if accessErrorIdentity(result, binding) {
			return accessPathPropagate
		}
		// localizedError's second argument is the wrapped cause. Merely
		// mentioning the error in a message or an arbitrary call is not proof.
		if call, ok := result.(*ast.CallExpr); ok {
			if function, ok := call.Fun.(*ast.Ident); ok && function.Obj == nil && function.Name == "localizedError" && len(call.Args) == 2 && accessErrorIdentity(call.Args[1], binding) {
				return accessPathPropagate
			}
		}
		if accessExpressionUses(result, binding) {
			return accessPathUnknown
		}
		return accessPathDiscard
	case *ast.BranchStmt:
		if current.Tok == token.CONTINUE && current.Label == nil {
			return accessPathSkip
		}
	case *ast.IfStmt:
		if current.Init != nil {
			return accessPathUnknown // May overwrite or shadow the access error.
		}
		truth := accessFailureCondition(current.Cond, binding)
		var paths accessFailurePath
		if truth&2 != 0 {
			branch := append(append([]ast.Stmt(nil), current.Body.List...), tail...)
			paths |= accessFailurePaths(branch, binding)
		}
		if truth&1 != 0 {
			branch := tail
			switch alternate := current.Else.(type) {
			case *ast.BlockStmt:
				branch = append(append([]ast.Stmt(nil), alternate.List...), tail...)
			case *ast.IfStmt:
				branch = append([]ast.Stmt{alternate}, tail...)
			case nil:
			default:
				return accessPathUnknown
			}
			paths |= accessFailurePaths(branch, binding)
		}
		if truth != 0 {
			return paths
		}
	case *ast.DeclStmt:
		// The head-read adapter declares an uninitialized zero value before
		// returning it beside err. Initializers and other effects are unknown.
		if declaration, ok := current.Decl.(*ast.GenDecl); ok && declaration.Tok == token.VAR {
			for _, spec := range declaration.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Values) != 0 {
					return accessPathUnknown
				}
			}
			return accessFailurePaths(tail, binding)
		}
	}
	return accessPathUnknown
}

// Truth is a bit set: 1=false, 2=true, 3=either, 0=unsupported use of the
// access error. Unknown policy predicates explore both paths; success-only
// guards cannot publish the error from their unreachable body.
func accessFailureCondition(expression ast.Expr, binding *ast.Ident) uint8 {
	switch current := expression.(type) {
	case *ast.ParenExpr:
		return accessFailureCondition(current.X, binding)
	case *ast.UnaryExpr:
		if current.Op == token.NOT {
			truth := accessFailureCondition(current.X, binding)
			return (truth&1)<<1 | (truth&2)>>1
		}
	case *ast.BinaryExpr:
		if current.Op == token.EQL || current.Op == token.NEQ {
			left, right := current.X, current.Y
			if accessErrorIdentity(right, binding) {
				left, right = right, left
			}
			if nilValue, ok := right.(*ast.Ident); ok && nilValue.Name == "nil" && nilValue.Obj == nil && accessErrorIdentity(left, binding) {
				if current.Op == token.NEQ {
					return 2
				}
				return 1
			}
		}
		if current.Op == token.LAND || current.Op == token.LOR {
			left, right := accessFailureCondition(current.X, binding), accessFailureCondition(current.Y, binding)
			if left == 0 || right == 0 {
				return 0
			}
			var truth uint8
			for a := uint8(1); a <= 2; a++ {
				for b := uint8(1); b <= 2; b++ {
					if left&a != 0 && right&b != 0 {
						value := a == 2 && b == 2
						if current.Op == token.LOR {
							value = a == 2 || b == 2
						}
						if value {
							truth |= 2
						} else {
							truth |= 1
						}
					}
				}
			}
			return truth
		}
	}
	if accessExpressionUses(expression, binding) {
		return 0
	}
	return 3
}

func accessErrorIdentity(expression ast.Expr, binding *ast.Ident) bool {
	if parenthesized, ok := expression.(*ast.ParenExpr); ok {
		return accessErrorIdentity(parenthesized.X, binding)
	}
	identifier, ok := expression.(*ast.Ident)
	return ok && binding.Obj != nil && identifier.Obj == binding.Obj
}

func accessExpressionUses(expression ast.Expr, binding *ast.Ident) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && accessErrorIdentity(identifier, binding) {
			found = true
		}
		return !found
	})
	return found
}

// recordedStorageAccessInventory is the audited classification of
// pkg/storage. Key is "<file>#<function>"; the value lists the classes of that
// function's call sites in source order. "skip" drops the failed constituent
// (the NOR-312 defect where the method already returns an error, and a
// contract decision where it cannot), "propagate" reports it.
var recordedStorageAccessInventory = map[string][]constituentAccessClass{
	"composite_capabilities.go#StreamNodesWithOptions":   {accessPropagates},
	"composite_capabilities.go#ForEachNodeIDByLabel":     {accessPropagates},
	"composite_capabilities.go#IterateNodes":             {accessPropagates},
	"composite_capabilities.go#walkReadableConstituents": {accessConditional},
	"composite_capabilities.go#GetSchemaForNamespace":    {accessSkips},

	"composite_engine.go#getConstituentForWrite":      {accessPropagates},
	"composite_engine.go#GetConstituentByAlias":       {accessPropagates},
	"composite_engine.go#CreateNode":                  {accessPropagates},
	"composite_engine.go#UpdateNode":                  {accessPropagates},
	"composite_engine.go#CreateEdge":                  {accessPropagates, accessFallsThrough, accessFallsThrough, accessPropagates, accessPropagates, accessPropagates},
	"composite_engine.go#UpdateEdge":                  {accessPropagates},
	"composite_engine.go#materializeConstituents":     {accessPropagates},
	"composite_engine.go#streamConstituents":          {accessPropagates},
	"composite_engine.go#StreamNodesByLabelProjected": {accessPropagates},
	"composite_engine.go#GetFirstNodeByLabel":         {accessPropagates},
	"composite_engine.go#GetEdgeBetween":              {accessPropagates},
	"composite_engine.go#sumConstituentDegree":        {accessPropagates},
	"composite_engine.go#GetSchema":                   {accessSkips},
	"composite_engine.go#BulkCreateNodes":             {accessPropagates},
	"composite_engine.go#BulkCreateEdges":             {accessPropagates, accessPropagates, accessPropagates},
	"composite_engine.go#BatchGetNodes":               {accessPropagates},
	"composite_engine.go#sumConstituentCounts":        {accessPropagates},
	"composite_engine.go#StreamNodeChunks":            {accessPropagates},

	"composite_entity_lookup.go#constituentsForEntity": {accessConditional},
	"composite_entity_lookup.go#ConstituentStorage":    {accessPropagates},
	"composite_entity_lookup.go#readFirstConstituent":  {accessPropagates},
	"composite_entity_lookup.go#writeFirstConstituent": {accessPropagates, accessPropagates},

	"edge_type_stream.go#StreamEdgesByType":         {accessPropagates},
	"edges_between_stream.go#MatchEdgesBetween":     {accessPropagates},
	"embedding_pending.go#CountPendingEmbeddingIDs": {accessSkips},
}

// recordedCypherAccessInventory is the classification of the pkg/cypher
// consumer that resolves USE composite.constituent.
var recordedCypherAccessInventory = map[string][]constituentAccessClass{
	"executor_use.go#resolveCompositeConstituent": {accessPropagates},
}

// Pin both conditional guards so an inverted policy cannot retain the same
// class unnoticed. These snapshots prompt review; runtime tests establish the
// public contracts.
var recordedConditionalAccessPolicies = map[string]string{
	"composite_capabilities.go#walkReadableConstituents": `if err != nil {
		if access == constituentAccessRequired { return err }
		continue
	}`,
	"composite_entity_lookup.go#constituentsForEntity": `if err != nil {
		if c.configuredConstituentDatabase(alias) == database { return nil, nil, local, true, err }
		continue
	}`,
}

func TestCompositeConstituentAccessCallSiteInventory(t *testing.T) {
	for _, scope := range []struct {
		name      string
		directory string
		recorded  map[string][]constituentAccessClass
	}{
		{"storage", ".", recordedStorageAccessInventory},
		{"cypher", filepath.Join("..", "cypher"), recordedCypherAccessInventory},
	} {
		t.Run(scope.name, func(t *testing.T) {
			sites, total := constituentAccessCalls(t, scope.directory, constituentAccessMethods)
			actual := map[string][]constituentAccessClass{}
			for _, site := range sites {
				actual[site.key()] = append(actual[site.key()], site.class)
				t.Logf("%s:%d %s %s", site.file, site.line, site.function, site.class)
				if site.class == accessConditional {
					var source bytes.Buffer
					if err := format.Node(&source, token.NewFileSet(), site.handling); err != nil {
						t.Fatal(err)
					}
					want, recorded := recordedConditionalAccessPolicies[site.key()]
					if !recorded || strings.Join(strings.Fields(source.String()), " ") != strings.Join(strings.Fields(want), " ") {
						t.Fatalf("%s: conditional access policy requires review:\n%s", site.key(), source.String())
					}
				}
			}
			classes := map[constituentAccessClass]int{}
			for _, site := range sites {
				classes[site.class]++
			}
			t.Logf("total=%d propagate=%d skip=%d discard=%d fallthrough=%d conditional=%d unclassified=%d",
				total, classes[accessPropagates], classes[accessSkips], classes[accessDiscards], classes[accessFallsThrough], classes[accessConditional], classes[accessUnclassified])
			if classes[accessUnclassified] != 0 {
				t.Fatalf("%d call sites could not be classified", classes[accessUnclassified])
			}
			keys := make([]string, 0, len(actual))
			for key := range actual {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				want, recorded := scope.recorded[key]
				if !recorded {
					t.Fatalf("%s: unrecorded call site %v", key, actual[key])
				}
				if len(want) != len(actual[key]) {
					t.Fatalf("%s: recorded %v, found %v", key, want, actual[key])
				}
				for i := range want {
					if want[i] != actual[key][i] {
						t.Fatalf("%s: recorded %v, found %v", key, want, actual[key])
					}
				}
			}
			recorded := 0
			for key, classes := range scope.recorded {
				recorded += len(classes)
				if _, found := actual[key]; !found {
					t.Fatalf("%s: recorded call sites are gone", key)
				}
			}
			if recorded != total {
				t.Fatalf("recorded %d call sites, extractor found %d", recorded, total)
			}
		})
	}
}
