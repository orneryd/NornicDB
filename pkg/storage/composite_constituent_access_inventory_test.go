// NOR-312 acceptance criterion 4: the complete constituent-access call-site
// population must be reclassified by actual contract rather than assumed
// equivalent. The historical inventory reported 86 sites; a later previous
// search reported 62. Neither number proves the population, so this file
// derives it from source on every run and fails when a site is added, removed
// or changes how it handles the access error.
package storage

import (
	"go/ast"
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
	// accessDiscards drops the error result entirely.
	accessDiscards constituentAccessClass = "discard"
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
			for position, value := range current.Rhs {
				if !isConstituentAccessCall(value, methods) || len(current.Lhs) <= position {
					continue
				}
				class := accessUnclassified
				switch {
				case discardsError(current):
					class = accessDiscards
				case index+1 < len(statements):
					class = classFromNextStatement(statements[index+1])
				}
				record(current, class)
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

// discardsError reports whether the assignment discards the call's error
// result (the second result of the access call).
func discardsError(assignment *ast.AssignStmt) bool {
	if len(assignment.Lhs) < 2 {
		return false
	}
	identifier, ok := assignment.Lhs[1].(*ast.Ident)
	return ok && identifier.Name == "_"
}

// classFromNextStatement classifies the access error by the statement that
// consumes it: continue keeps the partial result, return reports the failure.
func classFromNextStatement(statement ast.Stmt) constituentAccessClass {
	examine, ok := statement.(*ast.IfStmt)
	if !ok || !mentionsErr(examine.Cond) {
		return accessUnclassified
	}
	return blockClass(examine.Body.List)
}

func blockClass(statements []ast.Stmt) constituentAccessClass {
	for _, statement := range statements {
		switch current := statement.(type) {
		case *ast.BranchStmt:
			if current.Tok == token.CONTINUE {
				return accessSkips
			}
		case *ast.ReturnStmt:
			return accessPropagates
		case *ast.IfStmt:
			if class := blockClass(current.Body.List); class != accessUnclassified {
				return class
			}
		}
	}
	return accessUnclassified
}

func mentionsErr(expression ast.Expr) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && identifier.Name == "err" {
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
	"composite_capabilities.go#walkReadableConstituents": {accessPropagates},
	"composite_capabilities.go#GetSchemaForNamespace":    {accessSkips},

	"composite_engine.go#getConstituentForWrite":      {accessPropagates},
	"composite_engine.go#GetConstituentByAlias":       {accessPropagates},
	"composite_engine.go#CreateNode":                  {accessPropagates},
	"composite_engine.go#UpdateNode":                  {accessPropagates},
	"composite_engine.go#CreateEdge":                  {accessPropagates, accessPropagates, accessPropagates, accessPropagates, accessPropagates, accessPropagates},
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

	"composite_entity_lookup.go#constituentsForEntity": {accessPropagates},
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
			}
			classes := map[constituentAccessClass]int{}
			for _, site := range sites {
				classes[site.class]++
			}
			t.Logf("total=%d propagate=%d skip=%d discard=%d unclassified=%d",
				total, classes[accessPropagates], classes[accessSkips], classes[accessDiscards], classes[accessUnclassified])
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
