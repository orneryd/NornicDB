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

// constituentAccessMethods_r9port are the selector names that resolve a constituent
// engine, including the write-mode wrapper and the exported alias resolver.
var constituentAccessMethods_r9port = map[string]bool{
	"getConstituent":         true,
	"getConstituentForWrite": true,
	"GetConstituentByAlias":  true,
}

// constituentAccessClass_r9port is how a call site handles the access error.
type constituentAccessClass_r9port string

const (
	// accessPropagates_r9port returns the error to the caller.
	accessPropagates_r9port constituentAccessClass_r9port = "propagate"
	// accessSkips_r9port drops the failed constituent and keeps going.
	accessSkips_r9port constituentAccessClass_r9port = "skip"
	// accessDiscards_r9port drops the error result entirely.
	accessDiscards_r9port constituentAccessClass_r9port = "discard"
	// accessUnclassified_r9port is a call site the extractor could not classify; it
	// must never appear in a recorded inventory.
	accessUnclassified_r9port constituentAccessClass_r9port = "unclassified"
)

// constituentAccessSite_r9port is one call site, keyed by file and enclosing function
// so the inventory survives line movement.
type constituentAccessSite_r9port struct {
	file, function string
	line           int
	class          constituentAccessClass_r9port
}

func (s constituentAccessSite_r9port) key() string { return s.file + "#" + s.function }

func constituentAccessSourceFiles_r9port(directory string) ([]string, error) {
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

// constituentAccessCalls_r9port returns every access call site and the number of
// calls the AST contains, so an unclassified site cannot go unnoticed.
func constituentAccessCalls_r9port(t *testing.T, directory string, methods map[string]bool) ([]constituentAccessSite_r9port, int) {
	t.Helper()
	paths, err := constituentAccessSourceFiles_r9port(directory)
	if err != nil {
		t.Fatalf("read %s: %v", directory, err)
	}
	fset := token.NewFileSet()
	var sites []constituentAccessSite_r9port
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
			found, seen := scanConstituentAccessBlock_r9port(fset, decl.Body.List, name, decl.Name.Name, methods)
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

func scanConstituentAccessBlock_r9port(fset *token.FileSet, statements []ast.Stmt, file, function string, methods map[string]bool) ([]constituentAccessSite_r9port, int) {
	var sites []constituentAccessSite_r9port
	total := 0
	record := func(node ast.Node, class constituentAccessClass_r9port) {
		total++
		sites = append(sites, constituentAccessSite_r9port{file: file, function: function, line: fset.Position(node.Pos()).Line, class: class})
	}
	for index, statement := range statements {
		switch current := statement.(type) {
		case *ast.AssignStmt:
			for position, value := range current.Rhs {
				if !isConstituentAccessCall_r9port(value, methods) || len(current.Lhs) <= position {
					continue
				}
				class := accessUnclassified_r9port
				switch {
				case discardsError(current):
					class = accessDiscards_r9port
				case index+1 < len(statements):
					class = classFromNextStatement(statements[index+1])
				}
				record(current, class)
			}
		case *ast.ReturnStmt:
			for _, result := range current.Results {
				if isConstituentAccessCall_r9port(result, methods) {
					record(current, accessPropagates_r9port)
				}
			}
		case *ast.IfStmt:
			found, seen := scanConstituentAccessBlock_r9port(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
			if block, ok := current.Else.(*ast.BlockStmt); ok {
				found, seen = scanConstituentAccessBlock_r9port(fset, block.List, file, function, methods)
				sites, total = append(sites, found...), total+seen
			}
		case *ast.ForStmt:
			found, seen := scanConstituentAccessBlock_r9port(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.RangeStmt:
			found, seen := scanConstituentAccessBlock_r9port(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.BlockStmt:
			found, seen := scanConstituentAccessBlock_r9port(fset, current.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.SwitchStmt:
			found, seen := scanConstituentAccessBlock_r9port(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.TypeSwitchStmt:
			found, seen := scanConstituentAccessBlock_r9port(fset, current.Body.List, file, function, methods)
			sites, total = append(sites, found...), total+seen
		case *ast.CaseClause:
			found, seen := scanConstituentAccessBlock_r9port(fset, current.Body, file, function, methods)
			sites, total = append(sites, found...), total+seen
		}
	}
	return sites, total
}

func isConstituentAccessCall_r9port(expression ast.Expr, methods map[string]bool) bool {
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
func classFromNextStatement(statement ast.Stmt) constituentAccessClass_r9port {
	examine, ok := statement.(*ast.IfStmt)
	if !ok || !mentionsErr(examine.Cond) {
		return accessUnclassified_r9port
	}
	return blockClass(examine.Body.List)
}

func blockClass(statements []ast.Stmt) constituentAccessClass_r9port {
	for _, statement := range statements {
		switch current := statement.(type) {
		case *ast.BranchStmt:
			if current.Tok == token.CONTINUE {
				return accessSkips_r9port
			}
		case *ast.ReturnStmt:
			return accessPropagates_r9port
		case *ast.IfStmt:
			if class := blockClass(current.Body.List); class != accessUnclassified_r9port {
				return class
			}
		}
	}
	return accessUnclassified_r9port
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

// recordedStorageAccessInventory_r9port is the audited classification of
// pkg/storage. Key is "<file>#<function>"; the value lists the classes of that
// function's call sites in source order. "skip" drops the failed constituent
// (the NOR-312 defect where the method already returns an error, and a
// contract decision where it cannot), "propagate" reports it.
var recordedStorageAccessInventory_r9port = map[string][]constituentAccessClass_r9port{
	"composite_capabilities.go#StreamNodesWithOptions":        {accessPropagates_r9port},
	"composite_capabilities.go#GetNodeLatestEffective":        {accessPropagates_r9port},
	"composite_capabilities.go#GetEdgeLatestEffective":        {accessPropagates_r9port},
	"composite_capabilities.go#GetNodeLatestVisible":          {accessPropagates_r9port},
	"composite_capabilities.go#GetNodeVisibleAt":              {accessPropagates_r9port},
	"composite_capabilities.go#GetEdgeLatestVisible":          {accessPropagates_r9port},
	"composite_capabilities.go#GetEdgeVisibleAt":              {accessPropagates_r9port},
	"composite_capabilities.go#GetNodeCurrentHead":            {accessPropagates_r9port},
	"composite_capabilities.go#GetEdgeCurrentHead":            {accessPropagates_r9port},
	"composite_capabilities.go#ForEachNodeIDByLabel":          {accessPropagates_r9port},
	"composite_capabilities.go#GetNodeProjected":              {accessPropagates_r9port},
	"composite_capabilities.go#IterateNodes":                  {accessPropagates_r9port},
	"composite_capabilities.go#UpdateNodeEmbeddingSidecar":    {accessPropagates_r9port},
	"composite_capabilities.go#StreamParkedEmbeddingFailures": {accessPropagates_r9port},
	"composite_capabilities.go#PendingEmbeddingsCount":        {accessSkips_r9port},
	"composite_capabilities.go#ListNamespaces":                {accessSkips_r9port},
	"composite_capabilities.go#GetSchemaForNamespace":         {accessSkips_r9port},
	"composite_capabilities.go#GraphMutationVersion":          {accessSkips_r9port},
	"composite_capabilities.go#ConsumeCleanShutdownMarker":    {accessPropagates_r9port},
	"composite_capabilities.go#MarkCleanShutdown":             {accessPropagates_r9port},
	"composite_capabilities.go#PruneMVCCVersions":             {accessPropagates_r9port},
	"composite_capabilities.go#RebuildMVCCHeads":              {accessPropagates_r9port},
	"composite_capabilities.go#PruneTemporalHistory":          {accessPropagates_r9port},
	"composite_capabilities.go#RebuildTemporalIndexes":        {accessPropagates_r9port},
	"composite_capabilities.go#RegisterSnapshotReader":        {accessSkips_r9port},
	"composite_capabilities.go#LifecycleStatus":               {accessSkips_r9port},
	"composite_capabilities.go#TriggerPruneNow":               {accessPropagates_r9port},
	"composite_capabilities.go#PauseLifecycle":                {accessSkips_r9port},
	"composite_capabilities.go#ResumeLifecycle":               {accessSkips_r9port},
	"composite_capabilities.go#SetLifecycleSchedule":          {accessPropagates_r9port},
	"composite_capabilities.go#eventTargets":                  {accessSkips_r9port},

	"composite_engine.go#getConstituentForWrite":         {accessPropagates_r9port},
	"composite_engine.go#GetConstituentByAlias":          {accessPropagates_r9port},
	"composite_engine.go#CreateNode":                     {accessPropagates_r9port},
	"composite_engine.go#GetNode":                        {accessPropagates_r9port},
	"composite_engine.go#UpdateNode":                     {accessPropagates_r9port, accessPropagates_r9port, accessPropagates_r9port},
	"composite_engine.go#DeleteNode":                     {accessPropagates_r9port, accessPropagates_r9port},
	"composite_engine.go#CreateEdge":                     {accessPropagates_r9port, accessPropagates_r9port, accessPropagates_r9port, accessPropagates_r9port, accessPropagates_r9port, accessPropagates_r9port},
	"composite_engine.go#GetEdge":                        {accessPropagates_r9port},
	"composite_engine.go#UpdateEdge":                     {accessPropagates_r9port, accessPropagates_r9port, accessPropagates_r9port},
	"composite_engine.go#DeleteEdge":                     {accessPropagates_r9port, accessPropagates_r9port},
	"composite_engine.go#materializeConstituents":        {accessPropagates_r9port},
	"composite_engine.go#streamConstituents":             {accessPropagates_r9port},
	"composite_engine.go#StreamNodesByLabelProjected":    {accessPropagates_r9port},
	"composite_engine.go#GetFirstNodeByLabel":            {accessPropagates_r9port},
	"composite_engine.go#GetEdgeBetween":                 {accessPropagates_r9port},
	"composite_engine.go#GetInDegree":                    {accessPropagates_r9port},
	"composite_engine.go#GetOutDegree":                   {accessPropagates_r9port},
	"composite_engine.go#GetSchema":                      {accessSkips_r9port},
	"composite_engine.go#BulkCreateNodes":                {accessPropagates_r9port},
	"composite_engine.go#BulkCreateEdges":                {accessPropagates_r9port, accessPropagates_r9port, accessPropagates_r9port},
	"composite_engine.go#BulkDeleteNodes":                {accessPropagates_r9port},
	"composite_engine.go#BulkDeleteEdges":                {accessPropagates_r9port},
	"composite_engine.go#BatchGetNodes":                  {accessPropagates_r9port},
	"composite_engine.go#NodeCount":                      {accessPropagates_r9port},
	"composite_engine.go#EdgeCount":                      {accessPropagates_r9port},
	"composite_engine.go#EdgeCountByType":                {accessPropagates_r9port},
	"composite_engine.go#sumConstituentPositionalCounts": {accessPropagates_r9port},
	"composite_engine.go#StreamNodeChunks":               {accessPropagates_r9port},

	"composite_entity_lookup.go#constituentForElementID": {accessPropagates_r9port},
	"composite_entity_lookup.go#constituentsForEntity":   {accessPropagates_r9port},
	"composite_entity_lookup.go#ConstituentStorage":      {accessPropagates_r9port},

	"edge_type_stream.go#StreamEdgesByType":         {accessPropagates_r9port},
	"edges_between_stream.go#MatchEdgesBetween":     {accessPropagates_r9port},
	"embedding_pending.go#CountPendingEmbeddingIDs": {accessSkips_r9port},
}

// recordedCypherAccessInventory_r9port is the classification of the pkg/cypher
// consumer that resolves USE composite.constituent.
var recordedCypherAccessInventory_r9port = map[string][]constituentAccessClass_r9port{
	"executor_use.go#resolveCompositeConstituent": {accessPropagates_r9port},
}

func TestCompositeConstituentAccessCallSiteInventory_r9port(t *testing.T) {
	for _, scope := range []struct {
		name      string
		directory string
		recorded  map[string][]constituentAccessClass_r9port
	}{
		{"storage", ".", recordedStorageAccessInventory_r9port},
		{"cypher", filepath.Join("..", "cypher"), recordedCypherAccessInventory_r9port},
	} {
		t.Run(scope.name, func(t *testing.T) {
			sites, total := constituentAccessCalls_r9port(t, scope.directory, constituentAccessMethods_r9port)
			actual := map[string][]constituentAccessClass_r9port{}
			for _, site := range sites {
				actual[site.key()] = append(actual[site.key()], site.class)
				t.Logf("%s:%d %s %s", site.file, site.line, site.function, site.class)
			}
			classes := map[constituentAccessClass_r9port]int{}
			for _, site := range sites {
				classes[site.class]++
			}
			t.Logf("total=%d propagate=%d skip=%d discard=%d unclassified=%d",
				total, classes[accessPropagates_r9port], classes[accessSkips_r9port], classes[accessDiscards_r9port], classes[accessUnclassified_r9port])
			if classes[accessUnclassified_r9port] != 0 {
				t.Fatalf("%d call sites could not be classified", classes[accessUnclassified_r9port])
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
