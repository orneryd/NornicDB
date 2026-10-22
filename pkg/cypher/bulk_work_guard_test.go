package cypher

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Query-sized data is what a query passes or builds: a population (rows,
// entities, IDs, paths, bindings, or structs holding them), or a Cypher list
// or map value. Its size is the data's, not the query text's. Every sort of
// anything, and every copy, clone or nested pass over query-sized data, goes
// through the bulk owner (package bulk, population_bulk.go) or observes the
// statement within the work: a standard-library sort or copy has no loop at
// its call site for a check to reach, and a nested pass multiplies the work
// between two checks of its outer loop.
//
// bulkWorkExceptions lists the remaining calls and nested loops, keyed by
// file, enclosing function and call (or "nested <inner loop>"), each bounded
// by the query text: the clauses, expressions, labels, properties, variables
// or columns the statement writes.
var bulkWorkExceptions = map[string]string{
	"gds_projection.go:readGDSConfig:sort.Strings":                                                    "the configuration keys the call writes",
	"pipeline_projection.go:pipelineRowColumns:sort.Strings":                                          "the variables in scope",
	"subquery_value_expression.go:boundTraversalCount:nested inner.nodes":                             "the subquery pattern's node variables",
	"subquery_value_expression.go:boundTraversalCount:nested inner.rels":                              "the subquery pattern's relationship variables",
	"subquery_value_expression.go:boundTraversalCount:nested values":                                  "the variables the subquery imports",
	"match_index_seek.go:indexCandidateLabels:sort.Strings":                                           "the labels the schema's indexes on one property cover: the server's catalog",
	"show_schema_values.go:schemaConfigLiteral:sort.Strings":                                          "one index's configuration keys: the server's catalog",
	"executor_use.go:CompositeGraphs:sort.Strings":                                                    "the composite database's constituents: the server's catalog",
	"procedure_registry.go:List:sort.Slice":                                                           "the registered procedures: the server's catalog",
	"procedure_registry.go:ListBuiltIns:sort.Slice":                                                   "the built-in procedures: a fixed catalog the build declares",
	"call_apoc_path.go:configure:nested nodes":                                                        "once per node-list option the call's configuration writes",
	"call_cdc.go:keyMatches:append":                                                                   "one change event's key-constraint maps: one per key constraint of its entity",
	"result_stream.go:take:append":                                                                    "one page: at most the rows the client fetches",
	"call.go:sortedStringSet:sort.Strings":                                                            "a property's value types: Cypher's type names, a fixed set",
	"functions_neo5.go:sortValueTypes:sort.SliceStable":                                               "Cypher's value types, a fixed set",
	"executor_show.go:executeShowSettings:sort.Slice":                                                 "the server's setting definitions: a fixed catalog the build declares",
	"traversal.go:filterPathsByWhere:nested extra":                                                    "the variables the row binds",
	"remove_clause.go:pipelineApplyRemove:nested touched":                                             "the entities the REMOVE items name in one row",
	"executor_show.go:applyShowTail:append":                                                           "one result row's columns: the SHOW's columns and its YIELD sources",
	"aggregation_projection_scope.go:aggregatingProjectionScopeError:sort.Slice":                      "the expressions the clause writes",
	"label_expression_rewrite.go:applyLabelRewriteEdits:sort.SliceStable":                             "the rewrite's edits to the statement text",
	"path_selector.go:rewrittenExpression:sort.SliceStable":                                           "the rewrite's edits to the statement text",
	"map_pattern_context.go:scope:sort.Slice":                                                         "the statement's clauses",
	"explain.go:analyzeQuery:sort.SliceStable":                                                        "the statement's clauses",
	"pipeline_executor.go:pipelineLabelScanProjection:sort.Strings":                                   "the properties the statement reads",
	"pipeline_executor.go:pipelineNodePredicateProperties:sort.Strings":                               "the properties the statement reads",
	"pipeline_pattern_template.go:pipelinePropertiesKey:sort.Strings":                                 "the property keys the pattern writes",
	"pipeline_projection.go:pipelineWildcardColumns:sort.Strings":                                     "the variables in scope",
	"pipeline_projection.go:pipelineScopeColumns:sort.Strings":                                        "the variables in scope",
	"plan_hash.go:canonicalizePlan:sort.Strings":                                                      "the plan's keys",
	"merge.go:mergePropertyNamesSorted:sort.Strings":                                                  "the property keys the MERGE pattern writes",
	"relationship_merge_identity_cache.go:sortedIdentityKeys:sort.Strings":                            "the property keys the MERGE pattern writes",
	"unwind_relationship_merge_batch.go:relationshipMergeKeyProps:sort.Slice":                         "the property keys the MERGE pattern writes",
	"clauses.go:unwindMergeLabelsKey:sort.Strings":                                                    "the labels the statement writes",
	"executor_match_vector_cosine_fastpath.go:nodeProjectionPropertiesForVectorFastPath:sort.Strings": "the properties the statement projects",
	"executor_match_vector_cosine_fastpath.go:appendProjectedProperty:sort.Strings":                   "the properties the statement projects",
	"pipeline_call_operator.go:callPipelineResultFromRows:sort.Strings":                               "the variables the subquery binds",
	"quoted_variables.go:canonicalizeQuotedVariables:sort.Slice":                                      "the quoted variables the statement writes",
	"quoted_variables.go:sortColumnsByName:sort.SliceStable":                                          "the result's columns",
	"set_semantic_validation.go:expressionFreeVariables:sort.Strings":                                 "the variables the expression writes",
	"static_function_literal_arguments.go:checkStaticLiteralArguments:sort.Strings":                   "the literal arguments the call writes",
	"subquery_value_expression.go:executeCorrelatedSubqueryBody:sort.Strings":                         "the variables the subquery imports",
	"traversal.go:expandTraversalWildcardReturnItems:sort.Strings":                                    "the variables the pattern binds",
	"schema_property_types.go:parsePropertyType:sort.Slice":                                           "the type expression the statement writes",
	"database_options.go:applyDatabaseOptions:sort.Strings":                                           "the OPTIONS the statement writes",
	"call_cdc.go:parseChangeSelector:sort.Strings":                                                    "the selector the call writes",
	"gds_projection.go:readGDSPropertySpecs:sort.Strings":                                             "the properties the call names",
	"pipeline_executor.go:pipelineApplyChainedMatch:nested joinedRelationships[index]":                "the pattern's relationship variables",
	"pipeline_mutation.go:pipelineApplyMerge:nested nodeContext":                                      "the MERGE pattern's node variables",
	"pipeline_mutation.go:pipelineApplyMerge:nested relContext":                                       "the MERGE pattern's relationship variables",
	"pipeline_mutation.go:mergeRelationshipRows:nested nodeContext":                                   "the MERGE pattern's node variables",
	"pipeline_mutation.go:mergeRelationshipRows:nested relContext":                                    "the MERGE pattern's relationship variables",
	"pipeline_aggregate_stream.go:tryCartesianAggregatePartitions:nested patterns":                    "the patterns the statement writes",
	"executor_mutations.go:normalizeSetMatchRowsToNodes:nested row":                                   "one result row's columns: the statement's RETURN items",
	"executor_mutations.go:normalizeSetMatchRowsToEdges:nested row":                                   "one result row's columns: the statement's RETURN items",
}

// fnBulkWorkExceptions is bulkWorkExceptions for the function package.
var fnBulkWorkExceptions = map[string]string{}

func TestPopulationBulkCallSites(t *testing.T) {
	pkg := loadCypherPackageForGuard(t, map[string]string{"bulk_work_guard_fixture.go": bulkWorkGuardFixture})
	violations, used := bulkWorkViolations(pkg, pkg.files, bulkWorkExceptions)
	require.Empty(t, violations, "sorts, and copies or nested passes over query-sized data, must go through the bulk owner")
	require.Empty(t, staleGuardExceptions(bulkWorkExceptions, used), "exceptions that no longer match a call or loop")

	fixtureViolations, _ := bulkWorkViolations(pkg, pkg.fixtures, bulkWorkExceptions)
	require.Equal(t, []string{
		"bulk_work_guard_fixture.go:15 collSortFixture: sort.SliceStable outside the bulk owner ([]interface{})",
		"bulk_work_guard_fixture.go:21 sortNodesFixture: nested pass over query-sized data that does not observe the statement (values)",
		"bulk_work_guard_fixture.go:27 populationFixture: sort.Slice outside the bulk owner ([]*storage.Node)",
		"bulk_work_guard_fixture.go:28 populationFixture: sort.Strings outside the bulk owner ([]string)",
		"bulk_work_guard_fixture.go:29 populationFixture: append of query-sized data ([]*storage.Node)",
		"bulk_work_guard_fixture.go:30 populationFixture: slices.Clone of query-sized data ([]pipelineRow)",
		"bulk_work_guard_fixture.go:31 populationFixture: maps.Clone of query-sized data (map[storage.NodeID]*storage.Node)",
		"bulk_work_guard_fixture.go:34 populationFixture: nested pass over query-sized data that does not observe the statement (nodes)",
		// A copy in a pass is flagged; a copy on the pass's first change is one copy.
		"bulk_work_guard_fixture.go:62 populationFixture: append of query-sized data ([]interface{})",
	}, fixtureViolations)
}

// The function package's implementations get the statement through
// Context.Statement and use package bulk directly.
func TestFunctionPackageBulkCallSites(t *testing.T) {
	pkg := loadPackageForGuard(t, "./fn", cypherPackagePath+"/fn", nil)
	violations, used := bulkWorkViolations(pkg, pkg.files, fnBulkWorkExceptions)
	require.Empty(t, violations, "function implementations must sort, copy and nest query-sized passes through the bulk owner")
	require.Empty(t, staleGuardExceptions(fnBulkWorkExceptions, used), "exceptions that no longer match a call or loop")
}

// The fixture's first two functions are the shapes the round-7 review found
// in coll.sort and apoc.coll.sortNodes.
const bulkWorkGuardFixture = `package cypher

import (
	"context"
	"maps"
	"slices"
	"sort"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func collSortFixture(list []interface{}) []interface{} {
	out := make([]interface{}, len(list))
	copy(out, list)
	sort.SliceStable(out, func(i, j int) bool { return false })
	return out
}

func sortNodesFixture(values []interface{}) {
	for i := 0; i < len(values)-1; i++ {
		for j := i + 1; j < len(values); j++ {
		}
	}
}

func populationFixture(ctx context.Context, nodes []*storage.Node, rows []pipelineRow, names []string, byID map[storage.NodeID]*storage.Node, value map[string]interface{}) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	sort.Strings(names)
	_ = append(nodes, nodes...)
	_ = slices.Clone(rows)
	_ = maps.Clone(byID)
	_ = maps.Clone(value)
	for _, outer := range nodes {
		for _, inner := range nodes {
			_, _ = outer, inner
		}
	}
	_ = append(names, names...)
	_ = maps.Clone(rows[0])
	for _, row := range rows {
		for name := range row {
			_ = name
		}
	}
	for batch, err := range materializedBatches(ctx, len(nodes)) {
		if err != nil {
			return
		}
		for _, node := range nodes[batch.start:batch.end] {
			checkpoint := materializedRowCheckpoint{ctx: ctx}
			for _, other := range nodes {
				if checkpoint.due() && checkpoint.canceled() {
					return
				}
				_, _ = node, other
			}
		}
	}
	_ = sortPopulation(ctx, nodes, func(i, j int) bool { return false })
	list, _ := value["items"].([]interface{})
	for range list {
		_ = append([]interface{}(nil), list...)
	}
	var once []interface{}
	for i := range list {
		if once == nil {
			once = append([]interface{}(nil), list...)
		}
		once[i] = nil
	}
}
`

func bulkWorkViolations(pkg *guardPackage, files []*ast.File, exceptions map[string]string) ([]string, map[string]bool) {
	var violations []string
	used := map[string]bool{}
	report := func(file, function, kind string, at ast.Node, message string) {
		key := file + ":" + function + ":" + kind
		if _, excepted := exceptions[key]; excepted {
			used[key] = true
			return
		}
		violations = append(violations, fmt.Sprintf("%s:%d %s: %s", file, pkg.fset.Position(at.Pos()).Line, function, message))
	}
	for _, file := range files {
		name := filepath.Base(pkg.fset.Position(file.Pos()).Filename)
		if name == "population_bulk.go" {
			continue
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			walker := bulkWorkWalker{pkg: pkg, report: func(kind string, at ast.Node, message string) {
				report(name, function.Name.Name, kind, at, message)
			}}
			walker.walk(function.Body, nil)
		}
	}
	sort.Strings(violations)
	return violations, used
}

type bulkWorkWalker struct {
	pkg    *guardPackage
	report func(kind string, at ast.Node, message string)
}

// bulkWorkLoop is a loop's place in the nesting: whether it passes over
// query-sized data, or is an owner batch (materializedBatches, or the
// batch's slice of the population), and the variables it binds.
type bulkWorkLoop struct {
	node         ast.Node
	sized, batch bool
	binds        []types.Object
}

func (w bulkWorkWalker) walk(node ast.Node, loops []bulkWorkLoop) {
	ast.Inspect(node, func(n ast.Node) bool {
		if n == node {
			return true
		}
		switch x := n.(type) {
		case *ast.CallExpr:
			w.call(x, loops)
		case *ast.AssignStmt:
			w.alias(x, loops)
		case *ast.TypeSwitchStmt:
			w.switchAlias(x, loops)
		case *ast.IfStmt:
			target := w.firstChangeTarget(x)
			if target == nil {
				return true
			}
			for _, part := range []ast.Stmt{x.Init, x.Else} {
				if part != nil {
					w.walk(&ast.BlockStmt{List: []ast.Stmt{part}}, loops)
				}
			}
			w.walk(x.Body, loopsAround(target.Pos(), loops))
			return false
		case *ast.RangeStmt:
			loop := w.rangeLoop(x)
			loop.node = x
			for _, bound := range []ast.Expr{x.Key, x.Value} {
				if id, ok := bound.(*ast.Ident); ok && w.pkg.info.Defs[id] != nil {
					loop.binds = append(loop.binds, w.pkg.info.Defs[id])
				}
			}
			w.nested(x, x.X, loop, loops)
			if x.X != nil {
				w.walk(x.X, loops)
			}
			w.walk(x.Body, append(append([]bulkWorkLoop{}, loops...), loop))
			return false
		case *ast.ForStmt:
			loop := bulkWorkLoop{node: x, sized: x.Cond != nil && w.boundedByQuerySized(x.Cond)}
			if assign, ok := x.Init.(*ast.AssignStmt); ok {
				for _, lhs := range assign.Lhs {
					if id, ok := lhs.(*ast.Ident); ok && w.pkg.info.Defs[id] != nil {
						loop.binds = append(loop.binds, w.pkg.info.Defs[id])
					}
				}
			}
			w.nested(x, w.sizedOperand(x.Cond), loop, loops)
			if x.Init != nil {
				w.walk(x.Init, loops)
			}
			w.walk(x.Body, append(append([]bulkWorkLoop{}, loops...), loop))
			return false
		}
		return true
	})
}

// nested reports a pass over query-sized data inside another such pass (or
// an owner batch) whose body does not observe the statement. A pass over a
// part of the outer loop's item (its properties, its row's values, its
// relationships) is linear in the data, not a product: its collection uses a
// variable an enclosing loop binds.
func (w bulkWorkWalker) nested(loop ast.Node, header ast.Expr, this bulkWorkLoop, loops []bulkWorkLoop) {
	if !this.sized || this.batch || w.partOfOuterItem(header, loops) {
		return
	}
	for _, outer := range loops {
		if outer.sized || outer.batch {
			if !w.observes(loop) {
				text := bulkWorkExpr(header)
				w.report("nested "+text, loop, "nested pass over query-sized data that does not observe the statement ("+text+")")
			}
			return
		}
	}
}

// firstChangeTarget is the variable of a copy made on a pass's first change:
// if out == nil { out = <copy>; ... }. The body runs once for each value
// out is declared with, so its copy is one copy at the place out is
// declared, not one for each item of the passes in between.
func (w bulkWorkWalker) firstChangeTarget(statement *ast.IfStmt) types.Object {
	condition, ok := statement.Cond.(*ast.BinaryExpr)
	for ok && condition.Op == token.LAND {
		// changed && out == nil
		condition, ok = condition.Y.(*ast.BinaryExpr)
	}
	if !ok || condition.Op != token.EQL {
		return nil
	}
	variable, ok := condition.X.(*ast.Ident)
	if null, isIdent := condition.Y.(*ast.Ident); !ok || !isIdent || null.Name != "nil" {
		return nil
	}
	target := w.pkg.info.Uses[variable]
	if target == nil {
		return nil
	}
	for _, statement := range statement.Body.List {
		assign, ok := statement.(*ast.AssignStmt)
		if !ok || assign.Tok != token.ASSIGN {
			continue
		}
		for _, lhs := range assign.Lhs {
			if id, ok := lhs.(*ast.Ident); ok && w.pkg.info.Uses[id] == target {
				return target
			}
		}
	}
	return nil
}

// loopsAround is the loops that enclose a declaration.
func loopsAround(at token.Pos, loops []bulkWorkLoop) []bulkWorkLoop {
	var around []bulkWorkLoop
	for _, loop := range loops {
		if loop.node != nil && loop.node.Pos() <= at && at < loop.node.End() {
			around = append(around, loop)
		}
	}
	return around
}

// alias records, in the innermost loop, a variable declared as an access
// (a field, element, slice, type assertion or dereference) of a part of the
// loop's item: a pass over it is a pass over that part. A call's result is
// not an access, so a traversal from the item stays a pass of its own.
func (w bulkWorkWalker) alias(assign *ast.AssignStmt, loops []bulkWorkLoop) {
	if assign.Tok != token.DEFINE || len(loops) == 0 || len(assign.Rhs) == 0 {
		return
	}
	for index, lhs := range assign.Lhs {
		rhs := assign.Rhs[0]
		if len(assign.Rhs) == len(assign.Lhs) {
			rhs = assign.Rhs[index]
		} else if index > 0 {
			break
		}
		id, ok := lhs.(*ast.Ident)
		if !ok || w.pkg.info.Defs[id] == nil || !isAccessExpr(rhs) || !w.partOfOuterItem(rhs, loops) {
			continue
		}
		loops[len(loops)-1].binds = append(loops[len(loops)-1].binds, w.pkg.info.Defs[id])
	}
}

// switchAlias records a type switch's binding of a part of the loop's item.
func (w bulkWorkWalker) switchAlias(statement *ast.TypeSwitchStmt, loops []bulkWorkLoop) {
	assign, ok := statement.Assign.(*ast.AssignStmt)
	if !ok || len(loops) == 0 || len(assign.Rhs) != 1 {
		return
	}
	asserted, ok := assign.Rhs[0].(*ast.TypeAssertExpr)
	if !ok || !isAccessExpr(asserted.X) || !w.partOfOuterItem(asserted.X, loops) {
		return
	}
	for _, clause := range statement.Body.List {
		if object := w.pkg.info.Implicits[clause]; object != nil {
			loops[len(loops)-1].binds = append(loops[len(loops)-1].binds, object)
		}
	}
}

// isAccessExpr reports an expression that only reads a part of a value.
func isAccessExpr(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.Ident:
		return true
	case *ast.ParenExpr:
		return isAccessExpr(x.X)
	case *ast.SelectorExpr:
		return isAccessExpr(x.X)
	case *ast.IndexExpr:
		return isAccessExpr(x.X)
	case *ast.SliceExpr:
		return isAccessExpr(x.X)
	case *ast.TypeAssertExpr:
		return isAccessExpr(x.X)
	case *ast.StarExpr:
		return isAccessExpr(x.X)
	}
	return false
}

// partOfOuterItem reports a collection expression that uses a variable an
// enclosing loop binds.
func (w bulkWorkWalker) partOfOuterItem(collection ast.Expr, loops []bulkWorkLoop) bool {
	if collection == nil {
		return false
	}
	part := false
	ast.Inspect(collection, func(n ast.Node) bool {
		id, ok := n.(*ast.Ident)
		if !ok {
			return !part
		}
		used := w.pkg.info.Uses[id]
		for _, outer := range loops {
			for _, bound := range outer.binds {
				if used != nil && used == bound {
					part = true
				}
			}
		}
		return !part
	})
	return part
}

// sizedOperand is the query-sized collection a counter loop's condition
// bounds it by (len(x)), or the condition itself.
func (w bulkWorkWalker) sizedOperand(cond ast.Expr) ast.Expr {
	var operand ast.Expr
	if cond == nil {
		return nil
	}
	ast.Inspect(cond, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && operand == nil {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "len" && len(call.Args) == 1 && isQuerySizedValue(w.pkg.info.TypeOf(call.Args[0])) {
				operand = call.Args[0]
			}
		}
		return operand == nil
	})
	if operand == nil {
		return cond
	}
	return operand
}

func (w bulkWorkWalker) rangeLoop(loop *ast.RangeStmt) bulkWorkLoop {
	if loop.X == nil {
		return bulkWorkLoop{}
	}
	if call, ok := loop.X.(*ast.CallExpr); ok {
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "materializedBatches" {
			return bulkWorkLoop{batch: true}
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "len" && len(call.Args) == 1 {
			return bulkWorkLoop{sized: isQuerySizedValue(w.pkg.info.TypeOf(call.Args[0]))}
		}
	}
	if slice, ok := loop.X.(*ast.SliceExpr); ok && w.isBatchBound(slice.Low) {
		return bulkWorkLoop{sized: true, batch: true}
	}
	return bulkWorkLoop{sized: isQuerySizedValue(w.pkg.info.TypeOf(loop.X))}
}

// isBatchBound reports batch.start of an owner batch.
func (w bulkWorkWalker) isBatchBound(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	named, ok := w.pkg.info.TypeOf(selector.X).(*types.Named)
	return ok && named.Obj().Name() == "materializedBatch"
}

func (w bulkWorkWalker) boundedByQuerySized(cond ast.Expr) bool {
	sized := false
	ast.Inspect(cond, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "len" && len(call.Args) == 1 && isQuerySizedValue(w.pkg.info.TypeOf(call.Args[0])) {
				sized = true
			}
		}
		return !sized
	})
	return sized
}

// observes reports a loop whose body checks the statement: the owner's
// batches or checkpoint, a scan, a context's Err, or a bulk primitive.
func (w bulkWorkWalker) observes(loop ast.Node) bool {
	found := false
	ast.Inspect(loop, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return !found
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			switch fun.Name {
			case "materializedBatches", "sortPopulation", "sortPopulationStable", "sortPopulationFunc", "stableSortRows", "sortStrings", "appendPopulation", "copyPopulation", "clonePopulation", "clonePopulationMap":
				found = true
			}
		case *ast.SelectorExpr:
			switch fun.Sel.Name {
			case "due", "canceled", "visit", "Observe", "observe", "Err":
				found = true
			}
			if f := calledFunc(w.pkg.info, call); f != nil && f.Pkg() != nil && f.Pkg().Path() == cypherPackagePath+"/bulk" {
				found = true
			}
		}
		return !found
	})
	return found
}

// call reports a sort, or a bulk copy or clone: of a population anywhere, or
// of a Cypher list or map inside a pass over query-sized data (a copy loop).
// A single copy of one list is one memmove; inside a pass it is a product.
// A copy of a part of an enclosing loop's item (its row's values, its path's
// steps) is linear in the data the loop walks.
func (w bulkWorkWalker) call(call *ast.CallExpr, loops []bulkWorkLoop) {
	info := w.pkg.info
	inPass := false
	for _, loop := range loops {
		if loop.sized || loop.batch {
			inPass = true
		}
	}
	copied := func(data ast.Expr) bool {
		typ := info.TypeOf(data)
		return (isPopulationValue(typ) || inPass && isQuerySizedValue(typ)) && !w.partOfOuterItem(data, loops)
	}
	if builtin, ok := call.Fun.(*ast.Ident); ok {
		switch info.Uses[builtin] {
		case types.Universe.Lookup("append"):
			if call.Ellipsis.IsValid() && len(call.Args) == 2 && copied(call.Args[1]) {
				w.report("append", call, "append of query-sized data ("+guardTypeString(info.TypeOf(call.Args[1]))+")")
			}
		case types.Universe.Lookup("copy"):
			if len(call.Args) == 2 && copied(call.Args[1]) {
				w.report("copy", call, "copy of query-sized data ("+guardTypeString(info.TypeOf(call.Args[1]))+")")
			}
		}
		return
	}
	fn := calledFunc(info, call)
	if fn == nil || fn.Pkg() == nil || len(call.Args) == 0 {
		return
	}
	kind := fn.Pkg().Name() + "." + fn.Name()
	switch kind {
	case "sort.Slice", "sort.SliceStable", "sort.Sort", "sort.Stable", "sort.Strings", "sort.Ints", "sort.Float64s", "slices.Sort", "slices.SortFunc", "slices.SortStableFunc":
		w.report(kind, call, kind+" outside the bulk owner ("+guardTypeString(info.TypeOf(call.Args[0]))+")")
	case "slices.Clone", "maps.Clone", "maps.Copy", "slices.Concat":
		data := call.Args[0]
		if kind == "maps.Copy" && len(call.Args) == 2 {
			data = call.Args[1]
		}
		if copied(data) {
			w.report(kind, call, kind+" of query-sized data ("+guardTypeString(info.TypeOf(data))+")")
		}
	}
}

// isQuerySizedValue reports a population, or a Cypher list or map value. A
// row (pipelineRow) maps the query's variables, so its own entries are
// bounded by the query text.
func isQuerySizedValue(typ types.Type) bool {
	if typ == nil {
		return false
	}
	if named, ok := typ.(*types.Named); ok && named.Obj().Name() == "pipelineRow" {
		return false
	}
	if isPopulationValue(typ) {
		return true
	}
	switch u := typ.Underlying().(type) {
	case *types.Slice:
		_, values := u.Elem().Underlying().(*types.Interface)
		return values
	case *types.Map:
		_, values := u.Elem().Underlying().(*types.Interface)
		return values
	}
	return false
}

func bulkWorkExpr(expr ast.Expr) string {
	if expr == nil {
		return "loop"
	}
	text := types.ExprString(expr)
	if len(text) > 80 {
		text = text[:80]
	}
	return strings.TrimSpace(text)
}

// Every registered procedure handler hands its implementation the statement
// context, so no procedure runs without the statement it belongs to: a
// procedure that sorts, copies or walks query-sized data reaches the bulk
// owner with it.
func TestProcedureHandlersPassTheStatement(t *testing.T) {
	pkg := loadCypherPackageForGuard(t, nil)
	var violations []string
	for _, file := range pkg.files {
		name := filepath.Base(pkg.fset.Position(file.Pos()).Filename)
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.FuncLit)
			if !ok || !isProcedureHandler(pkg.info, literal) {
				return true
			}
			first := literal.Type.Params.List[0]
			if len(first.Names) == 0 || first.Names[0].Name == "_" {
				violations = append(violations, fmt.Sprintf("%s:%d: procedure handler discards its statement context", name, pkg.fset.Position(literal.Pos()).Line))
				return true
			}
			ctx := pkg.info.Defs[first.Names[0]]
			used := false
			ast.Inspect(literal.Body, func(inner ast.Node) bool {
				if id, ok := inner.(*ast.Ident); ok && pkg.info.Uses[id] == ctx {
					used = true
				}
				return !used
			})
			if !used {
				violations = append(violations, fmt.Sprintf("%s:%d: procedure handler does not pass its statement context", name, pkg.fset.Position(literal.Pos()).Line))
			}
			return true
		})
	}
	sort.Strings(violations)
	require.Empty(t, violations)
}

// isProcedureHandler reports a function literal with a procedure handler's
// signature: (ctx context.Context, e *StorageExecutor, cypher string, args
// []interface{}) (*ExecuteResult, error).
func isProcedureHandler(info *types.Info, literal *ast.FuncLit) bool {
	signature, ok := info.TypeOf(literal).(*types.Signature)
	if !ok || signature.Params().Len() != 4 || signature.Results().Len() != 2 {
		return false
	}
	return types.TypeString(signature.Params().At(0).Type(), nil) == "context.Context" &&
		strings.HasSuffix(types.TypeString(signature.Params().At(1).Type(), nil), ".StorageExecutor") &&
		types.TypeString(signature.Results().At(1).Type(), nil) == "error" &&
		len(literal.Type.Params.List) > 0
}
