package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The entities a statement reads from storage in bulk are held by an owner
// from the moment they are read (#1343), by construction: the storage
// package's own declarations say which reads return entities in bulk, and
// every call of one in this package is passed straight to an owner's held
// method. No other code path can read a node or relationship list the
// statement's limit doesn't see; a new storage read, or a new call of one,
// fails here until it has an owner.
//
// The owner contract:
//   - a storage read that returns nodes, relationships or their IDs in bulk
//     (a label's, a type's, the store's, an index's, a batch get) is the
//     argument of owner.held*(…), always;
//   - a read of one node's relationships is held the same way where the
//     reader keeps them past its next read; where it only checks them before
//     its next read (a storage cursor, which Neo4j doesn't count either) the
//     site is listed below with what it does;
//   - an owner a function makes (trackedRetention) is released in it;
//   - a function that returns entities it read holds them in an owner its
//     caller passes (a *memoryRetention parameter): an owner it made itself
//     would give their charge back as it returns them;
//   - a caller of a function that takes an owner passes one it releases, its
//     own parameter, or one a structure it uses owns (&x.memory), never nil;
//   - a storage visitor (a read that calls back per entity) keeps nothing by
//     itself: each call site is listed below with what its callback does,
//     and one that appends to a list holds it (holdAppended).

// entityVisitorSites are the reviewed storage visitor calls, by enclosing
// function and storage method, with what the callback keeps.
var entityVisitorSites = map[string]string{
	"visitNodesWithStreamingProjection.StreamNodesByLabelProjected":    "appends to the caller's held list (holdAppended) or visits",
	"visitNodesWithStreamingProjection.StreamNodesWithOptions":         "appends to the caller's held list (holdAppended) or visits",
	"visitNodesWithStreamingProjection.StreamNodes":                    "appends to the caller's held list (holdAppended) or visits",
	"tryStreamPipelineFilteredNodeCount.StreamNodesByLabelProjected":   "counts",
	"fetchCosineNodeScoresProjectedLabel.StreamNodesByLabelProjected":  "keeps the top-k scores, bounded by the query's limit",
	"callDbIndexVectorQueryNodesInput.StreamNodesWithOptions":          "keeps the top-k scores, bounded by the query's limit",
	"callDbIndexFulltextQueryRelationships.StreamEdgesWithFallback":    "scores each relationship; keeps the matches, held",
	"buildGraphProjection.StreamNodesWithFallback":                     "builds a GDS projection, which the graph catalog owns",
	"buildGraphProjection.StreamEdgesWithFallback":                     "builds a GDS projection, which the graph catalog owns",
	"traverseGraphWithStreamingStartNodes.StreamNodesByLabelProjected": "traverses from each start node as it is read",
	"traverseGraphWithStreamingStartNodes.StreamNodes":                 "traverses from each start node as it is read",
	"collectIndexedOrderWindow.VisitPropertyIndexGroupsInRange":        "keeps the best limit nodes, held as the window fills (holdAppended)",
}

// nodeAdjacencySites are the reviewed reads of one node's relationships that
// no owner holds, by enclosing function and storage method, with what the
// reader does with them before its next read.
var nodeAdjacencySites = map[string]string{
	"computePageRank.GetOutgoingEdges":                       "one node's relationships, folded into the rank's target IDs before the next node's",
	"computeBetweenness.GetOutgoingEdges":                    "one node's relationships, expanded into the search queue before the next node's",
	"getNodeEdges.GetOutgoingEdges":                          "one node's relationships, returned to the path algorithm that expands them before the next node's",
	"getNodeEdges.GetIncomingEdges":                          "one node's relationships, returned to the path algorithm that expands them before the next node's",
	"getNeighborsByhop.GetOutgoingEdges":                     "one frontier node's relationships, turned into neighbour IDs before the next node's",
	"getNeighborsByhop.GetIncomingEdges":                     "one frontier node's relationships, turned into neighbour IDs before the next node's",
	"apocSteps.GetOutgoingEdges":                             "one node's relationships, filtered to the steps it follows before the next node's",
	"apocSteps.GetIncomingEdges":                             "one node's relationships, filtered to the steps it follows before the next node's",
	"callApocPathSubgraphAll.GetOutgoingEdges":               "one member's relationships, filtered into the result before the next member's",
	"traverseChain.GetOutgoingEdges":                         "one node's relationships per hop of a depth-first existence check: at most one list per hop of the pattern",
	"traverseChain.GetIncomingEdges":                         "one node's relationships per hop of a depth-first existence check: at most one list per hop of the pattern",
	"tryExecuteDeleteWithWithLimitHotPath.GetOutgoingEdges":  "counts the deleted node's relationships for the statistics",
	"tryExecuteDeleteWithWithLimitHotPath.GetIncomingEdges":  "counts the deleted node's relationships for the statistics",
	"validatePolicyOnLabelChange.GetOutgoingEdges":           "checks one node's relationships against the policies",
	"validatePolicyOnLabelChange.GetIncomingEdges":           "checks one node's relationships against the policies",
	"evaluateRelationshipPatternInWhere.GetIncomingEdges":    "checks whether one node has a matching relationship",
	"evaluateRelationshipPatternInWhere.GetOutgoingEdges":    "checks whether one node has a matching relationship",
	"nodeHasResidualRelationships.GetOutgoingEdges":          "checks whether one node keeps a relationship",
	"nodeHasResidualRelationships.GetIncomingEdges":          "checks whether one node keeps a relationship",
	"boundRelationshipDeleteCandidateEdges.GetOutgoingEdges": "one source node's relationships, filtered before the next source's; the IDs to delete are held",
	"boundRelationshipDeleteCandidateEdges.GetIncomingEdges": "one source node's relationships, filtered before the next source's; the IDs to delete are held",
	"gdsNeighbors.GetOutgoingEdges":                          "one node's neighbours for a GDS link function",
	"gdsNeighbors.GetIncomingEdges":                          "one node's neighbours for a GDS link function",
	"incidentEdgeListsContext.GetOutgoingEdges":              "the per-node reader of the path searches and DETACH DELETE: each caller expands one node's lists before the next node's",
	"incidentEdgeListsContext.GetIncomingEdges":              "the per-node reader of the path searches and DETACH DELETE: each caller expands one node's lists before the next node's",
	"incidentEdgeListsContext.GetAdjacentEdges":              "the per-node reader of the path searches and DETACH DELETE: each caller expands one node's lists before the next node's",
	"selectRelationshipMergeCreateID.GetEdgesBetween":        "one endpoint pair's relationships, read to pick a free ID",
	"findRelationshipsForMerge.MatchEdgesBetween":            "one endpoint pair's matching relationships, for one MERGE row",
	"nodeDegreeCount.GetOutgoingEdges":                       "counts one node's relationships",
	"nodeDegreeCount.GetIncomingEdges":                       "counts one node's relationships",
	"traversalEdges.OutgoingEdgeHeaders":                     "one node's relationships for an expansion step; a depth-first expansion holds one list per hop of the current path, and the paths it keeps are reserved (reservePath)",
	"traversalEdges.IncomingEdgeHeaders":                     "one node's relationships for an expansion step; a depth-first expansion holds one list per hop of the current path, and the paths it keeps are reserved (reservePath)",
	"traversalEdges.GetOutgoingEdges":                        "one node's relationships for an expansion step; a depth-first expansion holds one list per hop of the current path, and the paths it keeps are reserved (reservePath)",
	"traversalEdges.GetIncomingEdges":                        "one node's relationships for an expansion step; a depth-first expansion holds one list per hop of the current path, and the paths it keeps are reserved (reservePath)",
	"lookupRelationshipBatchExistingEdge.GetEdgesBetween":    "one endpoint pair's relationships, matched before the next pair's",
}

// entityReadsKeptAcrossIterations are functions whose loops keep what each
// iteration reads (an ID set or a list they build), so the reads' charges
// stay while the loop runs, with what they keep.
var entityReadsKeptAcrossIterations = map[string]string{
	"tryCollectNodesFromPropertyIndexAnyOf":            "each label's IDs go into the ID set it returns nodes from",
	"tryCollectNodesFromPropertyIndexInAnyOf":          "each value's IDs go into the ID set it returns nodes from",
	"tryCollectNodesFromPropertyIndexInLiteralAnyOf":   "each value's IDs go into the ID set it returns nodes from",
	"tryCollectNodesFromPropertyIndexInOrParam":        "each value's IDs go into the ID set it returns nodes from",
	"tryCollectNodesFromPropertyIndexOrEquality":       "each branch's IDs go into the ID set it returns nodes from",
	"tryCollectNodesFromPropertyIndexInCompound":       "returns the first conjunct's seek; the loop stops at it",
	"tryCollectNodesFromPropertyIndexEqualityCompound": "returns the first conjunct's seek; the loop stops at it",
	"lookupPatternCandidatesUsingPropertyIndex":        "each index's IDs go into the sets it intersects",
	"mergeNodeIndexedCandidateIDs":                     "returns the first full composite match or keeps the smallest property result; at most one lookup per pattern property",
	"findMergeNode":                                    "returns at the first match; at most one lookup per index and pattern property",
	"buildRelationshipBatchNodeMatchIndex":             "each label's matched nodes go into the batch index it returns",
}

// entityReadsOutsideStatements are functions that read entities in bulk
// with no statement running, so no limit applies to them.
var entityReadsOutsideStatements = map[string]bool{
	// The procedure catalog, loaded when the executor starts.
	"loadPersistedProcedures": true,
	// A new index's fill, a schema operation (Neo4j's index population is
	// not query memory either).
	"backfillCompositeIndex": true,
	// Exported for callers outside a statement; no statement calls it.
	"ApplyIndexHint": true,
}

// entityReadOwnerType is the owner type.
const entityReadOwnerType = "memoryRetention"

// storageEntityReads derives, from the storage package's declarations, the
// methods and functions that return entities in bulk (reads) and those that
// call back per entity (visitors).
func storageEntityReads(t *testing.T) (reads, adjacency, visitors map[string]bool) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "storage", "*.go"))
	require.NoError(t, err)
	files := token.NewFileSet()
	parsed := make([]*ast.File, 0, len(paths))
	visitorTypes := map[string]bool{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		parsed = append(parsed, file)
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range general.Specs {
				if typeSpec, ok := spec.(*ast.TypeSpec); ok {
					if function, ok := typeSpec.Type.(*ast.FuncType); ok && funcTakesEntities(function, nil) {
						visitorTypes[typeSpec.Name.Name] = true
					}
				}
			}
		}
	}
	reads, adjacency, visitors = map[string]bool{}, map[string]bool{}, map[string]bool{}
	classify := func(name string, function *ast.FuncType) {
		if function.Results != nil {
			for _, field := range function.Results.List {
				if isEntityListType(field.Type, "") {
					reads[name] = true
					// One node's relationships: the read takes a node ID.
					for _, parameter := range function.Params.List {
						if named, ok := parameter.Type.(*ast.Ident); ok && named.Name == "NodeID" {
							adjacency[name] = true
						}
					}
					return
				}
			}
		}
		for _, field := range function.Params.List {
			if parameter, ok := field.Type.(*ast.FuncType); ok && funcTakesEntities(parameter, visitorTypes) {
				visitors[name] = true
			}
			if named, ok := field.Type.(*ast.Ident); ok && visitorTypes[named.Name] {
				visitors[name] = true
			}
		}
	}
	for _, file := range parsed {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				if n.Name.IsExported() {
					classify(n.Name.Name, n.Type)
				}
			case *ast.InterfaceType:
				for _, method := range n.Methods.List {
					if function, ok := method.Type.(*ast.FuncType); ok {
						for _, name := range method.Names {
							classify(name.Name, function)
						}
					}
				}
			}
			return true
		})
	}
	for name := range reads {
		delete(visitors, name)
	}
	return reads, adjacency, visitors
}

// isEntityListType reports whether expression is a bulk entity type: a slice
// of nodes, relationships or their IDs, or nodes by ID. qualifier is the
// package the entity types are named in ("" in the storage package itself).
func isEntityListType(expression ast.Expr, qualifier string) bool {
	entity := func(expression ast.Expr, names ...string) bool {
		if star, ok := expression.(*ast.StarExpr); ok {
			expression = star.X
		}
		var name string
		switch typed := expression.(type) {
		case *ast.Ident:
			if qualifier != "" {
				return false
			}
			name = typed.Name
		case *ast.SelectorExpr:
			if package_, ok := typed.X.(*ast.Ident); !ok || package_.Name != qualifier {
				return false
			}
			name = typed.Sel.Name
		default:
			return false
		}
		for _, candidate := range names {
			if name == candidate {
				return true
			}
		}
		return false
	}
	// element is what a container keeps: an entity, an entity ID, or a
	// container of them, at any depth (a cache by key, a list of lists).
	var element func(expression ast.Expr) bool
	element = func(expression ast.Expr) bool {
		switch typed := expression.(type) {
		case *ast.StarExpr:
			return entity(typed, "Node", "Edge")
		case *ast.ArrayType:
			return element(typed.Elt)
		case *ast.MapType:
			return element(typed.Value)
		}
		return entity(expression, "NodeID", "EdgeID")
	}
	switch typed := expression.(type) {
	case *ast.ArrayType:
		return element(typed.Elt)
	case *ast.MapType:
		return element(typed.Value)
	}
	return false
}

// funcTakesEntities reports whether a callback type receives entities.
func funcTakesEntities(function *ast.FuncType, visitorTypes map[string]bool) bool {
	for _, field := range function.Params.List {
		expression := field.Type
		if array, ok := expression.(*ast.ArrayType); ok {
			expression = array.Elt
		}
		if star, ok := expression.(*ast.StarExpr); ok {
			expression = star.X
		}
		if named, ok := expression.(*ast.Ident); ok {
			switch named.Name {
			case "Node", "Edge", "NodeID", "EdgeID":
				return true
			}
			if visitorTypes[named.Name] {
				return true
			}
		}
	}
	return false
}

// entityReadViolations checks the owner contract over sources (file name to
// source) of this package.
func entityReadViolations(t *testing.T, sources map[string][]byte, reads, adjacency, visitors map[string]bool, reviewedAdjacency, reviewed map[string]string) (violations []string, readCalls int) {
	t.Helper()
	files := token.NewFileSet()
	type function struct {
		declaration *ast.FuncDecl
		file        string
	}
	var functions []function
	ownerParameter := map[string]int{}
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		file, err := parser.ParseFile(files, name, sources[name], parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			declared, ok := declaration.(*ast.FuncDecl)
			if !ok || declared.Body == nil {
				continue
			}
			functions = append(functions, function{declared, name})
			index := 0
			for _, field := range declared.Type.Params.List {
				count := max(len(field.Names), 1)
				if isOwnerType(field.Type) {
					if previous, seen := ownerParameter[declared.Name.Name]; seen && previous != index {
						violations = append(violations, declared.Name.Name+": two functions of this name take an owner at different positions")
					}
					ownerParameter[declared.Name.Name] = index
				}
				index += count
			}
		}
	}
	seenVisitors, seenAdjacency := map[string]bool{}, map[string]bool{}
	for _, entry := range functions {
		declared := entry.declaration
		switch receiverType(declared) {
		case "transactionStorageWrapper":
			// It is a storage engine: its reads are the storage's.
			continue
		case entityReadOwnerType:
			// The owner's own methods.
			continue
		}
		if entityReadsOutsideStatements[declared.Name.Name] {
			continue
		}
		parameters, locals, borrowed, released := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
		aliases := map[string][]string{}
		for _, field := range declared.Type.Params.List {
			if isOwnerType(field.Type) {
				for _, name := range field.Names {
					parameters[name.Name] = true
				}
			}
		}
		returnsEntities := false
		if declared.Type.Results != nil {
			for _, field := range declared.Type.Results.List {
				if isEntityListType(field.Type, "storage") {
					returnsEntities = true
				}
			}
		}
		ast.Inspect(declared.Body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for index, left := range n.Lhs {
					target, ok := left.(*ast.Ident)
					if !ok || index >= len(n.Rhs) {
						continue
					}
					switch source := n.Rhs[index].(type) {
					case *ast.CallExpr:
						if called, ok := source.Fun.(*ast.Ident); ok && called.Name == "trackedRetention" {
							locals[target.Name] = true
						}
					case *ast.UnaryExpr:
						if source.Op == token.AND {
							if _, field := source.X.(*ast.SelectorExpr); field {
								borrowed[target.Name] = true
							}
						}
					case *ast.Ident:
						aliases[target.Name] = append(aliases[target.Name], source.Name)
					}
				}
			case *ast.CallExpr:
				if selector, ok := n.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "release" {
					if receiver, ok := selector.X.(*ast.Ident); ok {
						released[receiver.Name] = true
					}
				}
			}
			return true
		})
		// owner reports whether name is a usable owner, and whether the
		// function made it itself.
		var owner func(name string, depth int) (usable, made bool)
		owner = func(name string, depth int) (usable, made bool) {
			switch {
			case parameters[name], borrowed[name]:
				return true, false
			case locals[name]:
				return released[name], true
			}
			sources := aliases[name]
			if len(sources) == 0 || depth > 4 {
				return false, false
			}
			usable = true
			for _, source := range sources {
				sourceUsable, sourceMade := owner(source, depth+1)
				usable = usable && (sourceUsable || released[name])
				made = made || sourceMade
			}
			return usable, made
		}
		where := func(node ast.Node) string { return files.Position(node.Pos()).String() }
		checkOwner := func(node ast.Node, expression ast.Expr, use string) {
			if field, ok := expression.(*ast.SelectorExpr); ok && field.Sel.Name == "owner" {
				// An owner a structure keeps, given to it by its
				// constructor (whose callers pass one) for as long as it
				// keeps what it reads.
				return
			}
			name, ok := expression.(*ast.Ident)
			if !ok || name.Name == "nil" {
				violations = append(violations, where(node)+": "+use+" has no owner")
				return
			}
			usable, made := owner(name.Name, 0)
			switch {
			case !usable:
				violations = append(violations, where(node)+": "+use+": owner "+name.Name+" is not released")
			case made && returnsEntities:
				violations = append(violations, where(node)+": "+use+": "+declared.Name.Name+" returns entities but holds them in an owner it made; take one from its caller")
			}
		}
		var stack []ast.Node
		// checkIteration: a list read in a loop and bound for the iteration
		// only (x := read inside the loop) is dropped when the next one
		// replaces it, so the owner that holds it is released in the loop
		// before the next read: otherwise the charges of the lists it
		// dropped add up. A loop that keeps what each iteration reads (in a
		// set or list of its own) is listed in entityReadsKeptAcrossIterations.
		checkIteration := func(call *ast.CallExpr, owner, use string) {
			if _, kept := entityReadsKeptAcrossIterations[declared.Name.Name]; kept {
				return
			}
			if len(stack) == 0 {
				return
			}
			if assign, ok := stack[len(stack)-1].(*ast.AssignStmt); !ok || assign.Tok != token.DEFINE {
				return
			}
			var loop ast.Node
		ancestors:
			for index := len(stack) - 1; index >= 0; index-- {
				switch n := stack[index].(type) {
				case *ast.ForStmt, *ast.RangeStmt:
					loop = n
					break ancestors
				case *ast.FuncLit:
					break ancestors
				}
			}
			if loop == nil {
				return
			}
			released := false
			ast.Inspect(loop, func(node ast.Node) bool {
				if inner, ok := node.(*ast.CallExpr); ok {
					if selector, ok := inner.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "release" {
						if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == owner {
							released = true
						}
					}
				}
				return !released
			})
			if !released {
				violations = append(violations, where(call)+": "+use+" in a loop of "+declared.Name.Name+" binds a list for one iteration, but owner "+owner+" is not released in the loop: the lists it drops stay counted")
			}
		}
		ast.Inspect(declared.Body, func(node ast.Node) bool {
			if node == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			defer func() { stack = append(stack, node) }()
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var called string
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				called = fun.Sel.Name
				if strings.HasPrefix(called, "held") {
					checkOwner(call, fun.X, called)
				}
			case *ast.Ident:
				called = fun.Name
			}
			if index, takes := ownerParameter[called]; takes && index < len(call.Args) {
				checkOwner(call, call.Args[index], called)
				if owner, ok := call.Args[index].(*ast.Ident); ok {
					checkIteration(call, owner.Name, called)
				}
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && strings.HasPrefix(called, "held") {
				if owner, ok := selector.X.(*ast.Ident); ok {
					checkIteration(call, owner.Name, called)
				}
			}
			if _, isSelector := call.Fun.(*ast.SelectorExpr); !isSelector {
				return true
			}
			switch {
			case reads[called]:
				readCalls++
				parent, ok := stack[len(stack)-1].(*ast.CallExpr)
				if ok && isHeldCall(parent) && containsArgument(parent, call) {
					break
				}
				site := declared.Name.Name + "." + called
				if adjacency[called] {
					seenAdjacency[site] = true
					if _, ok := reviewedAdjacency[site]; ok {
						if receiver := receiverType(declared); receiver != "" && receiver != "StorageExecutor" {
							// A method of a type that keeps state can keep
							// the list across calls: it needs an owner.
							violations = append(violations, where(call)+": "+site+" is in a method of "+receiver+", which can keep what it reads across calls: hold it in an owner")
						}
						break
					}
				}
				violations = append(violations, where(call)+": "+called+"() read in "+declared.Name.Name+" is not held: pass it to an owner's held method")
			case visitors[called]:
				site := declared.Name.Name + "." + called
				seenVisitors[site] = true
				if _, ok := reviewed[site]; !ok {
					violations = append(violations, where(call)+": storage visitor "+site+" is not reviewed: add it to entityVisitorSites with what its callback keeps")
				}
			}
			return true
		})
	}
	for site := range reviewed {
		if !seenVisitors[site] {
			violations = append(violations, "entityVisitorSites lists "+site+", which no longer exists")
		}
	}
	for site := range reviewedAdjacency {
		if !seenAdjacency[site] {
			violations = append(violations, "nodeAdjacencySites lists "+site+", which is held or no longer exists")
		}
	}
	sort.Strings(violations)
	return violations, readCalls
}

func isOwnerType(expression ast.Expr) bool {
	star, ok := expression.(*ast.StarExpr)
	if !ok {
		return false
	}
	name, ok := star.X.(*ast.Ident)
	return ok && name.Name == entityReadOwnerType
}

func receiverType(declared *ast.FuncDecl) string {
	if declared.Recv == nil || len(declared.Recv.List) == 0 {
		return ""
	}
	expression := declared.Recv.List[0].Type
	if star, ok := expression.(*ast.StarExpr); ok {
		expression = star.X
	}
	if name, ok := expression.(*ast.Ident); ok {
		return name.Name
	}
	return ""
}

func isHeldCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && strings.HasPrefix(selector.Sel.Name, "held")
}

func containsArgument(call *ast.CallExpr, argument ast.Expr) bool {
	for _, candidate := range call.Args {
		if candidate == argument {
			return true
		}
	}
	return false
}

func TestEntityReadsHaveAnOwner(t *testing.T) {
	reads, adjacency, visitors := storageEntityReads(t)
	for _, name := range []string{"GetNodesByLabel", "AllNodes", "AllEdges", "GetOutgoingEdges", "GetEdgesByType", "PropertyIndexLookup", "LookupFull", "NodeIDsByLabel", "BatchGetNodes", "MatchEdgesBetween"} {
		require.True(t, reads[name], "%s is not derived as a bulk read", name)
	}
	for _, name := range []string{"GetOutgoingEdges", "GetIncomingEdges", "GetEdgesBetween", "MatchEdgesBetween", "OutgoingEdgeHeaders"} {
		require.True(t, adjacency[name], "%s is not derived as one node's relationships", name)
	}
	for _, name := range []string{"GetNodesByLabel", "AllEdges", "GetEdgesByType", "PropertyIndexLookup", "BatchGetNodes"} {
		require.False(t, adjacency[name], "%s is derived as one node's relationships", name)
	}
	for _, name := range []string{"StreamNodes", "StreamNodesByLabelProjected", "StreamEdgesWithFallback"} {
		require.True(t, visitors[name], "%s is not derived as a visitor", name)
	}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	sources := map[string][]byte{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		require.NoError(t, err)
		sources[name] = source
	}
	violations, readCalls := entityReadViolations(t, sources, reads, adjacency, visitors, nodeAdjacencySites, entityVisitorSites)
	require.Emptyf(t, violations, "entities read without an owner:\n%s", strings.Join(violations, "\n"))
	require.GreaterOrEqual(t, readCalls, 100, "the package's storage reads were not found")
}

// The check finds a read nothing holds, an owner never released, entities
// returned from an owner the function made, a caller passing no owner, and
// an unreviewed visitor.
func TestEntityReadCheckFindsPlantedSites(t *testing.T) {
	reads := map[string]bool{"GetNodesByLabel": true, "GetOutgoingEdges": true}
	adjacency := map[string]bool{"GetOutgoingEdges": true}
	visitors := map[string]bool{"StreamNodes": true}
	planted := `package cypher

func unheld(store storage.Engine) int {
	nodes, _ := store.GetNodesByLabel("A")
	return len(nodes)
}

func unreleased(ctx context.Context, store storage.Engine) int {
	owner := trackedRetention(ctx)
	nodes, _ := owner.heldNodes(store.GetNodesByLabel("A"))
	return len(nodes)
}

func returnsOwn(ctx context.Context, store storage.Engine) ([]*storage.Node, error) {
	owner := trackedRetention(ctx)
	defer owner.release()
	return owner.heldNodes(store.GetNodesByLabel("A"))
}

func takesOwner(owner *memoryRetention, store storage.Engine) ([]*storage.Node, error) {
	return owner.heldNodes(store.GetNodesByLabel("A"))
}

func passesNil(store storage.Engine) {
	_, _ = takesOwner(nil, store)
}

func visits(ctx context.Context, store storage.StreamingEngine) {
	_ = store.StreamNodes(ctx, nil)
}

func unreviewedAdjacency(store storage.Engine, node *storage.Node) bool {
	edges, _ := store.GetOutgoingEdges(node.ID)
	return len(edges) > 0
}

func accumulates(ctx context.Context, store storage.Engine, labels []string) int {
	owner := trackedRetention(ctx)
	defer owner.release()
	total := 0
	for _, label := range labels {
		nodes, _ := owner.heldNodes(store.GetNodesByLabel(label))
		total += len(nodes)
	}
	return total
}

func (c *pairCache) keeps(store storage.Engine, node *storage.Node) []*storage.Edge {
	edges, _ := store.GetOutgoingEdges(node.ID)
	return edges
}

func cacheByKey(ctx context.Context, store storage.Engine) (map[string]*storage.Node, error) {
	owner := trackedRetention(ctx)
	defer owner.release()
	nodes, err := owner.heldNodes(store.GetNodesByLabel("A"))
	byKey := map[string]*storage.Node{}
	for _, node := range nodes {
		byKey[string(node.ID)] = node
	}
	return byKey, err
}
`
	violations, _ := entityReadViolations(t, map[string][]byte{"planted.go": []byte(planted)}, reads, adjacency, visitors, map[string]string{"keeps.GetOutgoingEdges": "planted"}, map[string]string{})
	require.Len(t, violations, 9, "%s", strings.Join(violations, "\n"))
	for index, want := range []string{"GetNodesByLabel() read in unheld is not held", "owner owner is not released", "returnsOwn returns entities but holds them in an owner it made", "takesOwner has no owner", "storage visitor visits.StreamNodes is not reviewed", "GetOutgoingEdges() read in unreviewedAdjacency is not held", "heldNodes in a loop of accumulates binds a list for one iteration", "keeps.GetOutgoingEdges is in a method of pairCache", "cacheByKey returns entities but holds them in an owner it made"} {
		found := false
		for _, violation := range violations {
			found = found || strings.Contains(violation, want)
		}
		require.Truef(t, found, "violation %d (%s) not found in:\n%s", index, want, strings.Join(violations, "\n"))
	}

	clean := `package cypher

func holds(ctx context.Context, store storage.Engine) (int, error) {
	owner := trackedRetention(ctx)
	defer owner.release()
	nodes, err := takesOwner(owner, store)
	return len(nodes), err
}

func takesOwner(owner *memoryRetention, store storage.Engine) ([]*storage.Node, error) {
	return owner.heldNodes(store.GetNodesByLabel("A"))
}

func perNode(ctx context.Context, store storage.Engine, labels []string) int {
	edges := trackedRetention(ctx)
	defer edges.release()
	total := 0
	for _, label := range labels {
		edges.release()
		nodes, _ := edges.heldNodes(store.GetNodesByLabel(label))
		total += len(nodes)
	}
	return total
}

func visits(ctx context.Context, store storage.StreamingEngine) {
	_ = store.StreamNodes(ctx, nil)
}

func reviewedAdjacency(store storage.Engine, node *storage.Node) bool {
	edges, _ := store.GetOutgoingEdges(node.ID)
	return len(edges) > 0
}
`
	violations, readCalls := entityReadViolations(t, map[string][]byte{"clean.go": []byte(clean)}, reads, adjacency, visitors, map[string]string{"reviewedAdjacency.GetOutgoingEdges": "checks"}, map[string]string{"visits.StreamNodes": "visits"})
	require.Empty(t, violations)
	require.Equal(t, 3, readCalls)
}
