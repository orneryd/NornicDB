package storage

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The publication boundary (F-PUBLISH), extracted from this package's
// type-checked source so it stays complete as the code changes:
//   - the publishing commit paths: every function that hands the ordered
//     stage a publication (badger_publication_stage.go);
//   - every function a publication can reach, and every lock it takes there;
//   - the operation types a commit carries.
//
// TestPublicationStageBoundary fails when the extraction finds a publishing
// path, a lock (a consumer) or an operation type the tables below don't
// classify, or a commit a publication could reach: the stage must never wait
// on itself.

// publishingPaths maps each function that hands the stage a publication to
// the test that runs it under a held older publication.
var publishingPaths = map[string]string{
	"CreateNode":  "TestPostCommitPublicationOrder (create)",
	"UpdateNode":  "TestPostCommitPublicationOrder (update, restore, label add and remove)",
	"DeleteNode":  "TestPostCommitPublicationOrder (delete)",
	"CreateEdge":  "TestPostCommitEdgePublicationOrder (create)",
	"UpdateEdge":  "TestPostCommitEdgePublicationOrder (update, restore, type change)",
	"DeleteEdge":  "TestPostCommitEdgePublicationOrder (delete)",
	"publication": "transaction commits: TestPostCommitPublicationOrder and TestPostCommitEdgePublicationOrder (older-tx, newer-tx)",
	"commitPublication.run": "every publication: the commit's pending-embedding candidates (commutative, TestPendingCandidates*), then the writer's publish",
	"applyBufferedCommitsWithCountPublication": "TestPostCommitPublicationOrderWriteBehindReplay",
	"EnqueueDeindexIfSuppressed":               "TestPostCommitPublicationOrderSuppression",
	"UpdateNodeEmbeddingSidecar":               "TestPostCommitPublicationOrderEmbeddingSidecar",
}

// stageLocks classifies every lock a publication takes, by the struct field
// that holds it: the consumer it guards. A publishing committer must not
// hold any of them across its commit (the deadlock analysis in the PR body);
// a new one fails the test.
var stageLocks = map[string]string{
	"BadgerEngine.nodeCacheMu":           "node and query-node caches (TestPostCommitPublicationOrder: node cache, query-node cache)",
	"BadgerEngine.nodeBodyCacheMu":       "node body cache, versioned per Badger item (dropped with the node)",
	"BadgerEngine.edgeCacheMu":           "edge cache (TestPostCommitEdgePublicationOrder: edge cache)",
	"BadgerEngine.adjCacheMu":            "adjacency cache (TestPostCommitEdgePublicationOrder: adjacency, read twice)",
	"BadgerEngine.edgeTypeCacheMu":       "edge-type index cache (TestPostCommitEdgePublicationOrder: edge-type index)",
	"BadgerEngine.labelFirstNodeCacheMu": "label index cache (TestPostCommitPublicationOrder: label index)",
	"BadgerEngine.namespaceCountsMu":     "node and edge counts (both matrices: node, label and edge counts)",
	"BadgerEngine.schemasMu":             "schema lookup by namespace (read lock)",
	"SchemaManager.mu":                   "schema: index and unique constraint lookup (read lock)",
	"CompositeIndex.mu":                  "property and composite indexes (TestPostCommitPublicationOrder: property index, composite index)",
	"UniqueConstraint.mu":                "unique-value registry (TestPostCommitPublicationOrder: registry holder, duplicate rejected, replaced value free)",
	"relationshipUniqueIndexSet.mu":      "relationship-uniqueness index (TestPostCommitEdgePublicationOrder: duplicate rejected, replaced value free)",
	"pendingEmbeddingCandidates.mu":      "pending-embedding candidates, an add-only set of hints (TestPendingCandidates*)",
}

// uniqueKeyLockWait stands for waiting on a unique constraint key lock
// (waitForUniqueConstraintLock), which committers hold until they return.
const uniqueKeyLockWait = "unique key locks (waitForUniqueConstraintLock)"

// knownCommitterLocks are locks committers hold across their commit and its
// wait for the stage that the scan can't see, because a helper takes them
// and returns the release: the write barrier (beginNotifyingWrite,
// beginPublication, beginSchemaWrite), the transaction's mutex and count
// locks (Commit, acquireCommitPublicationLocked) and unique key locks
// (lockEngineWriteKeys, PrepareMergeKey).
var knownCommitterLocks = []string{
	"BadgerEngine.writeBarrier", "BadgerTransaction.mu", "BadgerEngine.labelCountWriteMu",
	"BadgerEngine.edgeTypeCountWriteMu", "BadgerEngine.totalCountWriteMu", uniqueKeyLockWait,
}

// heldAcrossCommit classifies every place that holds a stage lock across a
// call that can commit: why the commit carries no publication, so it does
// not run the stage.
var heldAcrossCommit = map[string]string{}

// lockOrderReviewed classifies every place that holds a stage lock while it
// takes a lock some committer holds across its stage wait: why the two are
// never the same instance in a wait cycle.
var lockOrderReviewed = map[string]string{
	"StageSchemaChanges holds SchemaManager.mu while taking BadgerTransaction.mu":                                    "the SchemaManager is the transaction's private schema view (tx.knowledgeSchema), never a schema a publication locks, and the BadgerTransaction is the staging transaction itself, which cannot be committing at the same time",
	"FindNodeNeedingEmbedding holds pendingEmbeddingCandidates.mu while withUpdate takes managedBadgerDB.commitGate": "commitGate is held only while a commit takes its timestamp and writes (commitShared), by a large commit until it releases the intent (beginLargeCommit to release) and by the write-behind acknowledgement (commitBufferedLocked); none of them waits for the stage, which applyThrough runs after the gate is released. FindNodeNeedingEmbedding's own commit carries no publication (it only deletes pending-embedding keys), so it does not run the stage",
}

// commitFunctions must never be reachable from a publication.
var commitFunctions = []string{
	"commitEngineWrite", "commitEngineWriteWithRead", "commitEngineWriteHeld", "withUpdate", "withNotifyingUpdate",
	"withPublishingUpdate", "withUpdateUnits", "commit", "commitPublishing", "commitShared", "commitBatch",
	"beginLargeCommit", "applyThrough", "waitPublished", "CommitAt", "Commit", "CommitContext",
}

// operationCoverage maps every operation type to the matrix rows that cover
// it, or why it publishes nothing.
var operationCoverage = map[string]string{
	"OpCreateNode":      "create, delete then recreate",
	"OpUpdateNode":      "update, restore, label add, label remove, two updates in one transaction",
	"OpDeleteNode":      "delete, delete without a snapshot (TestPostCommitPublicationWithinOneCommit)",
	"OpCreateEdge":      "edge create (delete then recreate)",
	"OpUpdateEdge":      "edge update, restore, type change",
	"OpDeleteEdge":      "edge delete",
	"OpUpdateEmbedding": "no derived state: embeddings are regenerable; the sidecar write path is TestPostCommitPublicationOrderEmbeddingSidecar",
}

type packageSource struct {
	fset  *token.FileSet
	files []*ast.File
	info  *types.Info
	decls map[*types.Func]*ast.FuncDecl
}

func checkPackageSource(t *testing.T) *packageSource {
	t.Helper()
	src := &packageSource{fset: token.NewFileSet(), decls: map[*types.Func]*ast.FuncDecl{}}
	ctxt := build.Default
	ctxt.BuildTags = append(ctxt.BuildTags, "noui", "nolocalllm")
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		if match, err := ctxt.MatchFile(".", path); err != nil || !match {
			continue
		}
		file, err := parser.ParseFile(src.fset, path, nil, 0)
		require.NoError(t, err)
		src.files = append(src.files, file)
	}
	src.info = &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
	var typeErrors []string
	conf := types.Config{Importer: importer.ForCompiler(src.fset, "source", nil), Error: func(err error) { typeErrors = append(typeErrors, err.Error()) }}
	_, _ = conf.Check("github.com/orneryd/nornicdb/pkg/storage", src.fset, src.files, src.info)
	require.Empty(t, typeErrors, "type-checking pkg/storage")
	for _, file := range src.files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				if obj, ok := src.info.Defs[fn.Name].(*types.Func); ok {
					src.decls[obj] = fn
				}
			}
		}
	}
	return src
}

// stageRoots returns the publications in the source, by the function each
// is handed over in: closures assigned to a commit writer's publish field or
// to a local named publish, closures passed to withPublishingUpdate, and the
// publication methods.
func (src *packageSource) stageRoots() map[string][]ast.Node {
	roots := map[string][]ast.Node{}
	for _, file := range src.files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "run" && fn.Recv != nil && types.ExprString(fn.Recv.List[0].Type) == "*commitPublication" {
				// The stage runs every publication through it.
				roots["commitPublication.run"] = append(roots["commitPublication.run"], fn.Body)
			}
			if fn.Name.Name == "publication" {
				// What the stage runs is the closure the method returns; the
				// rest of the method runs before the commit.
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if ret, ok := n.(*ast.ReturnStmt); ok {
						for _, result := range ret.Results {
							if lit, ok := result.(*ast.FuncLit); ok {
								roots[fn.Name.Name] = append(roots[fn.Name.Name], lit.Body)
							}
						}
					}
					return true
				})
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for i, lhs := range n.Lhs {
						if i >= len(n.Rhs) {
							break
						}
						lit, isLit := n.Rhs[i].(*ast.FuncLit)
						if !isLit {
							continue
						}
						switch target := lhs.(type) {
						case *ast.SelectorExpr:
							if target.Sel.Name == "publish" {
								roots[fn.Name.Name] = append(roots[fn.Name.Name], lit.Body)
							}
						case *ast.Ident:
							if target.Name == "publish" {
								roots[fn.Name.Name] = append(roots[fn.Name.Name], lit.Body)
							}
						}
					}
				case *ast.CallExpr:
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "withPublishingUpdate" && len(n.Args) >= 2 {
						if lit, ok := n.Args[1].(*ast.FuncLit); ok {
							roots[fn.Name.Name] = append(roots[fn.Name.Name], lit.Body)
						}
					}
				}
				return true
			})
		}
	}
	return roots
}

// callee resolves the function or method call calls, nil for a call of a
// function value (a closure, a func field) or a conversion.
func (src *packageSource) callee(call *ast.CallExpr) *types.Func {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		obj, _ := src.info.Uses[fun].(*types.Func)
		return obj
	case *ast.SelectorExpr:
		if sel := src.info.Selections[fun]; sel != nil {
			obj, _ := sel.Obj().(*types.Func)
			return obj
		}
		obj, _ := src.info.Uses[fun.Sel].(*types.Func)
		return obj
	case *ast.IndexExpr:
		if ident, ok := fun.X.(*ast.Ident); ok {
			obj, _ := src.info.Uses[ident].(*types.Func)
			return obj
		}
	}
	return nil
}

// lockName names the mutex a Lock or RLock call takes by the struct field
// that holds it ("BadgerEngine.nodeCacheMu"), or the package variable.
func (src *packageSource) lockName(x ast.Expr) string {
	switch x := x.(type) {
	case *ast.SelectorExpr:
		if sel := src.info.Selections[x]; sel != nil && sel.Kind() == types.FieldVal {
			recv := sel.Recv()
			if ptr, ok := recv.(*types.Pointer); ok {
				recv = ptr.Elem()
			}
			name := types.TypeString(recv, func(*types.Package) string { return "" })
			return name + "." + x.Sel.Name
		}
	case *ast.Ident:
		if obj := src.info.Uses[x]; obj != nil {
			return obj.Name()
		}
	case *ast.UnaryExpr:
		return src.lockName(x.X)
	}
	var b strings.Builder
	ast.Fprint(&b, nil, x, nil)
	return "unresolved " + b.String()
}

// reach walks the type-resolved call graph from roots: the package's own
// functions are followed, others recorded by name.
func (src *packageSource) reach(roots []ast.Node) (reached map[string]bool, locks map[string][]string, dynamic []string, via map[string]string) {
	reached, locks, via = map[string]bool{}, map[string][]string{}, map[string]string{}
	seen := map[*types.Func]bool{}
	var visit func(owner string, node ast.Node)
	visit = func(owner string, node ast.Node) {
		stored := storedClosures(node)
		ast.Inspect(node, func(n ast.Node) bool {
			if lit, ok := n.(*ast.FuncLit); ok && stored[lit] {
				return false // kept for later, not run here
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn := src.callee(call)
			if fn == nil {
				if _, isLit := call.Fun.(*ast.FuncLit); !isLit && !isConversion(src.info, call) && !isBuiltin(src.info, call) {
					entry := owner + ": " + exprString(call.Fun)
					if !slices.Contains(dynamic, entry) {
						dynamic = append(dynamic, entry)
					}
				}
				return true
			}
			name := fn.Name()
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (name == "Lock" || name == "RLock" || name == "TryLock" || name == "TryRLock") {
				lock := src.lockName(sel.X)
				if !slices.Contains(locks[lock], owner) {
					locks[lock] = append(locks[lock], owner)
				}
				return true
			}
			if !reached[name] {
				via[name] = owner
			}
			reached[name] = true
			if decl, own := src.decls[fn]; own && !seen[fn] {
				seen[fn] = true
				visit(name, decl.Body)
			}
			return true
		})
	}
	for _, root := range roots {
		visit("publication", root)
	}
	return reached, locks, dynamic, via
}

// storedClosures are the closures in node that are kept rather than run
// where they appear: assigned, declared, returned or placed in a literal.
// A closure passed to a call or called in place is walked.
func storedClosures(node ast.Node) map[*ast.FuncLit]bool {
	stored := map[*ast.FuncLit]bool{}
	mark := func(exprs ...ast.Expr) {
		for _, e := range exprs {
			if lit, ok := e.(*ast.FuncLit); ok {
				stored[lit] = true
			}
		}
	}
	ast.Inspect(node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			mark(n.Rhs...)
		case *ast.ValueSpec:
			mark(n.Values...)
		case *ast.ReturnStmt:
			mark(n.Results...)
		case *ast.KeyValueExpr:
			mark(n.Value)
		case *ast.CompositeLit:
			mark(n.Elts...)
		}
		return true
	})
	return stored
}

func isBuiltin(info *types.Info, call *ast.CallExpr) bool {
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	_, builtin := info.Uses[ident].(*types.Builtin)
	return builtin
}

// chain is the call path that first reached name.
func chain(via map[string]string, name string) string {
	path := []string{name}
	for at := name; via[at] != "" && len(path) < 40; at = via[at] {
		path = append(path, via[at])
	}
	slices.Reverse(path)
	return strings.Join(path, " -> ")
}

// publishingCommits returns every function of the package that can reach a
// commit carrying a publication, the commits that wait for the stage: a
// function that sets a commit writer's publish, hands withPublishingUpdate a
// publication, or stages pending-embedding candidates (which the commit
// writer publishes), and every function that can call one, found by a
// reverse search of the call graph. Stored closures are not calls.
func (src *packageSource) publishingCommits() map[*types.Func]bool {
	callers := map[*types.Func][]*types.Func{}
	reaches := map[*types.Func]bool{}
	var queue []*types.Func
	for fn, decl := range src.decls {
		// Closures a function defines run within its own commit, so they
		// count as its calls here.
		publishes := false
		ast.Inspect(decl.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					// A closure or a built publication (tx.publication());
					// forwarding a variable (withPublishingUpdate's own
					// parameter) publishes only what its caller passed.
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "publish" && i < len(n.Rhs) {
						switch n.Rhs[i].(type) {
						case *ast.FuncLit, *ast.CallExpr:
							publishes = true
						}
					}
				}
			case *ast.CallExpr:
				callee := src.callee(n)
				if callee == nil {
					return true
				}
				switch callee.Name() {
				case "withPublishingUpdate":
					if len(n.Args) >= 2 {
						if ident, isNil := n.Args[1].(*ast.Ident); !isNil || ident.Name != "nil" {
							publishes = true
						}
					}
				case "setPendingEmbeddingInTxn", "setKeyWithPendingHint":
					publishes = true
				}
				callers[callee] = append(callers[callee], fn)
			}
			return true
		})
		if publishes {
			reaches[fn] = true
			queue = append(queue, fn)
		}
	}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		for _, caller := range callers[fn] {
			if !reaches[caller] {
				reaches[caller] = true
				queue = append(queue, caller)
			}
		}
	}
	return reaches
}

// heldAcrossCommits finds, in every function of the package, the calls that
// can reach a commit carrying a publication (publishingCommits), which waits
// for the stage, while the function holds one of locks (nil: any lock): a
// lock taken earlier in the function (in source order) and not yet released.
// A deferred unlock holds to the end. Closures are their own scope.
func (src *packageSource) heldAcrossCommits(locks map[string]bool) []string {
	publishing := src.publishingCommits()
	var hits []string
	for fn, decl := range src.decls {
		held := map[string]int{}
		var walk func(n ast.Node, deferred bool)
		walk = func(n ast.Node, deferred bool) {
			ast.Inspect(n, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.FuncLit:
					return false
				case *ast.DeferStmt:
					walk(n.Call, true)
					return false
				case *ast.CallExpr:
					callee := src.callee(n)
					if callee == nil {
						return true
					}
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
						switch callee.Name() {
						case "Lock", "RLock":
							if !deferred {
								held[src.lockName(sel.X)]++
							}
							return true
						case "Unlock", "RUnlock":
							if !deferred {
								held[src.lockName(sel.X)]--
							}
							return true
						}
					}
					if publishing[callee] {
						for lock, count := range held {
							if count > 0 && (locks == nil || locks[lock]) {
								hits = append(hits, fmt.Sprintf("%s holds %s across %s", fn.Name(), lock, callee.Name()))
							}
						}
					}
				}
				return true
			})
		}
		walk(decl.Body, false)
		_ = fn
	}
	sort.Strings(hits)
	return slices.Compact(hits)
}

// locksReached returns every lock fn can take, transitively.
func (src *packageSource) locksReached(fn *types.Func, memo map[*types.Func]map[string]bool) map[string]bool {
	if got, done := memo[fn]; done {
		return got
	}
	got := map[string]bool{}
	memo[fn] = got // a cycle sees the partial set
	if fn.Name() == "waitForUniqueConstraintLock" {
		got[uniqueKeyLockWait] = true
	}
	decl, own := src.decls[fn]
	if !own {
		return got
	}
	stored := storedClosures(decl.Body)
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		if lit, ok := n.(*ast.FuncLit); ok && stored[lit] {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		callee := src.callee(call)
		if callee == nil {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (callee.Name() == "Lock" || callee.Name() == "RLock") {
			got[src.lockName(sel.X)] = true
			return true
		}
		for lock := range src.locksReached(callee, memo) {
			got[lock] = true
		}
		return true
	})
	return got
}

// stageLockOrder finds, in every function, a call made while holding one of
// stageLocks that can take one of committerLocks: a goroutine that waits for
// a lock a committer holds while holding a lock the stage needs.
func (src *packageSource) stageLockOrder(stageLocks, committerLocks map[string]bool) []string {
	memo := map[*types.Func]map[string]bool{}
	var hits []string
	for fn, decl := range src.decls {
		held := map[string]int{}
		var walk func(n ast.Node, deferred bool)
		walk = func(n ast.Node, deferred bool) {
			ast.Inspect(n, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.FuncLit:
					return false
				case *ast.DeferStmt:
					walk(n.Call, true)
					return false
				case *ast.CallExpr:
					callee := src.callee(n)
					if callee == nil {
						return true
					}
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
						switch callee.Name() {
						case "Lock", "RLock":
							lock := src.lockName(sel.X)
							if !deferred {
								for h, count := range held {
									if count > 0 && stageLocks[h] && committerLocks[lock] {
										hits = append(hits, fmt.Sprintf("%s holds %s while taking %s", fn.Name(), h, lock))
									}
								}
								held[lock]++
							}
							return true
						case "Unlock", "RUnlock":
							if !deferred {
								held[src.lockName(sel.X)]--
							}
							return true
						}
					}
					for h, count := range held {
						if count <= 0 || !stageLocks[h] {
							continue
						}
						for lock := range src.locksReached(callee, memo) {
							if committerLocks[lock] {
								hits = append(hits, fmt.Sprintf("%s holds %s while %s takes %s", fn.Name(), h, callee.Name(), lock))
							}
						}
					}
				}
				return true
			})
		}
		walk(decl.Body, false)
	}
	sort.Strings(hits)
	return slices.Compact(hits)
}

func isConversion(info *types.Info, call *ast.CallExpr) bool {
	tv, ok := info.Types[call.Fun]
	return ok && tv.IsType()
}

func exprString(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return exprString(e.X) + "." + e.Sel.Name
	case *ast.CallExpr:
		return exprString(e.Fun) + "()"
	case *ast.IndexExpr:
		return exprString(e.X) + "[]"
	case *ast.ParenExpr:
		return exprString(e.X)
	}
	return "?"
}

func TestPublicationStageBoundary(t *testing.T) {
	src := checkPackageSource(t)
	roots := src.stageRoots()

	var paths []string
	var all []ast.Node
	for path, nodes := range roots {
		paths = append(paths, path)
		all = append(all, nodes...)
	}
	sort.Strings(paths)
	t.Logf("publishing paths: %s", strings.Join(paths, ", "))
	for _, path := range paths {
		_, known := publishingPaths[path]
		assert.True(t, known, "publishing path %s has no case in the publication matrix (publishingPaths)", path)
	}
	for path := range publishingPaths {
		_, found := roots[path]
		assert.True(t, found, "publishingPaths lists %s, which no longer hands the stage a publication", path)
	}

	reached, locks, dynamic, via := src.reach(all)
	var names []string
	for name := range reached {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Logf("functions a publication reaches (%d): %s", len(names), strings.Join(names, ", "))
	sort.Strings(dynamic)
	t.Logf("calls of function values inside publications (%d): %s", len(dynamic), strings.Join(dynamic, "; "))
	for _, name := range commitFunctions {
		assert.False(t, reached[name], "a publication reaches %s: the stage could wait on itself (%s)", name, chain(via, name))
	}

	var lockNames []string
	for lock := range locks {
		lockNames = append(lockNames, lock)
	}
	sort.Strings(lockNames)
	for _, lock := range lockNames {
		t.Logf("stage lock %-40s taken in %s", lock, strings.Join(locks[lock], ", "))
	}
	lockSet := map[string]bool{}
	for _, lock := range lockNames {
		lockSet[lock] = true
	}
	// The other half of the deadlock analysis: a function that holds a stage
	// lock across a commit. Such a commit must carry no publication (it then
	// skips the stage) or the stage could wait for that lock.
	held := src.heldAcrossCommits(lockSet)
	for _, hit := range held {
		t.Logf("held across a commit: %s", hit)
	}
	// Every lock any function holds across a commit (a committer may hold it
	// while it waits for the stage), and any lock order from a stage lock to
	// one of them.
	committerLocks := map[string]bool{}
	for _, lock := range knownCommitterLocks {
		committerLocks[lock] = true
	}
	allHeld := src.heldAcrossCommits(nil)
	for _, hit := range allHeld {
		t.Logf("committer holds: %s", hit)
		if _, rest, ok := strings.Cut(hit, " holds "); ok {
			lock, _, _ := strings.Cut(rest, " across ")
			committerLocks[lock] = true
		}
	}
	order := src.stageLockOrder(lockSet, committerLocks)
	for _, hit := range order {
		t.Logf("stage lock order: %s", hit)
		_, known := lockOrderReviewed[hit]
		assert.True(t, known, "%s: a goroutine holding a stage lock waits for a lock a committer may hold; classify it in lockOrderReviewed", hit)
	}
	for _, lock := range lockNames {
		_, known := stageLocks[lock]
		assert.True(t, known, "a publication takes %s, which stageLocks doesn't classify: add its consumer, a matrix case and the deadlock analysis", lock)
	}
	for _, hit := range held {
		_, known := heldAcrossCommit[hit]
		assert.True(t, known, "%s: classify it in heldAcrossCommit (its commit must carry no publication)", hit)
	}

	// Every operation type a commit carries is covered.
	var ops []string
	for _, file := range src.files {
		if src.fset.Position(file.Pos()).Filename != "transaction.go" {
			continue
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				if ident, ok := value.Type.(*ast.Ident); !ok || ident.Name != "OperationType" {
					continue
				}
				for _, name := range value.Names {
					ops = append(ops, name.Name)
				}
			}
		}
	}
	sort.Strings(ops)
	t.Logf("operation types: %s", strings.Join(ops, ", "))
	require.NotEmpty(t, ops)
	for _, op := range ops {
		_, known := operationCoverage[op]
		assert.True(t, known, "operation type %s has no matrix coverage (operationCoverage)", op)
	}
	require.NotEmpty(t, publicationMatrixNodeConsumers)
	require.NotEmpty(t, publicationMatrixEdgeConsumers)
}
