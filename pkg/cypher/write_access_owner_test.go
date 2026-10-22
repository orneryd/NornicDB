package cypher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A composite statement opens a write on a constituent where the write
// runs, and only through FabricTransaction.ClaimWrite, keyed by the
// constituent's identity: a local statement that writes (fabricScope.open),
// a local write that starts an implicit transaction (fabricScope.openWrite)
// and a remote write session (RemoteFragmentExecutor.participant). Each
// write-open primitive has one opener, and every way a statement runs or
// writes storage is inventoried. The guard type-checks pkg/fabric and
// pkg/cypher and reports any other way: every reference is resolved to its
// object, and every function is named with its receiver.
const (
	fabricPath  = "github.com/orneryd/nornicdb/pkg/fabric"
	cypherPath  = "github.com/orneryd/nornicdb/pkg/cypher"
	storagePath = "github.com/orneryd/nornicdb/pkg/storage"
)

// writeAccessClaimSites are the functions that may call
// FabricTransaction.ClaimWrite: the conditions of the if statements around
// the call, innermost first, and statements that must come before it.
var writeAccessClaimSites = map[string]struct {
	conditions []string
	before     []string
}{
	cypherPath + ".fabricScope.open":                   {conditions: []string{"fragmentWrites(statement, !local)"}, before: []string{"participant, local := s.participant()"}},
	cypherPath + ".fabricScope.openWrite":              {before: []string{"participant, local := s.participant()"}},
	fabricPath + ".RemoteFragmentExecutor.participant": {conditions: []string{"writes"}, before: []string{"fabricTx, ok := FabricTransactionFromContext(ctx)"}},
}

// writeAccessOpeners are the write-open primitives and what reaches them,
// each with the only functions that may refer to it: the remote sessions,
// the local sub-transaction a statement joins, an implicit or explicit
// transaction's begin, the database a USE selects, and a fragment's
// dispatch to its participant.
var writeAccessOpeners = map[string][]string{
	fabricPath + ".FabricTransaction.ClaimWrite":                    {cypherPath + ".fabricScope.open", cypherPath + ".fabricScope.openWrite", fabricPath + ".RemoteFragmentExecutor.participant"},
	storagePath + ".RemoteEngine.BeginCypherTx":                     {fabricPath + ".RemoteFragmentExecutor.participant"},
	storagePath + ".RemoteEngine.BeginReadCypherTx":                 {fabricPath + ".RemoteFragmentExecutor.participant"},
	fabricPath + ".RemoteFragmentExecutor.participant":              {fabricPath + ".RemoteFragmentExecutor.executeRemoteQuery"},
	cypherPath + ".cypherFabricExecutor.ensureLocalShardTxExecutor": {cypherPath + ".fabricScope.joined"},
	cypherPath + ".fabricScope.joined":                              {cypherPath + ".fabricScope.executorFor", cypherPath + ".fabricScope.fragmentExecutor", cypherPath + ".fabricScope.openWrite"},
	cypherPath + ".fabricScope.openWrite":                           {cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback"},
	cypherPath + ".fabricScope.fragmentExecutor":                    {cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord"},
	cypherPath + ".fabricScope.executorFor":                         {cypherPath + ".StorageExecutor.runScopedStatement", cypherPath + ".StorageExecutor.useDatabase"},
	cypherPath + ".beginTransactionSnapshot":                        {cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback", cypherPath + ".StorageExecutor.handleBegin"},
	cypherPath + ".StorageExecutor.useDatabase":                     {cypherPath + ".StorageExecutor.executeOnDatabase", cypherPath + ".StorageExecutor.pipelineApplyCallSubqueryWithMetadata"},
	cypherPath + ".StorageExecutor.scopedExecutorForUse":            {cypherPath + ".StorageExecutor.useDatabase", cypherPath + ".StorageExecutor.runFabricStatement", cypherPath + ".StorageExecutor.executeShellCommand"},
	fabricPath + ".LocalFragmentExecutor.ExecuteWithRecord":         {fabricPath + ".FabricExecutor.dispatchExec", fabricPath + ".LocalFragmentExecutor.Execute", fabricPath + ".LocalFragmentExecutor.ExecuteWithRecordRows"},
	fabricPath + ".RemoteFragmentExecutor.Execute":                  {fabricPath + ".FabricExecutor.dispatchExec", fabricPath + ".RemoteFragmentExecutor.ExecuteRows"},
}

// writeAccessStatementEntries run a statement's text on an executor. Only
// the functions in writeAccessStatementCallers call them: the parts of a
// statement's own execution. A statement a procedure was given runs through
// executeUserStatement or Execute, which route it (runScopedStatement).
var writeAccessStatementEntries = map[string]bool{
	cypherPath + ".StorageExecutor.execute": true, cypherPath + ".StorageExecutor.executeRouted": true,
	cypherPath + ".StorageExecutor.executeWithoutTransaction": true, cypherPath + ".StorageExecutor.executeQueryAgainstStorage": true,
	cypherPath + ".StorageExecutor.executeInternal": true, cypherPath + ".StorageExecutor.executeImplicitAsync": true,
	cypherPath + ".StorageExecutor.executeWithImplicitTransaction": true, cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback": true,
	cypherPath + ".StorageExecutor.executeInTransaction": true, cypherPath + ".StorageExecutor.inTransactionStorage": true,
	cypherPath + ".StorageExecutor.executeOnDatabase": true, cypherPath + ".StorageExecutor.executeViaFabric": true,
	cypherPath + ".StorageExecutor.executeViaFabricWithTx": true, cypherPath + ".StorageExecutor.executeViaPreparedFabricWithTx": true,
	cypherPath + ".StorageExecutor.executeNestedFabric": true, cypherPath + ".StorageExecutor.runFabricStatement": true,
	cypherPath + ".StorageExecutor.runScopedStatement": true,
}

// writeAccessStorageWrites are the storage engine's write methods, by the
// interface the executor writes through.
var writeAccessStorageWrites = map[string]bool{
	storagePath + ".Engine.CreateNode": true, storagePath + ".Engine.UpdateNode": true, storagePath + ".Engine.DeleteNode": true,
	storagePath + ".Engine.CreateEdge": true, storagePath + ".Engine.UpdateEdge": true, storagePath + ".Engine.DeleteEdge": true,
	storagePath + ".Engine.BulkCreateNodes": true, storagePath + ".Engine.BulkCreateEdges": true, storagePath + ".Engine.BulkDeleteNodes": true,
	storagePath + ".Engine.BulkDeleteEdges": true, storagePath + ".Engine.DeleteByPrefix": true,
}

// writeAccessOwnerPins are statements an owner function must keep, in
// order: what makes its decision, or routes a statement or write through it.
var writeAccessOwnerPins = map[string][]string{
	cypherPath + ".statementWrites": {
		"query = classificationText(query)",
		"requirements := permissionRequirements(query)",
		"if requirements.Write || requirements.Schema {",
		"if !known || remote && (procedure.User || procedure.Spec.ExecutesStatements) {",
	},
	cypherPath + ".permissionRequirements": {`requirements.Write = keywords["CREATE"] || keywords["DELETE"] || keywords["SET"] ||`},
	cypherPath + ".calledProcedureNames":   {"opts.SkipParens = false", "opts.SkipBrackets = false", "opts.SkipBraces = false"},
	cypherPath + ".accessModeMemo.decide":  {"writes = statementWrites(query, remote)", "m.modes[key] = writes"},
	cypherPath + ".fabricScope.open": {
		"participant, local := s.participant()",
		"if fragmentWrites(statement, !local) {",
		"join = local && !inTransaction && s.joins && (!s.executor.autoCommit || s.tx.WriteShard() == participant)",
	},
	cypherPath + ".fabricScope.openWrite": {"if !local || !s.joins {", "return s.joined(ctx, participant)"},
	cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord": {
		"scope := &fabricScope{executor: c, tx: tx, catalog: c.catalog, dbName: dbName, engine: engine,",
		"participant, participantCtx, release, err := scope.fragmentExecutor(",
	},
	cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback": {
		"if scope := fabricScopeFrom(ctx); scope != nil {",
		"tx, err := beginTransactionSnapshot(txEngine)",
	},
	cypherPath + ".StorageExecutor.useDatabase": {
		"scope := fabricScopeFrom(ctx)",
		"return selected.executorFor(withFabricScope(ctx, selected), scopedExec, query)",
	},
	cypherPath + ".StorageExecutor.executeOnDatabase": {
		"scopedExec, ctx, release, err := e.useDatabase(ctx, db, query)",
		"result, err = scopedExec.executeViaFabric(ctx, query, params)",
	},
	cypherPath + ".StorageExecutor.executeViaFabric": {
		"if scope := fabricScopeFrom(ctx); scope != nil {",
		"return e.executeNestedFabric(ctx, cypher, params, scope)",
	},
	cypherPath + ".StorageExecutor.executeUserStatement": {
		"result, err = e.executeViaFabric(ctx, statement, params)",
		"result, err = e.runScopedStatement(ctx, statement, func(",
	},
	cypherPath + ".StorageExecutor.Execute":            {"return e.runScopedStatement(ctx, cypher, func("},
	cypherPath + ".StorageExecutor.runScopedStatement": {"target, ctx, release, err := scope.executorFor(ctx, e, statement)", "return run(ctx, target)"},
	cypherPath + ".ensureBuiltInProceduresRegistered": {
		`registerStatementProcedureLiteral("apoc.cypher.run",`, `registerStatementProcedureLiteral("apoc.cypher.doitall",`,
		`registerStatementProcedureLiteral("apoc.export.json.query",`, `registerStatementProcedureLiteral("apoc.export.csv.query",`,
	},
	fabricPath + ".decideAccessMode":                          {"if mode == nil || *mode == nil {\n\treturn true\n}", "return (*mode)(query, remote)"},
	fabricPath + ".fragmentMayWrite":                          {"return decideAccessMode(query, true)"},
	fabricPath + ".FabricExecutor.dispatchExec":               {"loc, execParams, ctx, err := e.prepareExecDispatch(ctx, tx, f, params)"},
	fabricPath + ".RemoteFragmentExecutor.executeRemoteQuery": {"handle, err := r.participant(ctx, sub, engine, decideAccessMode(query, true))"},
	fabricPath + ".RemoteFragmentExecutor.participant": {
		"if writes {\n\tif _, err := fabricTx.ClaimWrite(sub.ShardName); err != nil {",
		"writes = writes || fabricTx.WriteShard() == sub.ShardName",
		"open := engine.BeginReadCypherTx",
		"commitFn := func(_ *SubTransaction) error { return handle.endWithin(terminalCtx, handle.write) }",
	},
	fabricPath + ".FabricTransaction.Commit": {"if name != t.writeShard {", "if writer := t.subTxns[t.writeShard]; writer != nil && writer.State == \"open\" {"},
}

// writeAccessStatementCallers are the functions that run a statement with a
// statement entry (writeAccessStatementEntries): Execute and
// executeUserStatement, which route a statement a procedure was given, and
// the parts of a statement's own execution (a fragment, a UNION branch, a
// subquery body, a CALL tail, PROFILE, a transaction script, IN
// TRANSACTIONS batches, a shell parameter).
var writeAccessStatementCallers = map[string]bool{
	cypherPath + ".StorageExecutor.Execute":                               true,
	cypherPath + ".StorageExecutor.evaluateParamMapExpression":            true,
	cypherPath + ".StorageExecutor.executeCallTailSingleRow":              true,
	cypherPath + ".StorageExecutor.executeCaseRollbackTransactionScript":  true,
	cypherPath + ".StorageExecutor.executeCorrelatedSubqueryBody":         true,
	cypherPath + ".StorageExecutor.executeProfile":                        true,
	cypherPath + ".StorageExecutor.executeSimpleTransactionScript":        true,
	cypherPath + ".StorageExecutor.executeUnion":                          true,
	cypherPath + ".StorageExecutor.executeUserStatement":                  true,
	cypherPath + ".StorageExecutor.pipelineApplyCallSubqueryWithMetadata": true,
	cypherPath + ".StorageExecutor.resolveCorrelatedImportValue":          true,
	cypherPath + ".StorageExecutor.seedNodesFromOuterMatch":               true,
	cypherPath + ".StorageExecutor.tryExecuteCorrelatedBatchedLookup":     true,
	cypherPath + ".cypherFabricExecutor.executeFragmentQuery":             true,
}

// writeAccessStorageWriters are the functions that write storage through the
// engine (writeAccessStorageWrites): the write clauses, a WRITE procedure
// (apoc.import.json), procedure DDL and the inline embedding of a write.
var writeAccessStorageWriters = map[string]bool{
	cypherPath + ".StorageExecutor.applyCreatePlan":                      true,
	cypherPath + ".StorageExecutor.applyInlineEmbeddingMutations":        true,
	cypherPath + ".StorageExecutor.callApocImportJson":                   true,
	cypherPath + ".StorageExecutor.dropPersistedProcedure":               true,
	cypherPath + ".StorageExecutor.executeMergeWithContext":              true,
	cypherPath + ".StorageExecutor.installPersistedProcedure":            true,
	cypherPath + ".StorageExecutor.migrateProcedureCatalogIdentity":      true,
	cypherPath + ".StorageExecutor.pipelineApplyDelete":                  true,
	cypherPath + ".StorageExecutor.tryExecuteBoundRelationshipDelete":    true,
	cypherPath + ".StorageExecutor.tryExecuteDeleteWithWithLimitHotPath": true,
	cypherPath + ".createRelationshipForMerge":                           true,
	cypherPath + ".updateMutatedEdge":                                    true,
	cypherPath + ".updateMutatedNode":                                    true,
}

// writeAccessPkg is a package for the guard (info nil: parsed only).
type writeAccessPkg struct {
	path  string
	fset  *token.FileSet
	files []*ast.File
	info  *types.Info
}

type writeAccessGuard struct {
	violations []string
}

func (g *writeAccessGuard) report(pkg *writeAccessPkg, pos token.Pos, format string, args ...interface{}) {
	g.violations = append(g.violations, fmt.Sprintf("%s: %s", pkg.fset.Position(pos), fmt.Sprintf(format, args...)))
}

// funcKey names a function declaration as the guard's tables do: the
// package path, the receiver type for a method, then the name.
func funcKey(pkg string, fn *ast.FuncDecl) string {
	if fn.Recv != nil && len(fn.Recv.List) > 0 {
		recv := fn.Recv.List[0].Type
		if star, ok := recv.(*ast.StarExpr); ok {
			recv = star.X
		}
		if ident, ok := recv.(*ast.Ident); ok {
			return pkg + "." + ident.Name + "." + fn.Name.Name
		}
	}
	return pkg + "." + fn.Name.Name
}

func nodeText(fset *token.FileSet, node ast.Node) string {
	var buf bytes.Buffer
	_ = printer.Fprint(&buf, fset, node)
	return buf.String()
}

// objectName is pkgpath.Type.Name for a method, pkgpath.field.Name for a
// field, pkgpath.Name for a package-level object.
func objectName(obj types.Object) string {
	if obj == nil || obj.Pkg() == nil {
		return ""
	}
	switch o := obj.(type) {
	case *types.Func:
		if sig, ok := o.Type().(*types.Signature); ok && sig.Recv() != nil {
			return o.Pkg().Path() + "." + namedTypeName(sig.Recv().Type()) + "." + o.Name()
		}
	case *types.Var:
		if o.IsField() {
			return o.Pkg().Path() + ".field." + o.Name()
		}
		if o.Parent() != o.Pkg().Scope() {
			return ""
		}
	}
	return obj.Pkg().Path() + "." + obj.Name()
}

func namedTypeName(t types.Type) string {
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
	}
	if n, ok := t.(*types.Named); ok {
		return n.Obj().Name()
	}
	return ""
}

func typeName(t types.Type) string {
	if t == nil {
		return ""
	}
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
	}
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil {
		return n.Obj().Pkg().Path() + "." + n.Obj().Name()
	}
	return ""
}

// check runs every rule over pkgs.
func (g *writeAccessGuard) check(pkgs []*writeAccessPkg) {
	for _, pkg := range pkgs {
		for _, file := range pkg.files {
			var stack []ast.Node
			ast.Inspect(file, func(node ast.Node) bool {
				if node == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				stack = append(stack, node)
				var fn *ast.FuncDecl
				for _, outer := range stack {
					if decl, ok := outer.(*ast.FuncDecl); ok {
						fn = decl
					}
				}
				if fn == nil {
					return true
				}
				key := funcKey(pkg.path, fn)
				switch n := node.(type) {
				case *ast.Ident:
					g.checkReference(pkg, key, n, stack)
				case *ast.CompositeLit:
					if pkg.info == nil {
						return true
					}
					switch built := typeName(pkg.info.TypeOf(n)); {
					case built == fabricPath+".SubTransaction" && key != fabricPath+".FabricTransaction.GetOrOpen":
						g.report(pkg, n.Pos(), "SubTransaction built outside GetOrOpen (in %s)", key)
					case built == fabricPath+".FabricTransaction" && key != fabricPath+".NewFabricTransaction":
						g.report(pkg, n.Pos(), "FabricTransaction built outside NewFabricTransaction (in %s)", key)
					}
				}
				return true
			})
		}
		g.checkPins(pkg)
	}
}

// checkReference applies the rules about what an identifier refers to.
// stack ends with the identifier.
func (g *writeAccessGuard) checkReference(pkg *writeAccessPkg, key string, ident *ast.Ident, stack []ast.Node) {
	if pkg.info == nil {
		return
	}
	name := objectName(pkg.info.Uses[ident])
	if name == "" {
		return
	}
	// The expression the identifier names (the selector when it is one's
	// Sel) and what holds that expression.
	at := len(stack) - 1
	if sel, ok := stack[at-1].(*ast.SelectorExpr); ok && sel.Sel == ident {
		at--
	}
	expr, holder := stack[at], stack[at-1]
	call, called := holder.(*ast.CallExpr)
	called = called && call.Fun == expr
	if openers, primitive := writeAccessOpeners[name]; primitive {
		allowed := false
		for _, opener := range openers {
			allowed = allowed || opener == key
		}
		if !allowed {
			g.report(pkg, ident.Pos(), "%s reached outside its opener (in %s)", strings.TrimPrefix(name, pkg.path+"."), key)
			return
		}
		if !called && name != storagePath+".RemoteEngine.BeginCypherTx" && name != storagePath+".RemoteEngine.BeginReadCypherTx" {
			g.report(pkg, ident.Pos(), "%s used other than as a call (in %s)", strings.TrimPrefix(name, pkg.path+"."), key)
			return
		}
	}
	switch {
	case name == fabricPath+".FabricTransaction.GetOrOpen":
		if !called {
			g.report(pkg, ident.Pos(), "GetOrOpen used other than as a call (in %s)", key)
			return
		}
		want := "false"
		if key == fabricPath+".FabricTransaction.ClaimWrite" {
			want = "true"
		}
		flag, ok := call.Args[1].(*ast.Ident)
		if !ok || flag.Name != want || pkg.info.Uses[flag] != types.Universe.Lookup(want) {
			g.report(pkg, call.Pos(), "GetOrOpen's write flag is not %s (in %s)", want, key)
		}
	case name == fabricPath+".field.writeShard", name == fabricPath+".field.IsWrite":
		if key != fabricPath+".FabricTransaction.GetOrOpen" && isAssigned(expr, holder) {
			g.report(pkg, ident.Pos(), "%s set outside GetOrOpen (in %s)", ident.Name, key)
		}
	case name == fabricPath+".FabricTransaction.ClaimWrite":
		g.checkClaim(pkg, key, call, stack[:at-1])
	case writeAccessStatementEntries[name] && !writeAccessStatementEntries[key] && !writeAccessStatementCallers[key]:
		g.report(pkg, ident.Pos(), "%s runs a statement (%s) and is not in the inventory of a statement's own parts; a statement a procedure was given runs through executeUserStatement or Execute", key, strings.TrimPrefix(name, cypherPath+"."))
	case writeAccessStorageWrites[name] && !writeAccessStorageWriters[key]:
		g.report(pkg, ident.Pos(), "%s writes storage (%s) and is not in the inventory of storage writers", key, strings.TrimPrefix(name, storagePath+"."))
	}
}

func isAssigned(expr ast.Node, holder ast.Node) bool {
	switch p := holder.(type) {
	case *ast.AssignStmt:
		for _, lhs := range p.Lhs {
			if lhs == expr {
				return true
			}
		}
	case *ast.IncDecStmt:
		return p.X == expr
	case *ast.UnaryExpr:
		return p.Op == token.AND && p.X == expr
	case *ast.KeyValueExpr:
		return p.Key == expr
	}
	return false
}

// checkClaim requires a claim's error to decide the site's outcome and the
// claim to stand under exactly the site's conditions, after the statements
// the site requires. stack ends with what holds the call.
func (g *writeAccessGuard) checkClaim(pkg *writeAccessPkg, key string, call *ast.CallExpr, stack []ast.Node) {
	site := writeAccessClaimSites[key]
	holder := stack[len(stack)-1]
	checked := false
	var claimIf *ast.IfStmt
	switch h := holder.(type) {
	case *ast.ReturnStmt:
		checked = true
	case *ast.AssignStmt:
		if stmt, ok := stack[len(stack)-2].(*ast.IfStmt); ok && stmt.Init == h && len(h.Rhs) == 1 && nodeText(pkg.fset, stmt.Cond) == "err != nil" {
			checked, claimIf = true, stmt
		}
	}
	if !checked {
		g.report(pkg, call.Pos(), "ClaimWrite's error is not the outcome (in %s)", key)
	}
	var conditions []string
	var fn *ast.FuncDecl
	for i := len(stack) - 1; i >= 0; i-- {
		switch n := stack[i].(type) {
		case *ast.IfStmt:
			if n != claimIf {
				conditions = append(conditions, nodeText(pkg.fset, n.Cond))
			}
		case *ast.FuncLit:
			g.report(pkg, call.Pos(), "ClaimWrite called from a function literal (in %s)", key)
		case *ast.FuncDecl:
			fn = n
		}
	}
	if strings.Join(conditions, " | ") != strings.Join(site.conditions, " | ") {
		g.report(pkg, call.Pos(), "ClaimWrite under conditions %q, want %q (in %s)", conditions, site.conditions, key)
	}
	if fn != nil {
		for _, want := range site.before {
			if found := statementPos(pkg, fn, want); found == token.NoPos || found > call.Pos() {
				g.report(pkg, call.Pos(), "ClaimWrite without %q before it (in %s)", want, key)
			}
		}
	}
}

// statementPos is the position of the first statement of fn whose printed
// form starts with text.
func statementPos(pkg *writeAccessPkg, fn *ast.FuncDecl, text string) token.Pos {
	at := token.NoPos
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if at != token.NoPos {
			return false
		}
		if stmt, ok := node.(ast.Stmt); ok {
			if _, block := stmt.(*ast.BlockStmt); !block && strings.HasPrefix(nodeText(pkg.fset, stmt), text) {
				at = stmt.Pos()
				return false
			}
		}
		return true
	})
	return at
}

// checkPins requires every owner function of pkg to keep its pinned
// statements, in order, and the access mode to be installed once, from
// fragmentWrites.
func (g *writeAccessGuard) checkPins(pkg *writeAccessPkg) {
	seen := map[string]bool{}
	installed := 0
	for _, file := range pkg.files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := funcKey(pkg.path, fn)
			if pins, pinned := writeAccessOwnerPins[key]; pinned {
				seen[key] = true
				last := token.NoPos
				for _, want := range pins {
					at := statementPos(pkg, fn, want)
					if at == token.NoPos || at < last {
						g.report(pkg, fn.Pos(), "%s no longer has %q in place", key, want)
						continue
					}
					last = at
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SetAccessMode" {
					installed++
					if text := nodeText(pkg.fset, call); text != "fabric.SetAccessMode(fragmentWrites)" || fn.Name.Name != "init" {
						g.report(pkg, call.Pos(), "access mode installed other than from fragmentWrites at init: %s", text)
					}
				}
				return true
			})
		}
	}
	for key := range writeAccessOwnerPins {
		if strings.HasPrefix(key, pkg.path+".") && !seen[key] {
			g.violations = append(g.violations, fmt.Sprintf("owner %s is gone; re-establish the inventory", key))
		}
	}
	if pkg.path == cypherPath && installed != 1 {
		g.violations = append(g.violations, fmt.Sprintf("access mode installed %d times, want once", installed))
	}
}

// writeAccessListed is a package as go list describes it.
type writeAccessListed struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
	ImportMap  map[string]string
}

// writeAccessImporter type-checks dependencies from source, exported API
// only: the guard needs their types, not their bodies.
type writeAccessImporter struct {
	fset   *token.FileSet
	listed map[string]*writeAccessListed
	loaded map[string]*types.Package
}

func (imp *writeAccessImporter) Import(path string) (*types.Package, error) {
	if path == "unsafe" {
		return types.Unsafe, nil
	}
	if pkg := imp.loaded[path]; pkg != nil {
		return pkg, nil
	}
	listed := imp.listed[path]
	if listed == nil {
		return nil, fmt.Errorf("%s: not in go list -deps", path)
	}
	files, err := parseWriteAccessFiles(imp.fset, listed, nil)
	if err != nil {
		return nil, err
	}
	conf := types.Config{IgnoreFuncBodies: true, FakeImportC: true, Importer: imp.from(listed), Error: func(error) {}}
	pkg, _ := conf.Check(path, imp.fset, files, nil)
	imp.loaded[path] = pkg
	return pkg, nil
}

func (imp *writeAccessImporter) from(listed *writeAccessListed) types.Importer {
	return writeAccessImportFunc(func(path string) (*types.Package, error) {
		if mapped, ok := listed.ImportMap[path]; ok {
			path = mapped
		}
		return imp.Import(path)
	})
}

type writeAccessImportFunc func(path string) (*types.Package, error)

func (f writeAccessImportFunc) Import(path string) (*types.Package, error) { return f(path) }

// parseWriteAccessFiles parses a listed package, with replace giving a file
// (by base name) other source, or adding a zz_fixture_ file.
func parseWriteAccessFiles(fset *token.FileSet, listed *writeAccessListed, replace map[string]string) ([]*ast.File, error) {
	var files []*ast.File
	for _, name := range append(append([]string{}, listed.GoFiles...), listed.CgoFiles...) {
		var src interface{}
		if text, ok := replace[name]; ok {
			src = text
		}
		parsed, err := parser.ParseFile(fset, filepath.Join(listed.Dir, name), src, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, parsed)
	}
	for name, text := range replace {
		if strings.HasPrefix(name, "zz_fixture_") {
			parsed, err := parser.ParseFile(fset, name, text, 0)
			if err != nil {
				return nil, err
			}
			files = append(files, parsed)
		}
	}
	return files, nil
}

// writeAccessLoader loads pkg/fabric and pkg/cypher for the guard.
type writeAccessLoader struct {
	t      *testing.T
	imp    *writeAccessImporter
	fabric *writeAccessListed
	cypher *writeAccessListed
}

func newWriteAccessLoader(t *testing.T) *writeAccessLoader {
	t.Helper()
	listing, err := exec.Command("go", "list", "-deps", "-json=ImportPath,Dir,GoFiles,CgoFiles,ImportMap", ".").Output()
	require.NoError(t, err)
	imp := &writeAccessImporter{fset: token.NewFileSet(), listed: map[string]*writeAccessListed{}, loaded: map[string]*types.Package{}}
	for dec := json.NewDecoder(bytes.NewReader(listing)); dec.More(); {
		listed := &writeAccessListed{}
		require.NoError(t, dec.Decode(listed))
		imp.listed[listed.ImportPath] = listed
	}
	loader := &writeAccessLoader{t: t, imp: imp, fabric: imp.listed[fabricPath], cypher: imp.listed[cypherPath]}
	require.NotNil(t, loader.fabric)
	require.NotNil(t, loader.cypher)
	return loader
}

// typed type-checks listed with bodies, with replace's sources, and
// requires it to compile.
func (l *writeAccessLoader) typed(listed *writeAccessListed, replace map[string]string) *writeAccessPkg {
	l.t.Helper()
	files, err := parseWriteAccessFiles(l.imp.fset, listed, replace)
	require.NoError(l.t, err)
	var errs []error
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: l.imp.from(listed), FakeImportC: true, Error: func(err error) { errs = append(errs, err) }}
	_, _ = conf.Check(listed.ImportPath, l.imp.fset, files, info)
	require.Empty(l.t, errs, "type-check %s", listed.ImportPath)
	return &writeAccessPkg{path: listed.ImportPath, fset: l.imp.fset, files: files, info: info}
}

// typedOrNil is typed, or nil when replace's sources don't compile (a
// mutation that leaves an unused variable): the rules that read source alone
// still apply to them.
func (l *writeAccessLoader) typedOrNil(listed *writeAccessListed, replace map[string]string) *writeAccessPkg {
	l.t.Helper()
	files, err := parseWriteAccessFiles(l.imp.fset, listed, replace)
	require.NoError(l.t, err)
	compiles := true
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: l.imp.from(listed), FakeImportC: true, Error: func(error) { compiles = false }}
	_, _ = conf.Check(listed.ImportPath, l.imp.fset, files, info)
	if !compiles {
		return nil
	}
	return &writeAccessPkg{path: listed.ImportPath, fset: l.imp.fset, files: files, info: info}
}

// parsed parses listed only, with replace's sources, for the rules that read
// source alone.
func (l *writeAccessLoader) parsed(listed *writeAccessListed, replace map[string]string) *writeAccessPkg {
	l.t.Helper()
	files, err := parseWriteAccessFiles(l.imp.fset, listed, replace)
	require.NoError(l.t, err)
	return &writeAccessPkg{path: listed.ImportPath, fset: l.imp.fset, files: files}
}

func writeAccessViolations(pkgs ...*writeAccessPkg) []string {
	g := &writeAccessGuard{}
	g.check(pkgs)
	sort.Strings(g.violations)
	return g.violations
}

func TestWriteAccessHasOneOwner(t *testing.T) {
	loader := newWriteAccessLoader(t)
	require.Empty(t, writeAccessViolations(loader.typed(loader.fabric, nil), loader.typed(loader.cypher, nil)))
}

// Each way around the owner the guard exists for is reported: the bypass
// shapes of the earlier reviews (method values, writes split into helpers,
// ignored claims, claims without their decision, a neutered owner, a
// statement run past the router by any entry, a method of another type
// named like a statement part, a direct storage write, a transaction built
// as a literal) and each write-open primitive reached past its opener.
func TestWriteAccessOwnerGuardReportsBypasses(t *testing.T) {
	loader := newWriteAccessLoader(t)
	fixtures := []struct{ name, pkg, src, want string }{
		{"method_value_get_or_open", "fabric", "func bypassOpen(tx *FabricTransaction) {\n\topen := tx.GetOrOpen\n\t_, _ = open(\"p\", false)\n}", "GetOrOpen used other than as a call"},
		{"aliased_transaction_opens_writer", "fabric", "func bypassAlias(tx *FabricTransaction) {\n\tt := tx\n\t_, _ = t.GetOrOpen(\"p\", true)\n}", "write flag is not false (in " + fabricPath + ".bypassAlias)"},
		{"helper_computes_write_flag", "fabric", "func bypassHelper(tx *FabricTransaction, writes bool) {\n\t_, _ = tx.GetOrOpen(\"p\", writes)\n}", "write flag is not false (in " + fabricPath + ".bypassHelper)"},
		{"constant_expression_flag", "fabric", "func bypassNotFalse(tx *FabricTransaction) {\n\t_, _ = tx.GetOrOpen(\"p\", !false)\n}", "write flag is not false (in " + fabricPath + ".bypassNotFalse)"},
		{"write_shard_set", "fabric", "func bypassShard(tx *FabricTransaction) {\n\ttx.writeShard = \"p\"\n}", "writeShard set outside GetOrOpen"},
		{"sub_transaction_upgraded", "fabric", "func bypassUpgrade(sub *SubTransaction) {\n\tsub.IsWrite = true\n}", "IsWrite set outside GetOrOpen"},
		{"sub_transaction_built", "fabric", "func bypassBuild() *SubTransaction {\n\treturn &SubTransaction{IsWrite: true}\n}", "SubTransaction built outside GetOrOpen"},
		{"writer_via_struct_literal", "fabric", "func bypassLiteral(tx *FabricTransaction) {\n\t*tx = FabricTransaction{writeShard: \"p\", state: \"open\", subTxns: map[string]*SubTransaction{}}\n}", "FabricTransaction built outside NewFabricTransaction (in " + fabricPath + ".bypassLiteral)"},
		{"claim_from_new_site", "fabric", "func bypassClaim(tx *FabricTransaction) error {\n\t_, err := tx.ClaimWrite(\"p\")\n\treturn err\n}", "FabricTransaction.ClaimWrite reached outside its opener (in " + fabricPath + ".bypassClaim)"},
		{"remote_write_session_opened", "fabric", "func bypassSession(ctx context.Context, engine *storage.RemoteEngine) {\n\t_, _ = engine.BeginCypherTx(ctx)\n}", "RemoteEngine.BeginCypherTx reached outside its opener (in " + fabricPath + ".bypassSession)"},
		{"remote_participant_skipped", "fabric", "func (r *RemoteFragmentExecutor) bypassParticipant(ctx context.Context, sub *SubTransaction, engine *storage.RemoteEngine) {\n\t_, _ = r.participant(ctx, sub, engine, false)\n}", "RemoteFragmentExecutor.participant reached outside its opener (in " + fabricPath + ".RemoteFragmentExecutor.bypassParticipant)"},
		{"local_dispatch_bypass", "fabric", "func (e *FabricExecutor) bypassLocal(ctx context.Context, l *LocationLocal) {\n\t_, _ = e.local.ExecuteWithRecord(ctx, l, \"CREATE (n)\", nil, nil)\n}", "LocalFragmentExecutor.ExecuteWithRecord reached outside its opener (in " + fabricPath + ".FabricExecutor.bypassLocal)"},
		{"remote_executor_method_value", "fabric", "func (e *FabricExecutor) bypassRemote(ctx context.Context, l *LocationRemote) {\n\trun := e.remote.Execute\n\t_, _ = run(ctx, l, \"CREATE (n)\", nil, \"\")\n}", "RemoteFragmentExecutor.Execute reached outside its opener (in " + fabricPath + ".FabricExecutor.bypassRemote)"},
		{"method_value_claim", "cypher", "func bypassClaimValue(tx *fabric.FabricTransaction) error {\n\tclaim := tx.ClaimWrite\n\t_, err := claim(\"p\")\n\treturn err\n}", "FabricTransaction.ClaimWrite reached outside its opener (in " + cypherPath + ".bypassClaimValue)"},
		{"participant_joined_directly", "cypher", "func bypassJoin(ctx context.Context, c *cypherFabricExecutor, tx *fabric.FabricTransaction, sub *fabric.SubTransaction, engine storage.Engine) {\n\t_, _ = c.ensureLocalShardTxExecutor(ctx, tx, sub, \"db\", engine)\n}", "ensureLocalShardTxExecutor reached outside its opener (in " + cypherPath + ".bypassJoin)"},
		{"scope_joined_without_claim", "cypher", "func bypassScopeJoin(ctx context.Context, s *fabricScope) {\n\t_, _, _, _ = s.joined(ctx, \"p\")\n}", "fabricScope.joined reached outside its opener (in " + cypherPath + ".bypassScopeJoin)"},
		{"implicit_transaction_begun", "cypher", "func bypassBegin(engine TransactionCapableEngine) {\n\t_, _ = beginTransactionSnapshot(engine)\n}", "beginTransactionSnapshot reached outside its opener (in " + cypherPath + ".bypassBegin)"},
		{"database_selected_past_use", "cypher", "func (e *StorageExecutor) bypassSelect(db string) {\n\t_, _, _ = e.scopedExecutorForUse(db, \"\")\n}", "scopedExecutorForUse reached outside its opener (in " + cypherPath + ".StorageExecutor.bypassSelect)"},
		{"use_switch_reused", "cypher", "func (e *StorageExecutor) bypassUse(ctx context.Context) {\n\t_, _, _, _ = e.useDatabase(ctx, \"db\", \"CREATE (n)\")\n}", "useDatabase reached outside its opener (in " + cypherPath + ".StorageExecutor.bypassUse)"},
		{"statement_past_the_router", "cypher", "func bypassRun(ctx context.Context, e *StorageExecutor) {\n\t_, _ = e.executeInternal(ctx, \"CREATE (n)\", nil)\n}", cypherPath + ".bypassRun runs a statement (StorageExecutor.executeInternal)"},
		{"allowlisted_name_other_type", "cypher", "func (s *fabricScope) executeUnion(ctx context.Context, e *StorageExecutor, q string) {\n\t_, _ = e.executeInternal(ctx, q, nil)\n}", cypherPath + ".fabricScope.executeUnion runs a statement"},
		{"runner_execute", "cypher", "func bypassExecute(e *StorageExecutor) {\n\trun := e.execute\n\t_ = run\n}", cypherPath + ".bypassExecute runs a statement (StorageExecutor.execute)"},
		{"runner_routed", "cypher", "func bypassRouted(e *StorageExecutor) {\n\trun := e.executeRouted\n\t_ = run\n}", cypherPath + ".bypassRouted runs a statement (StorageExecutor.executeRouted)"},
		{"runner_implicit", "cypher", "func bypassImplicit(e *StorageExecutor) {\n\trun := e.executeImplicitAsync\n\t_ = run\n}", cypherPath + ".bypassImplicit runs a statement (StorageExecutor.executeImplicitAsync)"},
		{"runner_without_tx", "cypher", "func bypassWithoutTx(ctx context.Context, e *StorageExecutor, q string) {\n\t_, _ = e.executeWithoutTransaction(ctx, q, upperASCII(q))\n}", cypherPath + ".bypassWithoutTx runs a statement (StorageExecutor.executeWithoutTransaction)"},
		{"runner_against_storage", "cypher", "func bypassAgainst(ctx context.Context, e *StorageExecutor, q string) {\n\t_, _ = e.executeQueryAgainstStorage(ctx, q, upperASCII(q))\n}", cypherPath + ".bypassAgainst runs a statement (StorageExecutor.executeQueryAgainstStorage)"},
		{"runner_on_database", "cypher", "func bypassOnDatabase(ctx context.Context, e *StorageExecutor, q string) {\n\t_, _ = e.executeOnDatabase(ctx, \"db\", q, nil, true)\n}", cypherPath + ".bypassOnDatabase runs a statement (StorageExecutor.executeOnDatabase)"},
		{"direct_storage_write", "cypher", "func bypassStore(e *StorageExecutor) {\n\t_, _ = e.storage.CreateNode(&storage.Node{ID: \"x\"})\n}", cypherPath + ".bypassStore writes storage (Engine.CreateNode)"},
		{"direct_storage_write_value", "cypher", "func bypassStoreValue(e *StorageExecutor) {\n\tdel := e.storage.DeleteNode\n\t_ = del(\"x\")\n}", cypherPath + ".bypassStoreValue writes storage (Engine.DeleteNode)"},
	}
	fixtureSource := map[string]string{
		"fabric": "package fabric\n\nimport (\n\t\"context\"\n\n\t\"github.com/orneryd/nornicdb/pkg/storage\"\n)\n\nvar _ context.Context\nvar _ *storage.RemoteEngine\n",
		"cypher": "package cypher\n\nimport (\n\t\"context\"\n\n\t\"github.com/orneryd/nornicdb/pkg/fabric\"\n\t\"github.com/orneryd/nornicdb/pkg/storage\"\n)\n\nvar _ context.Context\nvar _ *fabric.FabricTransaction\nvar _ storage.Engine\n",
	}
	// executeRouted is a statement entry where the executor has one.
	entry, err := os.ReadFile(filepath.Join(loader.cypher.Dir, "executor_entry.go"))
	require.NoError(t, err)
	routed := strings.Contains(string(entry), "func (e *StorageExecutor) executeRouted(")
	for i := 0; i < len(fixtures); i++ {
		if fixtures[i].name == "runner_routed" && !routed {
			fixtures = append(fixtures[:i], fixtures[i+1:]...)
			i--
			continue
		}
		fixtureSource[fixtures[i].pkg] += "\n" + fixtures[i].src + "\n"
	}
	reported := map[string]string{
		"fabric": strings.Join(writeAccessViolations(loader.typed(loader.fabric, map[string]string{"zz_fixture_fabric.go": fixtureSource["fabric"]})), "\n"),
		"cypher": strings.Join(writeAccessViolations(loader.typed(loader.cypher, map[string]string{"zz_fixture_cypher.go": fixtureSource["cypher"]})), "\n"),
	}
	for _, fixture := range fixtures {
		t.Run("fixture/"+fixture.name, func(t *testing.T) {
			require.Contains(t, reported[fixture.pkg], fixture.want)
		})
	}

	read := func(listed *writeAccessListed, name string) string {
		src, err := os.ReadFile(filepath.Join(listed.Dir, name))
		require.NoError(t, err)
		return strings.ReplaceAll(string(src), "\r\n", "\n")
	}
	// Each mutation of the owner the earlier reviews tried, as it applies to
	// this owner, and the CALL { } and graph.byName ones.
	mutations := []struct {
		name   string
		fabric bool
		file   string
		from   string
		to     string
		want   string
	}{
		{"claim_error_ignored", false, "fabric_access_mode.go", "if _, err := s.tx.ClaimWrite(participant); err != nil {\n\t\t\treturn \"\", false, err\n\t\t}", "_, _ = s.tx.ClaimWrite(participant)", "ClaimWrite's error is not the outcome"},
		{"remote_use_treated_local", false, "fabric_access_mode.go", "if fragmentWrites(statement, !local) {", "if fragmentWrites(statement, false) {", "ClaimWrite under conditions"},
		{"claim_without_identity", false, "fabric_access_mode.go", "\tparticipant, local := s.participant()\n\tif fragmentWrites(", "\tparticipant, local := \"p\", true\n\tif fragmentWrites(", "without \"participant, local := s.participant()\" before it"},
		{"claimed_statement_not_joined", false, "fabric_access_mode.go", "join = local && !inTransaction && s.joins && (!s.executor.autoCommit || s.tx.WriteShard() == participant)", "join = local && !inTransaction && s.joins && !s.executor.autoCommit", "fabricScope.open no longer has"},
		{"implicit_write_commits_alone", false, "fabric_access_mode.go", "if !local || !s.joins {", "if true {", "fabricScope.openWrite no longer has"},
		{"remote_user_procedure_trusted", false, "fabric_access_mode.go", "if !known || remote && (procedure.User || procedure.Spec.ExecutesStatements) {", "if !known || remote && procedure.Spec.ExecutesStatements {", "statementWrites no longer has"},
		{"procedures_ignored", false, "fabric_access_mode.go", "if !known || remote && (procedure.User || procedure.Spec.ExecutesStatements) {", "if false {", "statementWrites no longer has"},
		{"no_canonical_text", false, "fabric_access_mode.go", "\tquery = classificationText(query)\n", "\t_ = classificationText\n", "statementWrites no longer has"},
		{"write_clauses_ignored", false, "fabric_access_mode.go", "if requirements.Write || requirements.Schema {", "if requirements.Write && requirements.Schema {", "statementWrites no longer has"},
		{"memo_not_decided", false, "fabric_access_mode.go", "writes = statementWrites(query, remote)", "writes = false", "accessModeMemo.decide no longer has"},
		{"fragment_runs_unscoped", false, "executor_fabric.go", "participant, participantCtx, release, err := scope.fragmentExecutor(", "participant, participantCtx, release, err := (*fabricScope)(nil).fragmentExecutor(", "ExecuteQueryWithRecord no longer has"},
		{"implicit_write_skips_scope", false, "executor.go", "if scope := fabricScopeFrom(ctx); scope != nil {\n\t\tjoined, joinedCtx, release, err := scope.openWrite(ctx)", "if scope := (*fabricScope)(nil); scope != nil {\n\t\tjoined, joinedCtx, release, err := scope.openWrite(ctx)", "executeWithImplicitTransactionCallback no longer has"},
		{"use_switch_skips_scope", false, "executor_use.go", "return selected.executorFor(withFabricScope(ctx, selected), scopedExec, query)", "_ = selected\n\treturn scopedExec, ctx, func() {}, nil", "useDatabase no longer has"},
		{"nested_composite_commits_alone", false, "executor_fabric.go", "\tif scope := fabricScopeFrom(ctx); scope != nil {\n\t\treturn e.executeNestedFabric(ctx, cypher, params, scope)\n\t}\n", "", "executeViaFabric no longer has"},
		{"procedure_statement_skips_router", false, "executor_internal.go", "result, err = e.runScopedStatement(ctx, statement, func(", "result, err = func(run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) { return run(ctx, e) }(func(", "executeUserStatement no longer has"},
		{"public_statement_skips_router", false, "executor_entry.go", "return e.runScopedStatement(ctx, cypher, func(", "return func(run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) { return run(ctx, e) }(func(", "StorageExecutor.Execute no longer has"},
		{"set_not_a_write", false, "authorization.go", "requirements.Write = keywords[\"CREATE\"] || keywords[\"DELETE\"] || keywords[\"SET\"] ||", "requirements.Write = keywords[\"CREATE\"] || keywords[\"DELETE\"] ||", "permissionRequirements no longer has"},
		{"procedures_in_braces_missed", false, "authorization.go", "\topts.SkipBraces = false\n\tvar names []string", "\topts.SkipBraces = true\n\tvar names []string", "calledProcedureNames no longer has"},
		{"run_not_marked", false, "procedure_registry_builtin.go", "registerStatementProcedureLiteral(\"apoc.cypher.run\",", "registerBuiltInProcedureLiteral(\"apoc.cypher.run\",", "ensureBuiltInProceduresRegistered no longer has"},
		{"owner_not_installed", false, "fabric_access_mode.go", "fabric.SetAccessMode(fragmentWrites)", "fabric.SetAccessMode(func(string, bool) bool { return false })", "access mode installed other than from fragmentWrites"},
		{"no_owner_counts_as_read", true, "access_mode.go", "if mode == nil || *mode == nil {\n\t\treturn true\n\t}", "if mode == nil || *mode == nil {\n\t\treturn false\n\t}", "decideAccessMode no longer has"},
		{"remote_treated_local", true, "remote_executor.go", "handle, err := r.participant(ctx, sub, engine, decideAccessMode(query, true))", "handle, err := r.participant(ctx, sub, engine, decideAccessMode(query, false))", "executeRemoteQuery no longer has"},
		{"remote_write_session_unclaimed", true, "remote_executor.go", "\tif writes {\n\t\tif _, err := fabricTx.ClaimWrite(sub.ShardName); err != nil {\n\t\t\treturn nil, err\n\t\t}\n\t}\n", "", "RemoteFragmentExecutor.participant"},
		{"remote_read_session_commits", true, "remote_executor.go", "return handle.endWithin(terminalCtx, handle.write) }", "return handle.endWithin(terminalCtx, true) }", "RemoteFragmentExecutor.participant no longer has"},
		{"remote_writer_reads_in_read_session", true, "remote_executor.go", "\twrites = writes || fabricTx.WriteShard() == sub.ShardName\n", "", "RemoteFragmentExecutor.participant no longer has"},
		{"writer_committed_among_readers", true, "transaction.go", "if name != t.writeShard {", "if true {", "FabricTransaction.Commit no longer has"},
		{"dispatch_without_resolution", true, "executor.go", "loc, execParams, ctx, err := e.prepareExecDispatch(ctx, tx, f, params)", "var loc Location\n\tvar execParams map[string]interface{}\n\tvar err error", "dispatchExec no longer has"},
	}
	for _, mutation := range mutations {
		t.Run("mutation/"+mutation.name, func(t *testing.T) {
			listed := loader.cypher
			if mutation.fabric {
				listed = loader.fabric
			}
			src := read(listed, mutation.file)
			require.Equal(t, 1, strings.Count(src, mutation.from), "mutation anchor moved in %s: %q", mutation.file, mutation.from)
			pkg := loader.parsed(listed, map[string]string{mutation.file: strings.Replace(src, mutation.from, mutation.to, 1)})
			if typed := loader.typedOrNil(listed, map[string]string{mutation.file: strings.Replace(src, mutation.from, mutation.to, 1)}); typed != nil {
				pkg = typed
			}
			require.Contains(t, strings.Join(writeAccessViolations(pkg), "\n"), mutation.want)
		})
	}
}
