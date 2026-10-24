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
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A composite statement writes one database, decided where a write reaches
// storage: by the write admission of the transaction the write goes through
// (storage.BadgerTransaction.SetWriteAdmission), which claims the database
// (FabricTransaction.ClaimWrite) before the transaction's first write.
// Execution gets one kind of storage for a database inside a composite
// statement: its participant storage (newParticipantStorage), an ordinary
// transactionStorageWrapper, transparent to capability lookups, whose
// transaction begins without claiming on its first write or MERGE key
// reservation (participantHandle.open installs the admission). A composite
// statement runs no batch of CALL { ... } IN TRANSACTIONS
// (executeTransactionBatch refuses one inside it). A remote constituent reached through its engine runs
// every operation in the statement's remote session (compositeStatement.remote),
// where a write claims it, and the composite router
// reaches its constituents' participant storage. The guard type-checks
// pkg/fabric and pkg/cypher, parses pkg/storage, and holds that construction
// to its one place: the wrapper's transaction is read only by its accessors,
// the lazy transaction, the remote engine and the handle are built and
// reached only by their constructors, admissions are installed only by
// their owners, every transaction write asks the admission, ClaimWrite has
// its claim sites, and the raw engine sources and write-open primitives
// have their openers.
// Every reference is resolved to its object, package-level ones included;
// a field is named with the struct that declares it.
const (
	fabricPath  = "github.com/orneryd/nornicdb/pkg/fabric"
	cypherPath  = "github.com/orneryd/nornicdb/pkg/cypher"
	storagePath = "github.com/orneryd/nornicdb/pkg/storage"
)

// writeAccessPackageLevel is the key of references outside any function.
const writeAccessPackageLevel = "(package level)"

// writeAccessClaimSites are the functions that may call
// FabricTransaction.ClaimWrite: the conditions of the if statements around
// the call, innermost first, and statements that must come before it.
var writeAccessClaimSites = map[string]struct {
	conditions []string
	before     []string
}{
	cypherPath + ".participantHandle.claim":            {},
	fabricPath + ".RemoteFragmentExecutor.participant": {conditions: []string{"writes"}},
}

// writeAccessOpener is who may refer to a write-open primitive: the only
// functions that may, and those of them that take it as a value.
type writeAccessOpener struct {
	callers []string
	values  []string
}

// writeAccessOpeners are the write-open primitives, the raw engine sources
// and what reaches them: the claims, the participant's transaction begin,
// the participant storage and gate constructions, the composite router,
// the remote sessions, an implicit or explicit transaction's begin, the
// database a USE selects, and a fragment's dispatch to its participant.
var writeAccessOpeners = map[string]writeAccessOpener{
	fabricPath + ".FabricTransaction.ClaimWrite":         {callers: []string{cypherPath + ".participantHandle.claim", fabricPath + ".RemoteFragmentExecutor.participant"}},
	cypherPath + ".participantHandle.claim":              {callers: []string{cypherPath + ".participantHandle.open"}, values: []string{cypherPath + ".participantHandle.open"}},
	cypherPath + ".participantHandle.open":               {callers: []string{cypherPath + ".newParticipantStorage"}, values: []string{cypherPath + ".newParticipantStorage"}},
	cypherPath + ".participantTransaction.begun":         {callers: []string{cypherPath + ".transactionStorageWrapper.transaction", cypherPath + ".transactionStorageWrapper.writable", cypherPath + ".transactionStorageWrapper.mergeTransaction"}},
	storagePath + ".BadgerTransaction.SetWriteAdmission": {callers: []string{cypherPath + ".participantHandle.open"}},
	cypherPath + ".withoutCompositeStatement":            {callers: []string{cypherPath + ".cypherFabricExecutor.statementFor"}},
	cypherPath + ".unwrapBadgerEngine": {callers: []string{
		cypherPath + ".loadAccessMeta", cypherPath + ".newStorageExecutor", cypherPath + ".setRevealOnEngine",
		cypherPath + ".StorageExecutor.callNornicDbDecayInfo", cypherPath + ".StorageExecutor.callNornicDbKnowledgePolicyDeindexStatus",
		cypherPath + ".StorageExecutor.callNornicDbKnowledgePolicyInfo", cypherPath + ".StorageExecutor.callNornicDbKnowledgePolicyResolve",
		cypherPath + ".StorageExecutor.cloneWithStorage", cypherPath + ".StorageExecutor.getDecayContext", cypherPath + ".StorageExecutor.propertyViewEngine",
	}},
	cypherPath + ".newParticipantStorage":                 {callers: []string{cypherPath + ".compositeStatement.participant"}},
	storagePath + ".RemoteEngine.WithStatementRunner":     {callers: []string{cypherPath + ".compositeStatement.remote"}},
	storagePath + ".NewStatementRemoteEngine":             {callers: []string{cypherPath + ".compositeStatement.remote", cypherPath + ".compositeStatement.router"}},
	fabricPath + ".RemoteFragmentExecutor.RunInStatement": {callers: []string{cypherPath + ".compositeStatement.remote"}},
	cypherPath + ".compositeStatement.participant":        {callers: []string{cypherPath + ".compositeStatement.on", cypherPath + ".compositeStatement.router", cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord"}},
	cypherPath + ".compositeStatement.router":             {callers: []string{cypherPath + ".compositeStatement.on", cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord"}},
	cypherPath + ".compositeStatement.remote":             {callers: []string{cypherPath + ".compositeStatement.on", cypherPath + ".compositeStatement.router"}},
	storagePath + ".CompositeEngine.WithConstituents":     {callers: []string{cypherPath + ".compositeStatement.router"}},
	cypherPath + ".DatabaseManagerInterface.GetStorageForUse": {callers: []string{
		cypherPath + ".StorageExecutor.runFabricStatement", cypherPath + ".StorageExecutor.fabricResultCacheKey", cypherPath + ".StorageExecutor.scopedExecutorForUse",
		cypherPath + ".StorageExecutor.resolveCompositeStorage", cypherPath + ".StorageExecutor.resolveCompositeConstituent", cypherPath + ".StorageExecutor.systemStorage",
	}},
	cypherPath + ".beginTransactionSnapshot":                 {callers: []string{cypherPath + ".StorageExecutor.beginImplicitTransaction", cypherPath + ".StorageExecutor.handleBegin"}},
	cypherPath + ".StorageExecutor.beginImplicitTransaction": {callers: []string{cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback", cypherPath + ".participantHandle.open"}},
	cypherPath + ".StorageExecutor.useDatabase":              {callers: []string{cypherPath + ".StorageExecutor.executeOnDatabase", cypherPath + ".StorageExecutor.pipelineApplyCallSubqueryWithMetadata"}},
	cypherPath + ".StorageExecutor.scopedExecutorForUse":     {callers: []string{cypherPath + ".StorageExecutor.useDatabase", cypherPath + ".StorageExecutor.runFabricStatement", cypherPath + ".StorageExecutor.executeShellCommand"}},
	storagePath + ".RemoteEngine.BeginCypherTx":              {callers: []string{fabricPath + ".RemoteFragmentExecutor.participant"}, values: []string{fabricPath + ".RemoteFragmentExecutor.participant"}},
	storagePath + ".RemoteEngine.BeginReadCypherTx":          {callers: []string{fabricPath + ".RemoteFragmentExecutor.participant"}, values: []string{fabricPath + ".RemoteFragmentExecutor.participant"}},
	fabricPath + ".RemoteFragmentExecutor.participant":       {callers: []string{fabricPath + ".RemoteFragmentExecutor.executeRemoteQuery", fabricPath + ".RemoteFragmentExecutor.RunInStatement"}},
	fabricPath + ".LocalFragmentExecutor.ExecuteWithRecord":  {callers: []string{fabricPath + ".FabricExecutor.dispatchExec", fabricPath + ".LocalFragmentExecutor.Execute", fabricPath + ".LocalFragmentExecutor.ExecuteWithRecordRows"}},
	fabricPath + ".RemoteFragmentExecutor.Execute":           {callers: []string{fabricPath + ".FabricExecutor.dispatchExec", fabricPath + ".RemoteFragmentExecutor.ExecuteRows"}},
}

// writeAccessField is who may read and who may set a field: nil read means
// any function may read it. A name ending in ".*" allows every method of
// that type. A reader listed in calls may use the field only to call one of
// its methods. A raw field is a raw database source: no function, a reader
// included, writes through it (writeAccessRawWrites).
type writeAccessField struct {
	read  []string
	set   []string
	calls map[string][]string
	raw   bool
}

// writeAccessRawWrites are the methods that write a database, or open a
// transaction or statement of their own on it: never called through a raw
// source.
var writeAccessRawWrites = map[string]bool{
	"CreateNode": true, "UpdateNode": true, "UpdateNodeObserved": true, "DeleteNode": true, "DeleteConnectedNode": true,
	"CreateEdge": true, "UpdateEdge": true, "DeleteEdge": true, "BulkCreateNodes": true, "BulkCreateEdges": true,
	"BulkDeleteNodes": true, "BulkDeleteEdges": true, "DeleteByPrefix": true, "BeginTransaction": true,
	"Execute": true, "ExecuteWithParams": true,
}

// writeAccessFields hold the participant storage's construction to its
// constructors: the wrapper's transaction behind its accessors, the lazy
// transaction and its admission, the remote engine's inner engine and
// claim, the handle's raw engine and transaction owner, and the
// coordinator's write state.
var writeAccessFields = map[string]writeAccessField{
	cypherPath + ".transactionStorageWrapper.tx": {
		read: []string{cypherPath + ".transactionStorageWrapper.transaction", cypherPath + ".transactionStorageWrapper.writable"},
		set:  []string{cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback", cypherPath + ".StorageExecutor.inTransactionStorage", cypherPath + ".StorageExecutor.routeAdministrationStatement"},
	},
	cypherPath + ".transactionStorageWrapper.lazy": {set: []string{cypherPath + ".newParticipantStorage"}},
	cypherPath + ".transactionStorageWrapper.underlying": {
		raw:  true,
		read: []string{cypherPath + ".transactionStorageWrapper.*", cypherPath + ".labellessPropertyIndexUsable"},
		set:  []string{cypherPath + ".newParticipantStorage", cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback", cypherPath + ".StorageExecutor.inTransactionStorage", cypherPath + ".StorageExecutor.routeAdministrationStatement"},
	},
	cypherPath + ".participantHandle.source": {raw: true, read: []string{cypherPath + ".participantHandle.open", cypherPath + ".participantHandle.run"}, set: []string{cypherPath + ".compositeStatement.participant"}},
	cypherPath + ".cypherFabricExecutor.base": {
		raw: true,
		read: []string{cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord", cypherPath + ".cypherFabricExecutor.ExecuteRecordQuery", cypherPath + ".cypherFabricExecutor.participants", cypherPath + ".compositeStatement.participant",
			cypherPath + ".cypherFabricExecutor.CachedResultColumns", cypherPath + ".cypherFabricExecutor.ResultProjection"},
		// Result metadata only: the statement's columns, from the base
		// executor's syntax owner, and nothing else of it.
		calls: map[string][]string{
			cypherPath + ".cypherFabricExecutor.CachedResultColumns": {"CachedResultColumns"},
			cypherPath + ".cypherFabricExecutor.ResultProjection":    {"ResultProjection"},
		},
		set: []string{cypherPath + ".StorageExecutor.executeViaPreparedFabricWithTx"},
	},
	cypherPath + ".participantTransaction.tx": {
		read: []string{cypherPath + ".participantTransaction.begun", cypherPath + ".transactionStorageWrapper.transaction"},
		set:  []string{},
	},
	cypherPath + ".participantTransaction.begin": {read: []string{cypherPath + ".participantTransaction.begun"}, set: []string{cypherPath + ".newParticipantStorage"}},
	cypherPath + ".compositeStatement.remotes": {
		read: []string{cypherPath + ".compositeStatement.remote", cypherPath + ".compositeStatement.joined"},
		set:  []string{cypherPath + ".compositeStatement.remote"},
	},
	cypherPath + ".participantHandle.engine":  {raw: true, read: []string{cypherPath + ".participantHandle.open"}, set: []string{cypherPath + ".compositeStatement.participant"}},
	cypherPath + ".participantHandle.storage": {set: []string{cypherPath + ".compositeStatement.participant"}},
	cypherPath + ".participantHandle.owner":   {read: []string{cypherPath + ".participantHandle.open", cypherPath + ".StorageExecutor.failTransaction"}, set: []string{}},
	cypherPath + ".compositeContext.storage": {
		read: []string{cypherPath + ".compositeContext.Value"},
		set:  []string{cypherPath + ".participantHandle.run", cypherPath + ".compositeStatement.on"},
	},
	fabricPath + ".FabricTransaction.writeShard": {set: []string{fabricPath + ".FabricTransaction.GetOrOpen"}},
	fabricPath + ".SubTransaction.IsWrite":       {set: []string{fabricPath + ".FabricTransaction.GetOrOpen"}},
}

// writeAccessConstructions are the only functions that may build a value of
// a type as a literal.
var writeAccessConstructions = map[string][]string{
	cypherPath + ".transactionStorageWrapper": {cypherPath + ".newParticipantStorage", cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback", cypherPath + ".StorageExecutor.inTransactionStorage", cypherPath + ".StorageExecutor.routeAdministrationStatement"},
	cypherPath + ".participantTransaction":    {cypherPath + ".newParticipantStorage"},
	cypherPath + ".participantHandle":         {cypherPath + ".compositeStatement.participant"},
	cypherPath + ".compositeStatement":        {cypherPath + ".cypherFabricExecutor.statementFor"},
	cypherPath + ".compositeContext":          {cypherPath + ".participantHandle.run", cypherPath + ".compositeStatement.on", cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord"},
	fabricPath + ".SubTransaction":            {fabricPath + ".FabricTransaction.GetOrOpen"},
	fabricPath + ".FabricTransaction":         {fabricPath + ".NewFabricTransaction"},
}

// writeAccessStorageWrites are the participant storage's writes, each of
// which writes through the transaction writable begins and admits.
var writeAccessStorageWrites = []string{"CreateNode", "UpdateNode", "DeleteNode", "DeleteConnectedNode", "CreateEdge", "UpdateEdge", "DeleteEdge", "BulkCreateEdges", "BulkDeleteNodes", "BulkDeleteEdges"}

// writeAccessTransactionWrites are the transaction's data writes, each of
// which asks its write admission (ensureDataWriteAllowedLocked) before it
// changes anything.
var writeAccessTransactionWrites = []string{"CreateNode", "UpdateNode", "DeleteNode", "DeleteConnectedNode", "CreateEdge", "UpdateEdge", "DeleteEdge", "BulkCreateEdges"}

// writeAccessOwnerPins are statements an owner function must keep, in
// order: what makes its decision, or routes a statement or write through it.
var writeAccessOwnerPins = map[string][]string{
	cypherPath + ".transactionStorageWrapper.writable":       {"if w.lazy == nil {", "tx, err := w.lazy.begun()", "return tx, tx.AdmitWrite()"},
	cypherPath + ".transactionStorageWrapper.transaction":    {"if w.lazy == nil {", "if tx, err := w.lazy.begun(); err == nil {"},
	cypherPath + ".transactionStorageWrapper.GetInnerEngine": {"return w.underlying"},
	cypherPath + ".participantTransaction.begun":             {"tx, err := p.begin()", "p.tx.Store(tx)"},
	cypherPath + ".participantHandle.claim":                  {"if _, err := h.tx.ClaimWrite(h.name); err != nil {"},
	cypherPath + ".prepareMergeKeys": {
		"preparer, ok := store.(storage.MergeNodePreparer)",
		"return preparer.PrepareMergeNodeForWrite(ctx, labels, props)",
		"return preparer.PrepareMergeNode(ctx, labels, props)",
	},
	cypherPath + ".transactionStorageWrapper.PrepareMergeNode":         {"tx, err := w.mergeTransaction()", "return tx.PrepareMergeNode(ctx, labels, props)"},
	cypherPath + ".transactionStorageWrapper.PrepareMergeNodeForWrite": {"tx, err := w.mergeTransaction()", "return tx.PrepareMergeNodeForWrite(ctx, labels, props)"},
	cypherPath + ".transactionStorageWrapper.mergeTransaction":         {"if tx := w.transaction(); tx != nil || w.lazy == nil {", "return w.lazy.begun()"},
	cypherPath + ".StorageExecutor.executeTransactionBatch":            {"if compositeStatementFrom(ctx) != nil {"},
	cypherPath + ".newParticipantStorage":                              {"lazy := &participantTransaction{begin: h.open}"},
	cypherPath + ".participantHandle.open": {
		"if _, err := h.tx.GetOrOpen(h.name, false); err != nil {",
		"started, err := h.source.beginImplicitTransaction(h.begin, engines.txEngine, h.namespace)",
		"if _, err := owner.beginTransaction(h.begin); err != nil {",
		"owner.txContext.storageWrapper = h.storage",
		"tx.SetWriteAdmission(h.claim)",
		"if err := h.tx.BindParticipantCallbacks(h.name, commit, rollback); err != nil {",
	},
	cypherPath + ".participantHandle.run": {
		"exec := h.source.cloneWithStorage(h.storage)",
		"return exec, &compositeContext{Context: ctx, statement: statement, storage: h.storage}",
	},
	cypherPath + ".compositeStatement.on": {
		"if store.lazy != nil {",
		"routed := exec.cloneWithStorage(s.router(store))",
		"remoteExec := exec.cloneWithStorage(s.remote(name, exec.storage))",
		"h, err := s.participant(database, exec.storage)",
		"runExec, runCtx := h.run(ctx, s)",
	},
	cypherPath + ".compositeStatement.router": {"return root.WithConstituents(func(", "return s.remote(name, engine)", "return h.storage"},
	cypherPath + ".compositeStatement.remote": {
		"bound = raw.WithStatementRunner(func(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {",
		"return c.remote.RunInStatement(s.begin, s.tx, name, raw, ctx, statement, params)",
	},
	// The statement's owner ends every participant however the statement
	// ends, a panic included, and a statement that fails or panics fails its
	// explicit transaction.
	cypherPath + ".StorageExecutor.executeViaPreparedFabricWithTx": {"if !ended {", "_ = tx.Rollback(nil)", "ended = true", "if err := tx.Commit(nil, nil); err != nil {"},
	cypherPath + ".cypherFabricExecutor.ExecuteQueryWithRecord": {
		"composite := c.statementFor(ctx, tx)",
		"exec = c.base.cloneForStorage(root).cloneWithStorage(composite.router(root))",
		"participant, err := composite.participant(dbName, engine)",
		"exec, ctx = participant.run(ctx, composite)",
	},
	cypherPath + ".StorageExecutor.executeWithImplicitTransactionCallback": {
		"composite := compositeStatementFrom(ctx)",
		"if composite != nil {",
		"target, targetCtx, err := composite.executorFor(ctx, e)",
		"return target.onTransactionStorage(targetCtx, participant, inlineEmbeddingEnabled, execute)",
	},
	cypherPath + ".StorageExecutor.useDatabase": {
		"composite := compositeStatementFrom(ctx)",
		"if composite == nil || scopedExec.shouldUseFabricPlanner(query) {",
		"return composite.on(ctx, resolvedDB, scopedExec)",
	},
	cypherPath + ".StorageExecutor.executeOnDatabase": {
		"scopedExec, ctx, err := e.useDatabase(ctx, db, query)",
		"result, err = scopedExec.executeViaFabric(ctx, query, params)",
	},
	cypherPath + ".StorageExecutor.executeViaFabric": {
		"if composite := compositeStatementFrom(ctx); composite != nil {",
		"return e.executeNestedFabric(ctx, cypher, params, composite)",
	},
	cypherPath + ".StorageExecutor.executeUserStatement": {
		"result, err = e.executeViaFabric(ctx, statement, params)",
		"result, err = e.inCompositeStatement(ctx, statement, func(",
	},
	cypherPath + ".StorageExecutor.Execute":              {"return e.inCompositeStatement(ctx, cypher, func("},
	cypherPath + ".StorageExecutor.inCompositeStatement": {"target, ctx, err := composite.executorFor(ctx, e)", "return run(ctx, target)"},
	cypherPath + ".StorageExecutor.callKpSuppress": {
		"if wrapper, ok := e.getStorage(ctx).(*transactionStorageWrapper); ok && wrapper.lazy != nil {",
		"if _, err := wrapper.writable(); err != nil {",
	},
	cypherPath + ".StorageExecutor.prepareSchemaTransaction":  {"tx, err := wrapper.writable()"},
	cypherPath + ".StorageExecutor.mutateSchema":              {"tx, err := wrapper.writable()"},
	cypherPath + ".StorageExecutor.executeSchemaCommand":      {"if tx, err = wrapper.writable(); err == nil {"},
	cypherPath + ".StorageExecutor.knowledgePolicySchema":     {"if transaction, err = wrapper.writable(); err != nil {"},
	cypherPath + ".StorageExecutor.executeKnowledgePolicyDDL": {"schema, err := e.knowledgePolicySchema(!show)"},
	// A remote participant's session follows the statement's access mode.
	cypherPath + ".statementWrites": {
		"query = classificationText(query)",
		"requirements := permissionRequirements(query)",
		"if requirements.Write || requirements.Schema {",
		"if !known || remote && (procedure.User || procedure.Spec.ExecutesStatements) {",
	},
	cypherPath + ".permissionRequirements": {`requirements.Write = keywords["CREATE"] || keywords["DELETE"] || keywords["SET"] ||`},
	cypherPath + ".calledProcedureNames":   {"opts.SkipParens = false", "opts.SkipBrackets = false", "opts.SkipBraces = false"},
	cypherPath + ".accessModeMemo.decide":  {"known = known && m.generation == generation", "writes = statementWrites(query, remote)", "m.modes[key] = writes"},
	cypherPath + ".ensureBuiltInProceduresRegistered": {
		`registerStatementProcedureLiteral("apoc.cypher.run",`, `registerStatementProcedureLiteral("apoc.cypher.doitall",`,
		`registerStatementProcedureLiteral("apoc.export.json.query",`, `registerStatementProcedureLiteral("apoc.export.csv.query",`,
	},
	fabricPath + ".decideAccessMode":                          {"if mode == nil || *mode == nil {\n\treturn true\n}", "return (*mode)(query, remote)"},
	fabricPath + ".fragmentMayWrite":                          {"return decideAccessMode(query, true)"},
	fabricPath + ".FabricExecutor.dispatchExec":               {"loc, execParams, ctx, err := e.prepareExecDispatch(ctx, tx, f, params)"},
	fabricPath + ".RemoteFragmentExecutor.executeRemoteQuery": {"handle, err := r.participant(ctx, fabricTx, sub, engine, decideAccessMode(query, true))"},
	fabricPath + ".RemoteFragmentExecutor.RunInStatement":     {"handle, err := r.participant(begin, fabricTx, sub, engine, decideAccessMode(query, true))", "return handle.tx.QueryCypher(ctx, query, params)"},
	fabricPath + ".RemoteFragmentExecutor.participant": {
		"if writes {\n\tif _, err := fabricTx.ClaimWrite(sub.ShardName); err != nil {",
		"writes = writes || fabricTx.WriteShard() == sub.ShardName",
		"open := engine.BeginReadCypherTx",
		"commitFn := func(_ *SubTransaction) error { return handle.endWithin(terminalCtx, handle.write) }",
	},
	fabricPath + ".FabricTransaction.Commit": {"if name != t.writeShard {", "if writer := t.subTxns[t.writeShard]; writer != nil && writer.State == \"open\" {"},
	// The transaction's write admission, asked before every write.
	storagePath + ".BadgerTransaction.admitWriteLocked":             {"if err := tx.writeAdmission(); err != nil {", "tx.writeAdmission = nil"},
	storagePath + ".BadgerTransaction.AdmitWrite":                   {"return tx.admitWriteLocked()"},
	storagePath + ".BadgerTransaction.ensureDataWriteAllowedLocked": {"return tx.admitWriteLocked()"},
	storagePath + ".BadgerTransaction.Schema":                       {"if err := tx.admitWriteLocked(); err != nil {", "tx.schemaTransaction = true"},
	storagePath + ".BadgerTransaction.KnowledgePolicySchema": {
		"view.SetPersister(func(updated *SchemaDefinition) error {",
		"if err := tx.admitWriteLocked(); err != nil {",
		"if err := tx.badgerTx.Set(schemaKey(tx.namespace), blob); err != nil {",
	},
}

func init() {
	for _, method := range writeAccessStorageWrites {
		writeAccessOwnerPins[cypherPath+".transactionStorageWrapper."+method] = []string{"tx, err := w.writable()"}
	}
	for _, method := range writeAccessTransactionWrites {
		writeAccessOwnerPins[storagePath+".BadgerTransaction."+method] = []string{"if err := tx.ensureDataWriteAllowedLocked(); err != nil {"}
	}
}

// writeAccessPkg is a package for the guard (info nil: parsed only).
type writeAccessPkg struct {
	path  string
	fset  *token.FileSet
	files []*ast.File
	info  *types.Info
	// fields names each struct field with the struct that declares it.
	fields map[*types.Var]string
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

// objectName is pkgpath.Type.Name for a method or a field of a struct type
// of pkg, pkgpath.Name for a package-level object.
func (pkg *writeAccessPkg) objectName(obj types.Object) string {
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
			if owner, ok := pkg.fields[o]; ok {
				return owner + "." + o.Name()
			}
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

// allowedIn reports whether key is one of allowed, a name ending in ".*"
// allowing every method of its type.
func allowedIn(key string, allowed []string) bool {
	for _, name := range allowed {
		if name == key || strings.HasSuffix(name, ".*") && strings.HasPrefix(key, strings.TrimSuffix(name, "*")) {
			return true
		}
	}
	return false
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
				key := ""
				for _, outer := range stack {
					switch decl := outer.(type) {
					case *ast.FuncDecl:
						key = funcKey(pkg.path, decl)
					case *ast.GenDecl:
						key = pkg.path + "." + writeAccessPackageLevel
					}
				}
				if key == "" {
					return true
				}
				switch n := node.(type) {
				case *ast.Ident:
					g.checkReference(pkg, key, n, stack)
				case *ast.CompositeLit:
					if pkg.info == nil {
						return true
					}
					built := typeName(pkg.info.TypeOf(n))
					if allowed, guarded := writeAccessConstructions[built]; guarded && !allowedIn(key, allowed) {
						g.report(pkg, n.Pos(), "%s built outside its constructors (in %s)", strings.TrimPrefix(built, pkg.path+"."), key)
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
	if pkg.info == nil || len(stack) < 2 {
		return
	}
	name := pkg.objectName(pkg.info.Uses[ident])
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
	short := strings.TrimPrefix(strings.TrimPrefix(name, pkg.path+"."), storagePath+".")
	if opener, primitive := writeAccessOpeners[name]; primitive {
		if !allowedIn(key, opener.callers) {
			g.report(pkg, ident.Pos(), "%s reached outside its opener (in %s)", short, key)
			return
		}
		if !called && !allowedIn(key, opener.values) {
			g.report(pkg, ident.Pos(), "%s used other than as a call (in %s)", short, key)
			return
		}
	}
	if field, guarded := writeAccessFields[name]; guarded {
		if isAssigned(expr, holder) {
			if !allowedIn(key, field.set) {
				g.report(pkg, ident.Pos(), "%s set outside its constructors (in %s)", short, key)
			}
		} else if field.read != nil && !allowedIn(key, field.read) {
			g.report(pkg, ident.Pos(), "%s read outside its accessors (in %s)", short, key)
		} else if methods, exact := field.calls[key]; exact && !slices.Contains(methods, calledMethod(expr, stack[:at+1])) {
			g.report(pkg, ident.Pos(), "%s read other than to call %s (in %s)", short, strings.Join(methods, ", "), key)
		}
		if field.raw {
			if method := rawWriteThrough(expr, stack[:at+1]); method != "" {
				g.report(pkg, ident.Pos(), "%s written through (%s, in %s)", short, method, key)
			}
		}
		return
	}
	switch name {
	case fabricPath + ".FabricTransaction.GetOrOpen":
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
	case fabricPath + ".FabricTransaction.ClaimWrite":
		if called {
			g.checkClaim(pkg, key, call, stack[:at-1])
		}
	}
}

// calledMethod is the method called on expr when expr is the receiver of a
// call (expr.Method(...)), or "". stack ends with expr.
func calledMethod(expr ast.Node, stack []ast.Node) string {
	at := len(stack) - 1
	if at < 2 {
		return ""
	}
	sel, ok := stack[at-1].(*ast.SelectorExpr)
	if !ok || sel.X != expr {
		return ""
	}
	if call, ok := stack[at-2].(*ast.CallExpr); ok && call.Fun == sel {
		return sel.Sel.Name
	}
	return ""
}

// rawWriteThrough is the write method (writeAccessRawWrites) the expression
// chain starting at expr calls, through selections, calls, dereferences and
// parentheses, or "". stack ends with expr.
func rawWriteThrough(expr ast.Node, stack []ast.Node) string {
	current := expr
	for i := len(stack) - 2; i >= 0; i-- {
		switch parent := stack[i].(type) {
		case *ast.SelectorExpr:
			if parent.X != current {
				return ""
			}
			if i > 0 {
				if call, ok := stack[i-1].(*ast.CallExpr); ok && call.Fun == parent && writeAccessRawWrites[parent.Sel.Name] {
					return parent.Sel.Name
				}
			}
		case *ast.CallExpr:
			if parent.Fun != current {
				return ""
			}
		case *ast.ParenExpr:
			if parent.X != current {
				return ""
			}
		case *ast.StarExpr:
			if parent.X != current {
				return ""
			}
		default:
			return ""
		}
		current = stack[i]
	}
	return ""
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
		if stmt, ok := stack[len(stack)-2].(*ast.IfStmt); ok && stmt.Init == h && len(h.Rhs) == 1 && nodeText(pkg.fset, stmt.Cond) == "err != nil" &&
			len(stmt.Body.List) == 1 && strings.HasPrefix(nodeText(pkg.fset, stmt.Body.List[0]), "return ") && strings.HasSuffix(nodeText(pkg.fset, stmt.Body.List[0]), "err") {
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
// statements, in order; a claimed write's pin is its first statement. The
// access mode is installed once, from fragmentWrites.
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
			g.violations = append(g.violations, fmt.Sprintf("owner %s is gone; re-establish the guard", key))
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

// writeAccessLoader loads pkg/fabric, pkg/cypher and pkg/storage for the
// guard.
type writeAccessLoader struct {
	t       *testing.T
	imp     *writeAccessImporter
	fabric  *writeAccessListed
	cypher  *writeAccessListed
	storage *writeAccessListed
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
	loader := &writeAccessLoader{t: t, imp: imp, fabric: imp.listed[fabricPath], cypher: imp.listed[cypherPath], storage: imp.listed[storagePath]}
	require.NotNil(t, loader.fabric)
	require.NotNil(t, loader.cypher)
	require.NotNil(t, loader.storage)
	return loader
}

// structFields names every field of pkg's struct types with its struct.
func structFields(path string, info *types.Info) map[*types.Var]string {
	fields := map[*types.Var]string{}
	for _, obj := range info.Defs {
		named, ok := obj.(*types.TypeName)
		if !ok || named.Pkg() == nil || named.Parent() != named.Pkg().Scope() {
			continue
		}
		if structure, ok := named.Type().Underlying().(*types.Struct); ok {
			for i := 0; i < structure.NumFields(); i++ {
				fields[structure.Field(i)] = path + "." + named.Name()
			}
		}
	}
	return fields
}

// typed type-checks listed with bodies, with replace's sources, and
// requires it to compile.
func (l *writeAccessLoader) typed(listed *writeAccessListed, replace map[string]string) *writeAccessPkg {
	l.t.Helper()
	pkg, errs := l.check(listed, replace)
	require.Empty(l.t, errs, "type-check %s", listed.ImportPath)
	return pkg
}

// typedOrNil is typed, or nil when replace's sources don't compile (a
// mutation that leaves an unused variable): the rules that read source alone
// still apply to them.
func (l *writeAccessLoader) typedOrNil(listed *writeAccessListed, replace map[string]string) *writeAccessPkg {
	l.t.Helper()
	pkg, errs := l.check(listed, replace)
	if len(errs) > 0 {
		return nil
	}
	return pkg
}

func (l *writeAccessLoader) check(listed *writeAccessListed, replace map[string]string) (*writeAccessPkg, []error) {
	files, err := parseWriteAccessFiles(l.imp.fset, listed, replace)
	require.NoError(l.t, err)
	var errs []error
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: l.imp.from(listed), FakeImportC: true, Error: func(err error) { errs = append(errs, err) }}
	_, _ = conf.Check(listed.ImportPath, l.imp.fset, files, info)
	return &writeAccessPkg{path: listed.ImportPath, fset: l.imp.fset, files: files, info: info, fields: structFields(listed.ImportPath, info)}, errs
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
	require.Empty(t, writeAccessViolations(loader.typed(loader.fabric, nil), loader.typed(loader.cypher, nil), loader.parsed(loader.storage, nil)))
}

// Each way around the one construction the guard exists for is reported:
// the wrapper's transaction read past its accessors, a participant storage
// built or changed elsewhere, the remote engine's inner engine or the
// handle's raw engine reached, an admission installed or dropped elsewhere,
// a lazy transaction begun past writable, a transaction write that doesn't
// ask its admission, a claim
// from a new site or through an alias (package-level ones included), a raw
// engine source or the composite router reached, the coordinator's write
// state changed, and each write-open primitive reached past its opener.
func TestWriteAccessOwnerGuardReportsBypasses(t *testing.T) {
	loader := newWriteAccessLoader(t)
	fixtures := []struct{ name, pkg, src, want string }{
		{"method_value_get_or_open", "fabric", "func bypassOpen(tx *FabricTransaction) {\n\topen := tx.GetOrOpen\n\t_, _ = open(\"p\", false)\n}", "GetOrOpen used other than as a call"},
		{"aliased_transaction_opens_writer", "fabric", "func bypassAlias(tx *FabricTransaction) {\n\tt := tx\n\t_, _ = t.GetOrOpen(\"p\", true)\n}", "write flag is not false (in " + fabricPath + ".bypassAlias)"},
		{"helper_computes_write_flag", "fabric", "func bypassHelper(tx *FabricTransaction, writes bool) {\n\t_, _ = tx.GetOrOpen(\"p\", writes)\n}", "write flag is not false (in " + fabricPath + ".bypassHelper)"},
		{"constant_expression_flag", "fabric", "func bypassNotFalse(tx *FabricTransaction) {\n\t_, _ = tx.GetOrOpen(\"p\", !false)\n}", "write flag is not false (in " + fabricPath + ".bypassNotFalse)"},
		{"write_shard_set", "fabric", "func bypassShard(tx *FabricTransaction) {\n\ttx.writeShard = \"p\"\n}", "FabricTransaction.writeShard set outside its constructors"},
		{"sub_transaction_upgraded", "fabric", "func bypassUpgrade(sub *SubTransaction) {\n\tsub.IsWrite = true\n}", "SubTransaction.IsWrite set outside its constructors"},
		{"sub_transaction_built", "fabric", "func bypassBuild() *SubTransaction {\n\treturn &SubTransaction{IsWrite: true}\n}", "SubTransaction built outside its constructors"},
		{"writer_via_struct_literal", "fabric", "func bypassLiteral(tx *FabricTransaction) {\n\t*tx = FabricTransaction{writeShard: \"p\", state: \"open\", subTxns: map[string]*SubTransaction{}}\n}", "FabricTransaction built outside its constructors (in " + fabricPath + ".bypassLiteral)"},
		{"claim_from_new_site", "fabric", "func bypassClaim(tx *FabricTransaction) error {\n\t_, err := tx.ClaimWrite(\"p\")\n\treturn err\n}", "FabricTransaction.ClaimWrite reached outside its opener (in " + fabricPath + ".bypassClaim)"},
		{"remote_write_session_opened", "fabric", "func bypassSession(ctx context.Context, engine *storage.RemoteEngine) {\n\t_, _ = engine.BeginCypherTx(ctx)\n}", "RemoteEngine.BeginCypherTx reached outside its opener (in " + fabricPath + ".bypassSession)"},
		{"remote_participant_skipped", "fabric", "func (r *RemoteFragmentExecutor) bypassParticipant(ctx context.Context, tx *FabricTransaction, sub *SubTransaction, engine *storage.RemoteEngine) {\n\t_, _ = r.participant(ctx, tx, sub, engine, false)\n}", "RemoteFragmentExecutor.participant reached outside its opener (in " + fabricPath + ".RemoteFragmentExecutor.bypassParticipant)"},
		{"local_dispatch_bypass", "fabric", "func (e *FabricExecutor) bypassLocal(ctx context.Context, l *LocationLocal) {\n\t_, _ = e.local.ExecuteWithRecord(ctx, l, \"CREATE (n)\", nil, nil)\n}", "LocalFragmentExecutor.ExecuteWithRecord reached outside its opener (in " + fabricPath + ".FabricExecutor.bypassLocal)"},
		{"remote_executor_method_value", "fabric", "func (e *FabricExecutor) bypassRemote(ctx context.Context, l *LocationRemote) {\n\trun := e.remote.Execute\n\t_, _ = run(ctx, l, \"CREATE (n)\", nil, \"\")\n}", "RemoteFragmentExecutor.Execute reached outside its opener (in " + fabricPath + ".FabricExecutor.bypassRemote)"},
		{"package_level_fabric_claim", "fabric", "var claimAlias = (*FabricTransaction).ClaimWrite", "FabricTransaction.ClaimWrite reached outside its opener (in " + fabricPath + "." + writeAccessPackageLevel + ")"},
		{"claim_from_new_cypher_site", "cypher", "func bypassClaimValue(tx *fabric.FabricTransaction) error {\n\tclaim := tx.ClaimWrite\n\t_, err := claim(\"p\")\n\treturn err\n}", "FabricTransaction.ClaimWrite reached outside its opener (in " + cypherPath + ".bypassClaimValue)"},
		{"package_level_claim_alias", "cypher", "var cypherClaimAlias = (*fabric.FabricTransaction).ClaimWrite", "FabricTransaction.ClaimWrite reached outside its opener (in " + cypherPath + "." + writeAccessPackageLevel + ")"},
		{"package_level_open_alias", "cypher", "var openAlias = (*participantHandle).open", "participantHandle.open reached outside its opener (in " + cypherPath + "." + writeAccessPackageLevel + ")"},
		{"raw_transaction_read", "cypher", "func bypassRawTx(w *transactionStorageWrapper) error {\n\t_, err := w.tx.CreateNode(&storage.Node{ID: \"x\"})\n\treturn err\n}", "transactionStorageWrapper.tx read outside its accessors (in " + cypherPath + ".bypassRawTx)"},
		{"lazy_transaction_dropped", "cypher", "func bypassLazy(w *transactionStorageWrapper) {\n\tw.lazy = nil\n}", "transactionStorageWrapper.lazy set outside its constructors (in " + cypherPath + ".bypassLazy)"},
		{"eager_storage_built", "cypher", "func bypassEager(tx *storage.BadgerTransaction, engine storage.Engine) storage.Engine {\n\treturn &transactionStorageWrapper{tx: tx, underlying: engine}\n}", "transactionStorageWrapper built outside its constructors (in " + cypherPath + ".bypassEager)"},
		{"lazy_transaction_stored", "cypher", "func bypassStoreTx(p *participantTransaction, tx *storage.BadgerTransaction) {\n\tp.tx.Store(tx)\n}", "participantTransaction.tx read outside its accessors (in " + cypherPath + ".bypassStoreTx)"},
		{"lazy_begin_called", "cypher", "func bypassBeginLazy(p *participantTransaction) {\n\t_, _ = p.begin()\n}", "participantTransaction.begin read outside its accessors (in " + cypherPath + ".bypassBeginLazy)"},
		{"handle_opened_directly", "cypher", "func bypassHandleOpen(h *participantHandle) {\n\tbegin := h.open\n\t_, _ = begin()\n}", "participantHandle.open reached outside its opener (in " + cypherPath + ".bypassHandleOpen)"},
		{"handle_raw_engine", "cypher", "func bypassHandleEngine(h *participantHandle) storage.Engine {\n\treturn h.engine\n}", "participantHandle.engine read outside its accessors (in " + cypherPath + ".bypassHandleEngine)"},
		{"participant_storage_built", "cypher", "func bypassStorage(engine storage.Engine) *transactionStorageWrapper {\n\treturn newParticipantStorage(nil, engine, \"\")\n}", "newParticipantStorage reached outside its opener (in " + cypherPath + ".bypassStorage)"},
		{"foreign_storage_in_context", "cypher", "func bypassContext(ctx context.Context, w *transactionStorageWrapper) context.Context {\n\treturn &compositeContext{Context: ctx, storage: w}\n}", "compositeContext built outside its constructors (in " + cypherPath + ".bypassContext)"},
		{"router_bypassed", "cypher", "func bypassRouter(root *storage.CompositeEngine) storage.Engine {\n\treturn root.WithConstituents(func(_ string, e storage.Engine) storage.Engine { return e })\n}", "CompositeEngine.WithConstituents reached outside its opener (in " + cypherPath + ".bypassRouter)"},
		{"raw_engine_source", "cypher", "func (e *StorageExecutor) bypassRawEngine() {\n\t_, _ = e.dbManager.GetStorageForUse(\"db\", \"\")\n}", "DatabaseManagerInterface.GetStorageForUse reached outside its opener (in " + cypherPath + ".StorageExecutor.bypassRawEngine)"},
		{"implicit_transaction_begun", "cypher", "func bypassBegin(engine TransactionCapableEngine) {\n\t_, _ = beginTransactionSnapshot(engine)\n}", "beginTransactionSnapshot reached outside its opener (in " + cypherPath + ".bypassBegin)"},
		{"database_selected_past_use", "cypher", "func (e *StorageExecutor) bypassSelect(db string) {\n\t_, _, _ = e.scopedExecutorForUse(db, \"\")\n}", "scopedExecutorForUse reached outside its opener (in " + cypherPath + ".StorageExecutor.bypassSelect)"},
		{"raw_underlying_write", "cypher", "func bypassUnderlying(w *transactionStorageWrapper) {\n\t_, _ = w.underlying.CreateNode(&storage.Node{ID: \"x\"})\n}", "transactionStorageWrapper.underlying read outside its accessors (in " + cypherPath + ".bypassUnderlying)"},
		{"badger_from_storage", "cypher", "func bypassUnwrapBadger(e *StorageExecutor) {\n\t_ = unwrapBadgerEngine(e.storage)\n}", "unwrapBadgerEngine reached outside its opener (in " + cypherPath + ".bypassUnwrapBadger)"},
		{"participant_source", "cypher", "func bypassSource(h *participantHandle) {\n\t_, _ = h.source.storage.CreateNode(&storage.Node{ID: \"x\"})\n}", "participantHandle.source read outside its accessors (in " + cypherPath + ".bypassSource)"},
		{"statement_base_storage", "cypher", "func bypassBase(c *cypherFabricExecutor) {\n\t_, _ = c.base.storage.CreateNode(&storage.Node{ID: \"x\"})\n}", "cypherFabricExecutor.base read outside its accessors (in " + cypherPath + ".bypassBase)"},
		{"composite_statement_dropped", "cypher", "func bypassWithout(ctx context.Context) context.Context {\n\treturn withoutCompositeStatement(ctx)\n}", "withoutCompositeStatement reached outside its opener (in " + cypherPath + ".bypassWithout)"},
		{"begun_without_claim", "cypher", "func bypassBegun(p *participantTransaction) {\n\t_, _ = p.begun()\n}", "participantTransaction.begun reached outside its opener (in " + cypherPath + ".bypassBegun)"},
		{"remote_engine_unbound", "cypher", "func bypassRunner(engine *storage.RemoteEngine) *storage.RemoteEngine {\n\treturn engine.WithStatementRunner(nil)\n}", "RemoteEngine.WithStatementRunner reached outside its opener (in " + cypherPath + ".bypassRunner)"},
		{"remote_session_from_new_site", "cypher", "func bypassStatementSession(ctx context.Context, r *fabric.RemoteFragmentExecutor, tx *fabric.FabricTransaction, engine *storage.RemoteEngine) {\n\t_, _, _ = r.RunInStatement(ctx, tx, \"p\", engine, ctx, \"CREATE (n)\", nil)\n}", "RemoteFragmentExecutor.RunInStatement reached outside its opener (in " + cypherPath + ".bypassStatementSession)"},
		{"admission_replaced", "cypher", "func bypassAdmission(tx *storage.BadgerTransaction) {\n\ttx.SetWriteAdmission(nil)\n}", "BadgerTransaction.SetWriteAdmission reached outside its opener (in " + cypherPath + ".bypassAdmission)"},
		{"implicit_begun_elsewhere", "cypher", "func (e *StorageExecutor) bypassImplicit(ctx context.Context, engine TransactionCapableEngine) {\n\t_, _ = e.beginImplicitTransaction(ctx, engine, \"\")\n}", "StorageExecutor.beginImplicitTransaction reached outside its opener (in " + cypherPath + ".StorageExecutor.bypassImplicit)"},
		{"use_switch_reused", "cypher", "func (e *StorageExecutor) bypassUse(ctx context.Context) {\n\t_, _, _ = e.useDatabase(ctx, \"db\", \"CREATE (n)\")\n}", "useDatabase reached outside its opener (in " + cypherPath + ".StorageExecutor.bypassUse)"},
	}
	fixtureSource := map[string]string{
		"fabric": "package fabric\n\nimport (\n\t\"context\"\n\n\t\"github.com/orneryd/nornicdb/pkg/storage\"\n)\n\nvar _ context.Context\nvar _ *storage.RemoteEngine\n",
		"cypher": "package cypher\n\nimport (\n\t\"context\"\n\n\t\"github.com/orneryd/nornicdb/pkg/fabric\"\n\t\"github.com/orneryd/nornicdb/pkg/storage\"\n)\n\nvar _ context.Context\nvar _ *fabric.FabricTransaction\nvar _ storage.Engine\n",
	}
	for _, fixture := range fixtures {
		fixtureSource[fixture.pkg] += "\n" + fixture.src + "\n"
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
	// Each change to the construction or its routing that would let a write
	// reach a database unclaimed or outside the statement's transaction.
	mutations := []struct {
		name string
		pkg  string
		file string
		from string
		to   string
		want string
	}{
		{"claim_error_ignored", "cypher", "fabric_participants.go", "if _, err := h.tx.ClaimWrite(h.name); err != nil {\n\t\treturn err\n\t}", "_, _ = h.tx.ClaimWrite(h.name)", "ClaimWrite's error is not the outcome"},
		{"claim_returns_nil", "cypher", "fabric_participants.go", "func (h *participantHandle) claim() error {\n\tif _, err := h.tx.ClaimWrite(h.name); err != nil {\n\t\treturn err\n\t}\n\treturn nil\n}", "func (h *participantHandle) claim() error {\n\treturn nil\n}", "participantHandle.claim no longer has"},
		{"participant_joins_unregistered", "cypher", "fabric_participants.go", "\tif _, err := h.tx.GetOrOpen(h.name, false); err != nil {\n\t\treturn nil, err\n\t}\n", "", "participantHandle.open no longer has"},
		{"writable_unadmitted", "cypher", "transaction_storage_wrapper.go", "\treturn tx, tx.AdmitWrite()", "\treturn tx, nil", "transactionStorageWrapper.writable no longer has"},
		{"writable_skips_begin", "cypher", "transaction_storage_wrapper.go", "\ttx, err := w.lazy.begun()", "\ttx, err := w.lazy.tx.Load(), error(nil)", "transactionStorageWrapper.writable no longer has"},
		{"merge_reservation_skipped", "cypher", "merge.go", "preparer, ok := store.(storage.MergeNodePreparer)", "preparer, ok := storage.MergeNodePreparer(nil), false", "prepareMergeKeys no longer has"},
		{"merge_reservation_unbegun", "cypher", "transaction_storage_wrapper_merge_keys.go", "\treturn w.lazy.begun()\n", "\treturn nil, nil\n", "transactionStorageWrapper.mergeTransaction no longer has"},
		{"participant_unadmitted", "cypher", "fabric_participants.go", "\ttx.SetWriteAdmission(h.claim)\n", "", "participantHandle.open no longer has"},
		{"use_switch_condition_skips_participant", "cypher", "executor_use.go", "if composite == nil || scopedExec.shouldUseFabricPlanner(query) {", "if true {", "useDatabase no longer has"},
		{"callbacks_unbound", "cypher", "fabric_participants.go", "if err := h.tx.BindParticipantCallbacks(h.name, commit, rollback); err != nil {", "if err := error(nil); err != nil {", "participantHandle.open no longer has"},
		{"commit_skips_storage_actions", "cypher", "fabric_participants.go", "\towner.txContext.storageWrapper = h.storage\n", "", "participantHandle.open no longer has"},
		{"storage_begins_without_claim", "cypher", "fabric_participants.go", "lazy := &participantTransaction{begin: h.open}", "lazy := &participantTransaction{begin: func() (*storage.BadgerTransaction, error) { return nil, nil }}", "newParticipantStorage no longer has"},
		{"batch_inside_composite", "cypher", "transaction_batch.go", "\tif compositeStatementFrom(ctx) != nil {\n\t\treturn nil, transactionalCallContextError(", "\tif false {\n\t\treturn nil, transactionalCallContextError(", "executeTransactionBatch no longer has"},
		{"run_on_raw_engine", "cypher", "fabric_participants.go", "exec := h.source.cloneWithStorage(h.storage)", "exec := h.source.cloneWithStorage(h.engine)", "participantHandle.run no longer has"},
		{"remote_engine_autocommits", "cypher", "fabric_participants.go", "return c.remote.RunInStatement(s.begin, s.tx, name, raw, ctx, statement, params)", "return raw.QueryCypher(ctx, statement, params)", "compositeStatement.remote no longer has"},
		{"panic_skips_rollback", "cypher", "executor_fabric.go", "\t\t\tif !ended {\n\t\t\t\t_ = tx.Rollback(nil)\n\t\t\t}\n", "", "executeViaPreparedFabricWithTx no longer has"},
		{"remote_use_unclaimed", "cypher", "fabric_participants.go", "remoteExec := exec.cloneWithStorage(s.remote(name, exec.storage))", "remoteExec := exec.cloneWithStorage(exec.storage)", "compositeStatement.on no longer has"},
		{"use_on_raw_engine", "cypher", "fabric_participants.go", "\th, err := s.participant(database, exec.storage)\n\tif err != nil {\n\t\treturn nil, ctx, err\n\t}\n\trunExec, runCtx := h.run(ctx, s)\n\treturn runExec, runCtx, nil", "\treturn exec, ctx, nil", "compositeStatement.on no longer has"},
		{"router_remote_unclaimed", "cypher", "fabric_participants.go", "return s.remote(name, engine)", "return engine", "compositeStatement.router no longer has"},
		{"router_raw_constituent", "cypher", "fabric_participants.go", "\t\treturn h.storage\n\t})", "\t\t_ = h\n\t\treturn engine\n\t})", "compositeStatement.router no longer has"},
		{"wrapper_write_unopened", "cypher", "transaction_storage_wrapper.go", "\ttx, err := w.writable()\n\tif err != nil {\n\t\treturn \"\", err\n\t}\n\tif err := w.GetSchema().RegisterTokens(node.Labels, nil); err != nil {\n\t\treturn \"\", err\n\t}", "\ttx := w.transaction()\n\tif err := w.GetSchema().RegisterTokens(node.Labels, nil); err != nil {\n\t\treturn \"\", err\n\t}", "transactionStorageWrapper.CreateNode no longer has"},
		{"fragment_on_raw_engine", "cypher", "executor_fabric.go", "exec, ctx = participant.run(ctx, composite)", "exec = c.base.cloneForStorage(engine)\n\t\t\t_ = participant", "ExecuteQueryWithRecord no longer has"},
		{"root_unrouted", "cypher", "executor_fabric.go", "exec = c.base.cloneForStorage(root).cloneWithStorage(composite.router(root))", "exec = c.base.cloneForStorage(root)", "ExecuteQueryWithRecord no longer has"},
		{"implicit_write_own_transaction", "cypher", "executor.go", "\tcomposite := compositeStatementFrom(ctx)\n\tif composite != nil {", "\tcomposite := compositeStatementFrom(ctx)\n\tif false {", "executeWithImplicitTransactionCallback no longer has"},
		{"use_switch_skips_participant", "cypher", "executor_use.go", "return composite.on(ctx, resolvedDB, scopedExec)", "return scopedExec, ctx, nil", "useDatabase no longer has"},
		{"nested_composite_commits_alone", "cypher", "executor_fabric.go", "\tif composite := compositeStatementFrom(ctx); composite != nil {\n\t\treturn e.executeNestedFabric(ctx, cypher, params, composite)\n\t}\n", "", "executeViaFabric no longer has"},
		{"procedure_statement_skips_router", "cypher", "executor_internal.go", "result, err = e.inCompositeStatement(ctx, statement, func(", "result, err = func(run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) { return run(ctx, e) }(func(", "executeUserStatement no longer has"},
		{"public_statement_skips_router", "cypher", "executor_entry.go", "return e.inCompositeStatement(ctx, cypher, func(", "return func(run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) { return run(ctx, e) }(func(", "StorageExecutor.Execute no longer has"},
		{"suppression_unclaimed", "cypher", "visibility_procedures.go", "if _, err := wrapper.writable(); err != nil {", "if err := error(nil); err != nil {", "callKpSuppress no longer has"},
		{"schema_change_unclaimed", "cypher", "schema_transaction.go", "\ttx, err := wrapper.writable()\n\tif err != nil {\n\t\treturn err\n\t}\n\tif err := tx.SetNamespace(", "\ttx := wrapper.transaction()\n\tif err := tx.SetNamespace(", "prepareSchemaTransaction no longer has"},
		{"memo_ignores_procedure_changes", "cypher", "fabric_access_mode.go", "known = known && m.generation == generation", "known = known", "accessModeMemo.decide no longer has"},
		{"remote_user_procedure_trusted", "cypher", "fabric_access_mode.go", "if !known || remote && (procedure.User || procedure.Spec.ExecutesStatements) {", "if !known || remote && procedure.Spec.ExecutesStatements {", "statementWrites no longer has"},
		{"write_clauses_ignored", "cypher", "fabric_access_mode.go", "if requirements.Write || requirements.Schema {", "if requirements.Write && requirements.Schema {", "statementWrites no longer has"},
		{"memo_not_decided", "cypher", "fabric_access_mode.go", "writes = statementWrites(query, remote)", "writes = false", "accessModeMemo.decide no longer has"},
		{"set_not_a_write", "cypher", "authorization.go", "requirements.Write = keywords[\"CREATE\"] || keywords[\"DELETE\"] || keywords[\"SET\"] ||", "requirements.Write = keywords[\"CREATE\"] || keywords[\"DELETE\"] ||", "permissionRequirements no longer has"},
		{"procedures_in_braces_missed", "cypher", "authorization.go", "\topts.SkipBraces = false\n\tvar names []string", "\topts.SkipBraces = true\n\tvar names []string", "calledProcedureNames no longer has"},
		{"run_not_marked", "cypher", "procedure_registry_builtin.go", "registerStatementProcedureLiteral(\"apoc.cypher.run\",", "registerBuiltInProcedureLiteral(\"apoc.cypher.run\",", "ensureBuiltInProceduresRegistered no longer has"},
		{"owner_not_installed", "cypher", "fabric_access_mode.go", "fabric.SetAccessMode(fragmentWrites)", "fabric.SetAccessMode(func(string, bool) bool { return false })", "access mode installed other than from fragmentWrites"},
		{"no_owner_counts_as_read", "fabric", "access_mode.go", "if mode == nil || *mode == nil {\n\t\treturn true\n\t}", "if mode == nil || *mode == nil {\n\t\treturn false\n\t}", "decideAccessMode no longer has"},
		{"remote_treated_local", "fabric", "remote_executor.go", "handle, err := r.participant(ctx, fabricTx, sub, engine, decideAccessMode(query, true))", "handle, err := r.participant(ctx, fabricTx, sub, engine, decideAccessMode(query, false))", "executeRemoteQuery no longer has"},
		{"statement_remote_treated_read", "fabric", "remote_executor.go", "handle, err := r.participant(begin, fabricTx, sub, engine, decideAccessMode(query, true))", "handle, err := r.participant(begin, fabricTx, sub, engine, false)", "RunInStatement no longer has"},
		{"remote_write_session_unclaimed", "fabric", "remote_executor.go", "\tif writes {\n\t\tif _, err := fabricTx.ClaimWrite(sub.ShardName); err != nil {\n\t\t\treturn nil, err\n\t\t}\n\t}\n", "", "RemoteFragmentExecutor.participant"},
		{"remote_read_session_commits", "fabric", "remote_executor.go", "return handle.endWithin(terminalCtx, handle.write) }", "return handle.endWithin(terminalCtx, true) }", "RemoteFragmentExecutor.participant no longer has"},
		{"remote_writer_reads_in_read_session", "fabric", "remote_executor.go", "\twrites = writes || fabricTx.WriteShard() == sub.ShardName\n", "", "RemoteFragmentExecutor.participant no longer has"},
		{"writer_committed_among_readers", "fabric", "transaction.go", "if name != t.writeShard {", "if true {", "FabricTransaction.Commit no longer has"},
		{"data_write_unadmitted", "storage", "badger_transaction_schema.go", "\treturn tx.admitWriteLocked()\n}", "\treturn nil\n}", "ensureDataWriteAllowedLocked no longer has"},
		{"create_unadmitted", "storage", "badger_transaction.go", "func (tx *BadgerTransaction) CreateNode(node *Node) (NodeID, error) {\n\ttx.mu.Lock()\n\tdefer tx.mu.Unlock()\n\n\tif err := tx.ensureDataWriteAllowedLocked(); err != nil {\n\t\treturn \"\", err\n\t}\n", "func (tx *BadgerTransaction) CreateNode(node *Node) (NodeID, error) {\n\ttx.mu.Lock()\n\tdefer tx.mu.Unlock()\n", "BadgerTransaction.CreateNode no longer has"},
		{"schema_unadmitted", "storage", "badger_transaction_schema.go", "\tif err := tx.admitWriteLocked(); err != nil {\n\t\ttx.mu.Unlock()\n\t\treturn nil, err\n\t}\n", "", "BadgerTransaction.Schema no longer has"},
		{"knowledge_policy_unadmitted", "storage", "badger_transaction_knowledge_schema.go", "\t\tif err := tx.admitWriteLocked(); err != nil {\n\t\t\treturn err\n\t\t}\n", "", "BadgerTransaction.KnowledgePolicySchema no longer has"},
		{"admission_refusal_ignored", "storage", "badger_transaction_admission.go", "\tif err := tx.writeAdmission(); err != nil {\n\t\treturn err\n\t}\n", "\t_ = tx.writeAdmission()\n", "BadgerTransaction.admitWriteLocked no longer has"},
		{"dispatch_without_resolution", "fabric", "executor.go", "loc, execParams, ctx, err := e.prepareExecDispatch(ctx, tx, f, params)", "var loc Location\n\tvar execParams map[string]interface{}\n\tvar err error", "dispatchExec no longer has"},
	}
	for _, mutation := range mutations {
		t.Run("mutation/"+mutation.name, func(t *testing.T) {
			listed := map[string]*writeAccessListed{"cypher": loader.cypher, "fabric": loader.fabric, "storage": loader.storage}[mutation.pkg]
			src := read(listed, mutation.file)
			require.Equal(t, 1, strings.Count(src, mutation.from), "mutation anchor moved in %s: %q", mutation.file, mutation.from)
			pkg := loader.parsed(listed, map[string]string{mutation.file: strings.Replace(src, mutation.from, mutation.to, 1)})
			// pkg/storage's rules read source alone.
			if listed != loader.storage {
				if typed := loader.typedOrNil(listed, map[string]string{mutation.file: strings.Replace(src, mutation.from, mutation.to, 1)}); typed != nil {
					pkg = typed
				}
			}
			require.Contains(t, strings.Join(writeAccessViolations(pkg), "\n"), mutation.want)
		})
	}
}
