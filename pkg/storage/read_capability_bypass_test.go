package storage_test

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type capabilitySource struct {
	path             string
	file             *ast.File
	info             *types.Info
	engine           *types.Interface
	stored           *types.Interface
	members          []workcensus.Member
	readTypes        bypassReadTypes
	executions       map[*types.Func]int
	executionSymbols map[string]int
}

// Discover the method population from the production interfaces, rather than
// keeping a consumer/file allowlist. Core and metadata operations retain their
// separate contracts. The execution callbacks are the only places that may
// invoke an optional data-read interface.
// Every storage interface is classified by contract. A new interface fails the
// guard until classified; operation names and consumer lists cannot hide it.
func capabilityReadMethods(t *testing.T, sources []capabilitySource) map[string]bool {
	t.Helper()
	classes := map[string]string{}
	for _, name := range strings.Fields("ContextTransactionEqualityLookup AdjacentEdgesEngine BatchNodeWithoutEmbeddingsReader EdgeHeaderReader EdgeTypeStreamer EdgesBetweenMatcher EmbeddingCountProvider EmbeddingFailureStreamer ExportableEngine IncidentEdgeLister LabelIndexEngine LabelNodeIDLookupEngine LabelStatsEngine MVCCEnumerationEngine MVCCHeadEngine MVCCIndexedVisibilityEngine MVCCLatestEffectiveEngine MVCCLifecycleDebtEngine MVCCVisibilityEngine NamespaceEdgeTypeStatsProvider NamespaceLabelStatsProvider NamespaceTemporalCurrentNodeProvider NamespaceTemporalLookupProvider NodeEmbeddingMetaReader NodeIterator NodeProjectionReader NodeWithoutEmbeddingsReader NodeWithoutEmbeddingsStreamer PendingEmbeddingCounter PrefixNodeWithoutEmbeddingsReader PrefixStatsEngine PrefixStreamingEngine ProjectedLabelNodeReader ProjectedNodeReader ProjectedPrefixNodeReader QueryNodeReader ReadSnapshotter RelationshipEndpointChecker ScopedEdgeTypeStreamer ScopedLabelNodeIDReader ScopedLabelNodeReader ScopedStoredEntityReader StoredEntityReader StreamingEngine TemporalCurrentNodeEngine TemporalLookupEngine TransactionEqualityLookup edgeHead historicalBetweenEdges historicalIncomingEdges historicalLabelNodes historicalOutgoingEdges historicalTypedEdges materializedAccess nodeHead scopedHistoricalLabelNodes scopedLabelFirstNode scopedLabelNodes scopedProjectedLabelNodes snapshotEdge snapshotNode MVCCHeadScanner MVCCVersionScanner nodeStreamer edgeStreamer nodeChunkStreamer outgoingHeaderRead incomingHeaderRead") {
		classes[name] = "read"
	}
	for _, name := range strings.Fields("BatchNodeWithoutEmbeddingsCapability EngineUnwrapper GraphMutationVersionProvider NamespaceGraphMutationVersionProvider NamespaceLister NamespacePropertyKeyLookup NamespaceSchemaProvider PropertyKeyLookup ReadCapabilityAvailability ReadCapabilityScope ReadCapabilitySource ReadCapabilityDeclaration ageProvider CapabilityOwnerSource MembershipProvider EngineBoundaryDeclaration ReadViewBindingProvider StoredReadDeclaration ReadOperationGuard QueryNodeReadBinder TemporalNodeReadBinder EdgeHeaderReadBinder ReadViewBindingPublisher") {
		classes[name] = "metadata"
	}
	for _, name := range strings.Fields("entityWriter EntityCommitAdmissionGate EntityDirectForwarder ManagedEntityEngine EntityWriteBoundary ManagedEntityRuntimeRegistry EntityCommitSubmitter StoredEntityReadBoundary ManagedEntityUsageResetter ChangeCaptureEngine ConnectedNodeDeleter EmbeddingSidecarUpdater EventSubscriber MVCCAppendEngine MVCCLifecycleController MVCCLifecycleEngine MVCCLifecycleScheduleEngine MVCCMaintenanceEngine ManualSuppressionEngine MergeNodePreparer NamespaceResetNotifier PropertyKeyRegistry StartupMaintenanceStateEngine StorageEventNotifier TemporalMaintenanceEngine accessIncrementor TransactionCommitter ObservedNodeUpdater ObservedEdgeUpdater") {
		classes[name] = "mutation"
	}
	// The snapshot lifetime finalizer is exact; its data methods are also the
	// NodeIterator/StreamingEngine contracts above. Contextual CDC is the
	// already selected physical capture owner's page operation, not an optional
	// graph data view (ChangeCaptureOf owns selection through the common cursor).
	for _, name := range strings.Fields("ReadSnapshotView ContextualChangeCaptureEngine namespacedPointMethod Engine RemoteCypherMode RemoteCypherTx RemoteParameterValue SnapshotReaderRegistry SnapshotVisitor StructuredLogger TypedPropertyValue WALLogger labelIndexReader StoredReadResult StoredReadProvider badgerKV badgerViewer batchTarget constraintGraphView embeddingSourceJudge kvWriter migrationBatch mvccHeadKeyLookup mvccHeadKeyWriter mvccVersionKeyer pendingWriteOwner pendingWriteSource remoteTransport storageEventCallback") {
		classes[name] = "internal"
	}
	methods := map[string]bool{}
	readTypes := bypassReadTypes{}
	coreTypes := bypassReadTypes{}
	core := map[string]bool{}
	for _, source := range sources {
		parent := filepath.Dir(source.path)
		if filepath.Base(parent) != "storage" || filepath.Base(filepath.Dir(parent)) != "pkg" {
			continue
		}
		for _, member := range source.members {
			if member.Syntax == nil {
				continue
			}
			spec, ok := member.Syntax.Node.(*ast.TypeSpec)
			if !ok {
				continue
			}
			object := source.info.Defs[spec.Name]
			if object == nil {
				continue
			}
			inter, ok := object.Type().Underlying().(*types.Interface)
			if !ok {
				continue
			}
			class, classified := classes[spec.Name.Name]
			if !classified {
				t.Errorf("unclassified storage interface %s", spec.Name.Name)
			}
			for i := 0; i < inter.NumMethods(); i++ {
				name := inter.Method(i).Name()
				if spec.Name.Name == "Engine" {
					core[name] = true
					coreTypes[name] = append(coreTypes[name], inter.Method(i).Type().(*types.Signature))
				}
				if class == "read" {
					methods[name] = true
					readTypes[name] = append(readTypes[name], inter.Method(i).Type().(*types.Signature))
				}
			}
		}
	}
	for name := range core {
		var retained []*types.Signature
		for _, candidate := range readTypes[name] {
			ordinary := false
			for _, contract := range coreTypes[name] {
				ordinary = ordinary || sameBypassSignature(candidate, 0, contract)
			}
			if !ordinary {
				retained = append(retained, candidate)
			}
		}
		readTypes[name] = retained
		if len(retained) == 0 {
			delete(methods, name)
		}
	}
	executions, symbols := bypassExecutionOwners(sources)
	for i := range sources {
		sources[i].readTypes = readTypes
		sources[i].executions = executions
		sources[i].executionSymbols = symbols
	}
	return methods
}

func executionCallName(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	case *ast.IndexExpr:
		return executionCallName(x.X)
	case *ast.IndexListExpr:
		return executionCallName(x.X)
	}
	return ""
}

func capabilityBypasses(fset *token.FileSet, source capabilitySource, methods map[string]bool) []string {
	if source.info != nil {
		return canonicalCapabilityBypasses(fset, source, methods)
	}
	var found []string
	report := func(node ast.Node, reason string) {
		found = append(found, fmt.Sprintf("%s: %s", fset.Position(node.Pos()), reason))
	}
	imports := map[string]bool{}
	for _, imp := range source.file.Imports {
		if imp.Name != nil {
			imports[imp.Name.Name] = true
		} else {
			parts := strings.Split(strings.Trim(imp.Path.Value, "\""), "/")
			imports[parts[len(parts)-1]] = true
		}
	}
	safe := map[*ast.FuncLit]bool{}
	controlled := map[types.Object]bool{}
	ast.Inspect(source.file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			switch executionCallName(call.Fun) {
			case "ReadWithCapability", "ReadValueWithCapability", "VisitWithCapability", "VisitWhileWithCapability", "visitCompositeRead", "ExecuteBoundRead":
				// Selection and delivery are owned by these functions. Only the optional
				// operation callback (argument 1) receives a capability.
				if len(call.Args) > 1 {
					if fn, ok := call.Args[1].(*ast.FuncLit); ok {
						safe[fn] = true
						if source.info != nil && len(fn.Type.Params.List) > 0 {
							for _, id := range fn.Type.Params.List[0].Names {
								controlled[source.info.Defs[id]] = true
							}
						}
					}
				}
			}
		}
		return true
	})
	ownerFile := filepath.Base(source.path) == "read_capability.go" || filepath.Base(source.path) == "read_operation.go"
	var receiver string
	var walk func(ast.Node, int)
	walk = func(node ast.Node, depth int) {
		if node == nil {
			return
		}
		if lit, ok := node.(*ast.FuncLit); ok && safe[lit] && source.info == nil {
			return
		} // mutation positive control
		switch x := node.(type) {
		case *ast.Ident:
			if x.Name == "ReadCapabilityForEngine" || (!ownerFile && x.Name == "readCapabilityForEngine") {
				report(x, "raw capability discovery outside execution owner")
			}
		case *ast.SelectorExpr:
			if methods[x.Sel.Name] {
				if source.info != nil {
					selected := source.info.Selections[x]
					if selected == nil {
						return
					}
					recv := selected.Recv()
					_, isInterface := recv.Underlying().(*types.Interface)
					concrete := recv
					if ptr, ok := concrete.(*types.Pointer); ok {
						concrete = ptr.Elem()
					}
					named, isNamed := concrete.(*types.Named)
					storageType := isNamed && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "github.com/orneryd/nornicdb/pkg/storage"
					if !isInterface && !storageType && !types.Implements(recv, source.engine) {
						return
					}
					if id, ok := x.X.(*ast.Ident); ok && controlled[source.info.Uses[id]] {
						return
					}
				}
				id, ident := x.X.(*ast.Ident)
				// A provider can reuse its own implementation. Package-level execution
				// helpers are calls into an owner, not calls to an interface method.
				if !(ident && (imports[id.Name] || id.Name == receiver)) {
					report(x, "direct optional read "+x.Sel.Name)
				}
			}
		}
		ast.Inspect(node, func(child ast.Node) bool {
			if child == node {
				return true
			}
			walk(child, depth+1)
			return false
		})
	}
	for _, decl := range source.file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		receiver = ""
		if fn.Recv != nil && len(fn.Recv.List) > 0 && len(fn.Recv.List[0].Names) > 0 {
			receiver = fn.Recv.List[0].Names[0].Name
		}
		if fn.Name.Name == "ReadCapabilityForEngine" {
			report(fn, "exported raw capability discovery")
		}
		walk(fn, 0)
	}
	return found
}

func TestReadCapabilityBypassGuard(t *testing.T) {
	var sources []capabilitySource
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	loaded, fset, err := capabilitycensus.Packages("../..")
	require.NoError(t, err)
	require.NotEmpty(t, loaded)
	var engine, stored *types.Interface
	for _, pkg := range loaded {
		if pkg.PkgPath == "github.com/orneryd/nornicdb/pkg/storage" {
			engine = pkg.Types.Scope().Lookup("Engine").Type().Underlying().(*types.Interface)
			stored = pkg.Types.Scope().Lookup("StoredReadProvider").Type().Underlying().(*types.Interface)
		}
	}
	require.NotNil(t, engine)
	byFile := map[string][]workcensus.Member{}
	for _, member := range result.Inventory.Members {
		if member.Syntax != nil && member.Syntax.Info != nil {
			path := fset.Position(member.Syntax.Node.Pos()).Filename
			byFile[path] = append(byFile[path], member)
		}
	}
	for _, pkg := range loaded {
		require.Empty(t, pkg.Errors, "guard requires complete type information: %s", pkg.PkgPath)
		for _, file := range pkg.Syntax {
			source := capabilitySource{path: fset.Position(file.Pos()).Filename, file: file, info: pkg.TypesInfo, engine: engine, stored: stored}
			source.members = byFile[source.path]
			sources = append(sources, source)
		}
	}
	methods := capabilityReadMethods(t, sources)
	require.True(t, methods["StreamParkedEmbeddingFailures"])
	require.True(t, methods["MatchEdgesBetween"])
	require.True(t, methods["IsCurrentTemporalNode"])
	require.False(t, methods["Close"], "snapshot finalization is an exact lifecycle operation")
	var found []string
	for _, source := range sources {
		found = append(found, capabilityBypasses(fset, source, methods)...)
	}
	sort.Strings(found)
	for _, finding := range found {
		t.Error(finding)
	}
	require.Empty(t, found, "optional data reads must execute inside the shared owner")
}

func TestReadCapabilityBypassMutationControl(t *testing.T) {
	methods := map[string]bool{"IsCurrentTemporalNode": true, "StreamParkedEmbeddingFailures": true, "MatchEdgesBetween": true}
	for _, source := range []string{
		`package consumer; func bypass(e Engine){r,_:=ReadCapabilityForEngine[Reader](e);r.Read()}`,
		`package consumer; func bypass(r Reader){r.IsCurrentTemporalNode(nil,at)}`,
		`package consumer; func bypass(r Reader){r.StreamParkedEmbeddingFailures(ctx,visitor)}`,
		`package consumer; func bypass(r Reader){r.MatchEdgesBetween(a,b,kind,nil,predicate)}`,
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "mutation.go", source, 0)
		require.NoError(t, err)
		require.NotEmpty(t, capabilityBypasses(fset, capabilitySource{path: "mutation.go", file: file}, methods))
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "owner.go", `package consumer; func read(e Engine){ReadWithCapability(e,func(r Reader)(bool,error){return r.IsCurrentTemporalNode(nil,at)},nil)}`, 0)
	require.NoError(t, err)
	require.Empty(t, capabilityBypasses(fset, capabilitySource{path: "owner.go", file: file}, methods))
}

// The common engine enumerates declarations, calls and identifiers once. This
// client checks receiver ownership and execution-callback provenance on those
// typed members; it does not traverse production syntax again.
func canonicalCapabilityBypasses(fset *token.FileSet, source capabilitySource, methods map[string]bool) []string {
	var found []string
	report := func(node ast.Node, reason string) {
		found = append(found, fmt.Sprintf("%s: %s", fset.Position(node.Pos()), reason))
	}
	controlled := map[types.Object]bool{}
	receivers := map[types.Object]bool{}
	for _, member := range source.members {
		switch node := member.Syntax.Node.(type) {
		case *ast.FuncDecl:
			if node.Recv != nil {
				for _, field := range node.Recv.List {
					for _, name := range field.Names {
						receivers[source.info.Defs[name]] = true
					}
				}
			}
		case *ast.CallExpr:
			argument, owned := bypassCallbackArgument(member, source, node)
			if !owned || len(node.Args) <= argument {
				continue
			}
			callback, ok := node.Args[argument].(*ast.FuncLit)
			if !ok || len(callback.Type.Params.List) == 0 {
				continue
			}
			for _, name := range callback.Type.Params.List[0].Names {
				controlled[source.info.Defs[name]] = true
			}
		}
	}
	ownerFile := filepath.Base(source.path) == "read_capability.go" || filepath.Base(source.path) == "read_operation.go"
	for _, member := range source.members {
		id, ok := member.Syntax.Node.(*ast.Ident)
		if !ok {
			continue
		}
		object := source.info.Uses[id]
		if object == nil {
			object = source.info.Defs[id]
		}
		if function, ok := object.(*types.Func); ok && function.Pkg() != nil && function.Pkg().Path() == "github.com/orneryd/nornicdb/pkg/storage" {
			// Compare the actual raw API definition, not an unrelated same-named call.
			exported, _ := function.Pkg().Scope().Lookup("ReadCapabilityForEngine").(*types.Func)
			private, _ := function.Pkg().Scope().Lookup("readCapabilityForEngine").(*types.Func)
			if exported != nil && function.Origin() == exported.Origin() || !ownerFile && private != nil && function.Origin() == private.Origin() {
				report(id, "raw capability discovery outside execution owner")
			}
		}
		if !methods[id.Name] || len(member.Syntax.Ancestors) == 0 {
			continue
		}
		selector, ok := member.Syntax.Ancestors[len(member.Syntax.Ancestors)-1].(*ast.SelectorExpr)
		if !ok || selector.Sel != id {
			continue
		}
		selected := source.info.Selections[selector]
		if selected == nil {
			continue
		}
		if !source.readTypes.matches(selected) {
			continue
		}
		// An interface invocation selects a structural capability. A concrete
		// invocation belongs to this guard when its receiver is an actual Engine
		// or declared narrow stored view. A DB/application facade with the same
		// scalar signature is not thereby an optional storage provider. The
		// canonical census still inventories that call for semantic review.
		_, narrow := selected.Recv().Underlying().(*types.Interface)
		dataView := source.engine != nil && types.Implements(selected.Recv(), source.engine) || source.stored != nil && types.Implements(selected.Recv(), source.stored)
		if !narrow && !dataView {
			continue
		}
		var receiver ast.Expr = selector.X
		if selected.Kind() == types.MethodExpr {
			ancestors := member.Syntax.Ancestors
			if len(ancestors) < 2 {
				continue
			}
			call, ok := ancestors[len(ancestors)-2].(*ast.CallExpr)
			if !ok || call.Fun != selector {
				continue
			} // retained data is separately reviewed
			if len(call.Args) == 0 {
				report(selector, "unresolved optional method-expression receiver")
				continue
			}
			receiver = call.Args[0]
		}
		if receiver, ok := receiver.(*ast.Ident); ok && (controlled[source.info.Uses[receiver]] || receivers[source.info.Uses[receiver]]) {
			continue
		}
		report(selector, "direct optional read "+selector.Sel.Name)
	}
	sort.Strings(found)
	return found
}

func TestReadCapabilityBypassMethodExpressionControl(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module methodexpression\n\ngo 1.26\n"), 0644))
	source := `package methodexpression
 type Engine interface{Core()}
 type Reader interface{IsCurrentTemporalNode()(bool,error)}
 func retained()func(Reader)(bool,error){return Reader.IsCurrentTemporalNode}
 func bypass(r Reader)(bool,error){return Reader.IsCurrentTemporalNode(r)}
 `
	require.NoError(t, os.WriteFile(filepath.Join(root, "source.go"), []byte(source), 0644))
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	var view capabilitySource
	view.path = filepath.Join(root, "source.go")
	view.members = inventory.Members
	var fset *token.FileSet
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		view.info = member.Syntax.Info
		fset = member.Syntax.FileSet
		if file, ok := member.Syntax.Node.(*ast.File); ok {
			view.file = file
		}
		if spec, ok := member.Syntax.Node.(*ast.TypeSpec); ok && spec.Name.Name == "Engine" {
			view.engine = member.Syntax.Info.Defs[spec.Name].Type().Underlying().(*types.Interface)
		}
		if spec, ok := member.Syntax.Node.(*ast.TypeSpec); ok && spec.Name.Name == "Reader" {
			contract := member.Syntax.Info.Defs[spec.Name].Type().Underlying().(*types.Interface)
			view.readTypes = bypassReadTypes{}
			for i := 0; i < contract.NumMethods(); i++ {
				method := contract.Method(i)
				view.readTypes[method.Name()] = append(view.readTypes[method.Name()], method.Type().(*types.Signature))
			}
		}
	}
	require.NotNil(t, view.file)
	require.NotNil(t, view.engine)
	problems := canonicalCapabilityBypasses(fset, view, map[string]bool{"IsCurrentTemporalNode": true})
	require.Len(t, problems, 1, "retaining a method expression is distinct from invoking it outside the owner")
	require.Contains(t, problems[0], "direct optional read IsCurrentTemporalNode")
}

// Re-extract the same complete typed population after a new outside-package
// consumer is planted. Same spelling cannot authorize its callbacks, and a
// different service signature cannot become a storage read by spelling alone.
func TestReadCapabilityBypassTypedOutsidePackageControl(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/orneryd/nornicdb\n\ngo 1.26\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "reader.go"), []byte(`package bypasscontrol
 type Engine interface{Core()}
 type Reader interface{IsCurrentTemporalNode()(bool,error)}
 func Retained()func(Reader)(bool,error){return Reader.IsCurrentTemporalNode}
 `), 0644))
	storageDirectory := filepath.Join(root, "pkg", "storage")
	require.NoError(t, os.MkdirAll(storageDirectory, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(storageDirectory, "owner.go"), []byte(`package storage
 import "context"
 type Engine interface{Core()}
	 type StoredReadProvider interface{ReadStored(context.Context)error}
 func VisitWithCapability[T,Row any](ctx context.Context,engine Engine,read func(T,func(Row)error)error,fallback func(func(Row)error)error,visit func(Row)error)error{return nil}
 `), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "controlled.go"), []byte(`package bypasscontrol
 import("context"; "github.com/orneryd/nornicdb/pkg/storage")
 func directOwner(engine storage.Engine)error{return storage.VisitWithCapability(context.Background(),engine,func(reader Reader,deliver func(bool)error)error{value,err:=reader.IsCurrentTemporalNode();if err!=nil{return err};return deliver(value)},nil,func(bool)error{return nil})}
 func aliasOwner(engine storage.Engine)error{invoke:=storage.VisitWithCapability[Reader,bool];return invoke(context.Background(),engine,func(reader Reader,deliver func(bool)error)error{value,err:=reader.IsCurrentTemporalNode();if err!=nil{return err};return deliver(value)},nil,func(bool)error{return nil})}
 `), 0644))
	inspect := func() []string {
		inventory, err := workcensus.Extract(capabilitycensus.Config(root))
		require.NoError(t, err)
		catalog := bypassReadTypes{}
		files := map[string]*capabilitySource{}
		var fset *token.FileSet
		var engine, stored *types.Interface
		for _, member := range inventory.Members {
			if member.Syntax == nil || member.Syntax.Info == nil {
				continue
			}
			syntax := member.Syntax
			fset = syntax.FileSet
			path := fset.Position(syntax.Node.Pos()).Filename
			view := files[path]
			if view == nil {
				view = &capabilitySource{path: path, info: syntax.Info}
				files[path] = view
			}
			view.members = append(view.members, member)
			if file, ok := syntax.Node.(*ast.File); ok {
				view.file = file
			}
			if spec, ok := syntax.Node.(*ast.TypeSpec); ok && spec.Name.Name == "Reader" {
				contract := syntax.Info.Defs[spec.Name].Type().Underlying().(*types.Interface)
				for i := 0; i < contract.NumMethods(); i++ {
					method := contract.Method(i)
					catalog[method.Name()] = append(catalog[method.Name()], method.Type().(*types.Signature))
				}
			}
			if spec, ok := syntax.Node.(*ast.TypeSpec); ok && syntax.Package.Path() == "github.com/orneryd/nornicdb/pkg/storage" && spec.Name.Name == "StoredReadProvider" {
				stored = syntax.Info.Defs[spec.Name].Type().Underlying().(*types.Interface)
			}
			if spec, ok := syntax.Node.(*ast.TypeSpec); ok && syntax.Package.Path() == "github.com/orneryd/nornicdb/pkg/storage" && spec.Name.Name == "Engine" {
				engine = syntax.Info.Defs[spec.Name].Type().Underlying().(*types.Interface)
			}
		}
		require.NotEmpty(t, catalog)
		var problems []string
		var sources []capabilitySource
		for _, view := range files {
			sources = append(sources, *view)
		}
		owners, symbols := bypassExecutionOwners(sources)
		require.NotEmpty(t, owners, "actual owner objects must be found in the typed fixture")
		for _, view := range files {
			view.readTypes = catalog
			view.engine = engine
			view.stored = stored
			view.executions = owners
			view.executionSymbols = symbols
			problems = append(problems, canonicalCapabilityBypasses(fset, *view, map[string]bool{"IsCurrentTemporalNode": true})...)
		}
		sort.Strings(problems)
		return problems
	}
	require.Empty(t, inspect())
	directory := filepath.Join(root, "plugins", "unfamiliar")
	require.NoError(t, os.MkdirAll(directory, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "consumer.go"), []byte(`package unfamiliar
 import original "github.com/orneryd/nornicdb"
	 import "context"
 func ReadWithCapability[R any](owner original.Reader,read func(original.Reader)(R,error),fallback func()(R,error))(R,error){return read(owner)}
 func direct(owner original.Reader)(bool,error){return ReadWithCapability(owner,func(reader original.Reader)(bool,error){return reader.IsCurrentTemporalNode()},nil)}
 func alias(owner original.Reader)(bool,error){invoke:=ReadWithCapability[bool];return invoke(owner,func(reader original.Reader)(bool,error){return reader.IsCurrentTemporalNode()},nil)}
 type Service struct{}
 func(Service)Core(){}
 func(Service)IsCurrentTemporalNode(int)string{return "service"}
 func service(owner Service)string{return owner.IsCurrentTemporalNode(1)}
	 type Facade struct{}
	 func(Facade)IsCurrentTemporalNode()(bool,error){return true,nil}
	 func facade(owner Facade)(bool,error){return owner.IsCurrentTemporalNode()}
	 type NarrowView struct{}
	 func(NarrowView)ReadStored(ctx context.Context)error{return ctx.Err()}
	 func(NarrowView)IsCurrentTemporalNode()(bool,error){return true,nil}
	 func narrow(owner NarrowView)(bool,error){return owner.IsCurrentTemporalNode()}
	 type EngineView struct{}
	 func(EngineView)Core(){}
	 func(EngineView)IsCurrentTemporalNode()(bool,error){return true,nil}
	 func concrete(owner EngineView)(bool,error){return owner.IsCurrentTemporalNode()}
 `), 0644))
	problems := inspect()
	require.Len(t, problems, 4, "both fake-owner callbacks, the actual narrow view and concrete Engine must fail; application facades do not become providers by scalar signature")
	for _, problem := range problems {
		require.Contains(t, filepath.ToSlash(problem), "plugins/unfamiliar/consumer.go")
		require.Contains(t, problem, "direct optional read IsCurrentTemporalNode")
	}
}
