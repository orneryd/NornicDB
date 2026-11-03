// Statement context for APOC's whole-graph reads.
//
// The storage contract's bulk read is context-first. Every APOC helper that
// reaches it from a statement takes that statement's context first, so the
// Cypher function adapter (which binds a context-first handler's first
// parameter) supplies the caller's lifetime. The procedure-classified helpers
// that the bundled plugin does not implement have no statement path at all;
// they say so where they read and use an explicit background lifetime.
package apoc

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// apocBulkReadContextCallers are the APOC helpers that reach a whole-graph
// read (AllNodes or AllRelationships) from a supported statement path: the
// plugin exports them as scalar functions, or a supported export calls them.
// Each takes the statement's
// context first and hands it to every whole-graph read it makes.
var apocBulkReadContextCallers = map[string][]string{
	"label":    {"Count", "Exists", "List", "Nodes", "Search", "Stats"},
	"meta":     {"NodeLabels", "PropertyKeys", "RelTypes", "Schema"},
	"refactor": {"RenameLabel", "RenameProperty", "RenameType"},
	"schema":   {"Info", "Labels", "PropertiesDistinct", "Stats", "Types"},
	"search": {
		"Contains", "DidYouMean", "Exists", "FullText", "Fuzzy", "In", "Match", "Missing",
		"MultiSearchAll", "MultiSearchAny", "Node", "NodeAll", "NodeAny", "NodeReduced",
		"NotIn", "NotNull", "Null", "Parallel", "Prefix", "Range", "Regex", "Suggest",
		"Suffix",
	},
	"warmup": {"Indexes", "Nodes", "Properties", "Progress", "Relationships", "Run", "RunWithParams"},
}

// apocBulkReadProcedureCallers are the APOC helpers that reach the
// whole-graph read with no statement path: apoc/plugin_exports.go classifies
// each name as procedure-only, and docs/features/apoc-procedure-capabilities.md
// records that the bundled runtime and plugin implement none of those
// procedures. Their only entry is a Go caller (apoc.Call or a direct call),
// which carries no caller context, so their read states its background
// lifetime where it reads instead of taking a context nothing could supply.
var apocBulkReadProcedureCallers = map[string][]string{
	"meta":   {"Graph", "Stats"},
	"schema": {"Nodes", "Relationships"},
	"stats":  {"Degrees"},
}

// apocBulkReadInternalOwners are the read-reaching helpers that no registered
// APOC name reaches directly: a package's own scan owner, which the registered
// helpers above call with the caller's context. One owner serves every helper of
// the package, so the population records it once instead of attributing the
// read back to whichever helper happened to call it; a name that answers from a
// forwarding duplicate of the owner stays unregistered rather than being counted
// as a supported function.
//
// apoc/search's owner is Functions.newResultScan, the one helper that reaches the
// whole-graph read: it takes the caller's context first. The traversal that
// operation methods run caches that caller's Done channel once. selectLabels reaches no
// read of its own - it calls only the single-label search its caller hands it -
// so the extractor below attributes the read to this owner alone.
var apocBulkReadInternalOwners = map[string][]string{
	"search": {"newResultScan"},
}

// apocBulkReadGoOnlyCallers are the APOC helpers that reach a whole-graph read
// with neither a statement path nor a registered APOC name: apoc/apoc.go
// registers no name for them, so no statement and no apoc.Call reaches them and
// nothing can supply a context. Their whole-graph reads therefore name an
// explicit background lifetime where they read, and their Go signatures stay
// unchanged.
var apocBulkReadGoOnlyCallers = map[string][]string{
	"refactor": {"RenameRelProperty"},
}

// apocPackageSource is one apoc package's parsed source: the local functions
// each function calls, whether it reads the whole graph itself, and its body.
type apocPackageSource struct {
	calls     map[string]map[string]bool
	direct    map[string]bool
	bodies    map[string]string
	receivers map[string]string
	contexts  map[string]bool
}

// apocSourceInventory parses every apoc package and returns, per package
// directory, the functions that reach a whole-graph read, directly or through
// another function of the same package. It is the repeatable extractor for the
// two pinned populations above: a new whole-graph helper fails this test until
// its context is declared.
func apocSourceInventory(t *testing.T) (map[string][]string, map[string]apocPackageSource) {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	inventory := map[string][]string{}
	sources := map[string]apocPackageSource{}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "plugin-src" {
			continue
		}
		source := apocParsePackage(t, entry.Name())
		sources[entry.Name()] = source
		if len(source.direct) == 0 {
			continue
		}
		reaching := map[string]bool{}
		for name := range source.direct {
			reaching[name] = true
		}
		for changed := true; changed; {
			changed = false
			for caller, callees := range source.calls {
				if reaching[caller] {
					continue
				}
				for callee := range callees {
					if reaching[callee] {
						reaching[caller] = true
						changed = true
						break
					}
				}
			}
		}
		names := make([]string, 0, len(reaching))
		for name := range reaching {
			names = append(names, name)
		}
		sort.Strings(names)
		inventory[entry.Name()] = names
	}
	return inventory, sources
}

// apocParsePackage parses one package directory's non-test Go files. Bound
// helpers are methods, so method declarations are part of the population, and a
// call through the instance (f.Prefix(...)) is an intra-package call like any
// other.

func apocParsePackage(t *testing.T, dir string) apocPackageSource {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	source := apocPackageSource{
		calls:     map[string]map[string]bool{},
		direct:    map[string]bool{},
		bodies:    map[string]string{},
		receivers: map[string]string{},
		contexts:  map[string]bool{},
	}
	declared := map[string]bool{}
	fset := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		file, err := parser.ParseFile(fset, path, contents, 0)
		require.NoError(t, err, path)
		imports := apocImportNames(file)
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			name := function.Name.Name
			declared[name] = true
			source.receivers[name] = apocReceiverName(function)
			source.contexts[name] = apocDeclaresCallerContext(function.Type)
			callees := source.calls[name]
			if callees == nil {
				callees = map[string]bool{}
				source.calls[name] = callees
			}
			start := fset.Position(function.Body.Pos()).Offset
			end := fset.Position(function.Body.End()).Offset
			source.bodies[name] = string(contents[start:end])
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					callees[fun.Name] = true
				case *ast.SelectorExpr:
					if fun.Sel.Name == "AllNodes" || fun.Sel.Name == "AllRelationships" {
						source.direct[name] = true
					}
					// A bound helper is called through its instance (f.Prefix(...)), which
					// is an intra-package call; a qualified call on an imported package
					// is not.
					if receiver, isIdent := fun.X.(*ast.Ident); isIdent && !imports[receiver.Name] {
						callees[fun.Sel.Name] = true
					}
				}
				return true
			})
		}
	}
	for _, callees := range source.calls {
		for callee := range callees {
			if !declared[callee] {
				delete(callees, callee)
			}
		}
	}
	return source
}

// apocReceiverName returns the receiver type a declared method binds to, or the
// empty string for a free function. The census resolves owners from this, so it
// never has to assume that every instance type shares one name.
func apocReceiverName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return ""
	}
	return apocReceiverTypeName(function.Recv.List[0].Type)
}

func apocReceiverTypeName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.StarExpr:
		return apocReceiverTypeName(typed.X)
	case *ast.Ident:
		return typed.Name
	case *ast.IndexExpr:
		return apocReceiverTypeName(typed.X)
	case *ast.IndexListExpr:
		return apocReceiverTypeName(typed.X)
	}
	return ""
}

// apocDeclaresCallerContext reports whether a declared function takes the
// caller's context as its first parameter, which is the signature the Cypher
// adapter binds a statement lifetime to.
func apocDeclaresCallerContext(functionType *ast.FuncType) bool {
	if functionType == nil || functionType.Params == nil || len(functionType.Params.List) == 0 {
		return false
	}
	selector, ok := functionType.Params.List[0].Type.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	qualified, ok := selector.X.(*ast.Ident)
	return ok && qualified.Name == "context" && selector.Sel.Name == "Context"
}

// apocImportNames returns the local names a parsed file uses for its imports, so
// a qualified call (storage.AllNodes, strings.Split) is not mistaken for a call
// to a helper of this package.
func apocImportNames(file *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, spec := range file.Imports {
		if spec.Name != nil {
			names[spec.Name.Name] = true
			continue
		}
		path := strings.Trim(spec.Path.Value, "\"")
		if at := strings.LastIndex(path, "/"); at >= 0 {
			path = path[at+1:]
		}
		names[path] = true
	}
	return names
}

// apocRegisteredSource is a registered APOC name and the admitted handler the
// apoc package function behind it was compiled into.
type apocRegisteredSource struct {
	name      string
	directory string
	function  string
	handler   *registry.Callable
}

// apocRegisteredSources returns every registered name whose handler is an
// apoc/<package> helper that this library declares. The owner is resolved from
// the admitted handler's own runtime symbol and then verified against the parsed
// source, so a name is only ever attributed to a helper whose declaration exists
// - not to any callable that happens to carry a similar symbol.
func apocRegisteredSources(t *testing.T) []apocRegisteredSource {
	t.Helper()
	require.NoError(t, Initialize(apocstorage.NewInMemoryStorage(), nil))
	_, parsed := apocSourceInventory(t)
	names := List()
	sort.Strings(names)
	sources := make([]apocRegisteredSource, 0, len(names))
	for _, name := range names {
		info, found := GetInfo(name)
		require.True(t, found, name)
		require.NotNil(t, info.Handler, name)
		symbol := info.Handler.Symbol()
		at := strings.LastIndex(symbol, "/apoc/")
		if at < 0 {
			continue
		}
		directory, function, resolved := apocSymbolTarget(symbol[at+len("/apoc/"):], parsed)
		require.True(t, resolved,
			"%s is bound to %q, which must name an apoc helper the package source declares", name, symbol)
		sources = append(sources, apocRegisteredSource{
			name:      name,
			directory: directory,
			function:  function,
			handler:   info.Handler,
		})
	}
	return sources
}

// apocClosureOwner maps a closure symbol (load.JSONItems.func1, including
// nested .funcN.M forms) to its enclosing declared function, the source owner
// of a handler returned by a factory.
func apocClosureOwner(symbol string) string {
	for {
		dot := strings.LastIndex(symbol, ".")
		if dot <= 0 {
			return symbol
		}
		last := strings.TrimPrefix(symbol[dot+1:], "func")
		if last == "" || strings.Trim(last, "0123456789") != "" {
			return symbol
		}
		symbol = symbol[:dot]
	}
}

// apocSymbolTarget maps one admitted handler symbol to the apoc package
// directory and the helper the parsed source declares behind it. A bound helper
// is a method value (apoc/search.(*Functions).Prefix-fm,
// apoc/meta.(*BoundFunctions).Schema-fm); a free helper is a plain function
// (apoc/coll.Sum). The receiver type is read from the source, so the census does
// not assume that every instance type shares one name.
func apocSymbolTarget(rest string, sources map[string]apocPackageSource) (directory, function string, ok bool) {
	if receiverAt := strings.Index(rest, ".("); receiverAt > 0 {
		method, bound, found := strings.Cut(rest[receiverAt+2:], ").")
		if !found {
			return "", "", false
		}
		directory = rest[:receiverAt]
		receiver := strings.TrimPrefix(method, "*")
		function = strings.TrimSuffix(bound, "-fm")
		if receiver == "" || function == "" || strings.Contains(directory, "/") {
			return "", "", false
		}
		source, parsed := sources[directory]
		if !parsed || source.receivers[function] != receiver {
			return "", "", false
		}
		return directory, function, true
	}
	rest = apocClosureOwner(rest)
	dot := strings.LastIndex(rest, ".")
	if dot <= 0 || strings.Contains(rest[:dot], "/") {
		return "", "", false
	}
	directory, function = rest[:dot], strings.TrimSuffix(rest[dot+1:], "-fm")
	source, parsed := sources[directory]
	if !parsed {
		return "", "", false
	}
	if receiver, declared := source.receivers[function]; !declared || receiver != "" {
		return "", "", false
	}
	return directory, function, true
}

// apocPinnedSource returns the extracted, pinned population as the extractor
// returns it: one sorted function list per package directory.
func apocPinnedSource() map[string][]string {
	pinned := map[string][]string{}
	for directory, functions := range apocBulkReadContextCallers {
		pinned[directory] = append(pinned[directory], functions...)
	}
	for directory, functions := range apocBulkReadProcedureCallers {
		pinned[directory] = append(pinned[directory], functions...)
	}
	for directory, functions := range apocBulkReadGoOnlyCallers {
		pinned[directory] = append(pinned[directory], functions...)
	}
	for directory, functions := range apocBulkReadInternalOwners {
		pinned[directory] = append(pinned[directory], functions...)
	}
	for directory := range pinned {
		sort.Strings(pinned[directory])
	}
	return pinned
}

// apocPinnedHandlers maps a directory and function to its one registered name.
func apocPinnedHandlers(sources []apocRegisteredSource) map[string]map[string]apocRegisteredSource {
	handlers := map[string]map[string]apocRegisteredSource{}
	for _, source := range sources {
		if handlers[source.directory] == nil {
			handlers[source.directory] = map[string]apocRegisteredSource{}
		}
		handlers[source.directory][source.function] = source
	}
	return handlers
}

// apocStatementHelpers returns the registered handlers of the pinned
// context-first population, sorted by name.
func apocStatementHelpers(t *testing.T, handlers map[string]map[string]apocRegisteredSource) []apocRegisteredSource {
	t.Helper()
	var helpers []apocRegisteredSource
	for directory, functions := range apocBulkReadContextCallers {
		for _, function := range functions {
			source, found := handlers[directory][function]
			require.True(t, found, "%s.%s must be registered", directory, function)
			helpers = append(helpers, source)
		}
	}
	sort.Slice(helpers, func(i, j int) bool { return helpers[i].name < helpers[j].name })
	return helpers
}

// TestAPOCBulkReadPopulationCoversEveryHelper extracts every APOC helper that
// reaches the whole-graph read and pins the complete population, so a new
// helper is a visible decision rather than a silent context gap.
func TestAPOCBulkReadPopulationCoversEveryHelper(t *testing.T) {
	inventory, _ := apocSourceInventory(t)
	require.Equal(t, apocPinnedSource(), inventory,
		"the pinned whole-graph helper population must cover every extracted helper")
	require.NotEmpty(t, inventory, "the extractor must find the whole-graph helpers")
}

// TestAPOCStatementHelpersTakeCallerContext checks both statement handlers and
// native Go helpers for caller-owned contexts. Procedure classification and
// bundled-runtime capability gaps remain separate from the Go API contract.
func TestAPOCStatementHelpersTakeCallerContext(t *testing.T) {
	inventory, sources := apocSourceInventory(t)
	require.Equal(t, apocPinnedSource(), inventory)
	handlers := apocPinnedHandlers(apocRegisteredSources(t))

	for _, helper := range apocStatementHelpers(t, handlers) {
		require.NotNil(t, helper.handler.Signature(), helper.name)
		require.True(t, apocHandlerTakesCallerContext(helper.handler.Signature()),
			"%s takes the statement context first", helper.name)
		// The owner the runtime symbol named must also be a helper this package
		// declares, with the context-first signature: a name is reachable only
		// through a real declared owner, not through an anonymous callable.
		require.Contains(t, sources[helper.directory].bodies, helper.function,
			"%s is bound to %s.%s, which the package source must declare",
			helper.name, helper.directory, helper.function)
		require.True(t, sources[helper.directory].contexts[helper.function],
			"%s.%s is declared with the caller context first", helper.directory, helper.function)
	}

	for directory, functions := range apocBulkReadInternalOwners {
		for _, function := range functions {
			_, registered := handlers[directory][function]
			require.False(t, registered,
				"%s.%s is the package's own scan owner, so no APOC name answers from it directly",
				directory, function)
			require.True(t, sources[directory].contexts[function],
				"%s.%s takes the caller context first, like every helper that calls it",
				directory, function)
			reachesRead := sources[directory].direct[function]
			for callee := range sources[directory].calls[function] {
				if sources[directory].direct[callee] {
					reachesRead = true
				}
			}
			require.True(t, reachesRead,
				"%s.%s is a whole-graph reader, directly or through another helper of the package",
				directory, function)
		}
	}

	for directory, functions := range apocBulkReadProcedureCallers {
		for _, function := range functions {
			helper, found := handlers[directory][function]
			require.True(t, found, "%s.%s must be registered", directory, function)
			require.True(t, isProcedureName(helper.name), "procedure classification is unchanged")
			require.NotContains(t, PluginFunctionNames(), helper.name)
			require.Contains(t, apocProcedureAuditGaps(t), helper.name)
			require.True(t, apocHandlerTakesCallerContext(helper.handler.Signature()), "the native Go helper now requires its caller context")
			require.True(t, sources[directory].contexts[function])
			require.NotContains(t, sources[directory].bodies[function], "context.Background()")
		}
	}

	for directory, functions := range apocBulkReadGoOnlyCallers {
		for _, function := range functions {
			_, registered := handlers[directory][function]
			require.False(t, registered,
				"%s.%s must have no registered APOC name to be a Go-only entry", directory, function)
			require.True(t, sources[directory].contexts[function], "%s.%s requires its native Go caller context", directory, function)
			require.NotContains(t, sources[directory].bodies[function], "context.Background()")
		}
	}

	scalar := map[string]bool{}
	for _, name := range PluginFunctionNames() {
		scalar[name] = true
	}
	helpers := apocStatementHelpers(t, handlers)
	supported := 0
	for _, helper := range helpers {
		if scalar[helper.name] {
			supported++
		}
	}
	require.Positive(t, supported,
		"a context-first population that no statement can reach would prove nothing")
	t.Logf("context-first whole-graph helpers: %d, of which %d are statement-reachable scalar exports",
		len(helpers), supported)
}

// apocProcedureAuditGaps returns the names docs/features/apoc-procedure-capabilities.md
// records as procedure-kind names that the bundled runtime and plugin do not
// implement. That audit is the second half of the evidence a context-free helper
// needs: the registry classifies the name as a procedure, and the audit records
// that no statement can reach it.
func apocProcedureAuditGaps(t *testing.T) map[string]bool {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("..", "docs", "features", "apoc-procedure-capabilities.md"))
	require.NoError(t, err)
	gaps := map[string]bool{}
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Split(line, "|")
		if len(fields) < 4 {
			continue
		}
		name := strings.Trim(strings.TrimSpace(fields[1]), "`")
		if !strings.HasPrefix(name, "apoc.") {
			continue
		}
		if strings.Contains(fields[3], "Not implemented by bundled runtime/plugin") {
			gaps[name] = true
		}
	}
	require.NotEmpty(t, gaps, "the audit must record the procedures the bundled runtime does not implement")
	return gaps
}

// apocBulkReadKind names the whole-graph read a helper made, so the population
// checks prove every read reaches storage and not only the node read.
type apocBulkReadKind string

const (
	apocBulkReadNodes         apocBulkReadKind = "nodes"
	apocBulkReadRelationships apocBulkReadKind = "relationships"
)

// apocBulkReadObservation is one whole-graph read and the lifetime it received.
type apocBulkReadObservation struct {
	kind apocBulkReadKind
	ctx  context.Context
}

// recordingAPOCStorage observes the lifetime each whole-graph read receives
// and, like the real bulk read, stops with the caller's own error.
type recordingAPOCStorage struct {
	mu           sync.Mutex
	observations []apocBulkReadObservation
	afterRead    func()
}

func (r *recordingAPOCStorage) record(kind apocBulkReadKind, ctx context.Context) error {
	r.mu.Lock()
	r.observations = append(r.observations, apocBulkReadObservation{kind: kind, ctx: ctx})
	count := len(r.observations)
	r.mu.Unlock()
	if r.afterRead != nil && count == 1 {
		// The caller stops inside its first whole-graph read, so the read
		// completes and the helper must notice the stopped caller afterwards.
		r.afterRead()
	}
	return ctx.Err()
}

func (r *recordingAPOCStorage) AllNodes(ctx context.Context) ([]*apocstorage.Node, error) {
	if err := r.record(apocBulkReadNodes, ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *recordingAPOCStorage) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observations = nil
	r.afterRead = nil
}

func (r *recordingAPOCStorage) GetNode(context.Context, int64) (*apocstorage.Node, error) {
	return nil, apocstorage.ErrNodeNotFound
}

func (r *recordingAPOCStorage) CreateNode(context.Context, []string, map[string]interface{}) (*apocstorage.Node, error) {
	return &apocstorage.Node{}, nil
}

func (r *recordingAPOCStorage) UpdateNode(context.Context, int64, map[string]interface{}) error {
	return nil
}

func (r *recordingAPOCStorage) DeleteNode(context.Context, int64) error { return nil }

func (r *recordingAPOCStorage) AddLabels(context.Context, int64, []string) error { return nil }

func (r *recordingAPOCStorage) RemoveLabels(context.Context, int64, []string) error { return nil }

func (r *recordingAPOCStorage) UpdateNodeLabels(context.Context, int64, []string) error { return nil }

func (r *recordingAPOCStorage) GetRelationship(context.Context, int64) (*apocstorage.Relationship, error) {
	return nil, apocstorage.ErrRelationshipNotFound
}

func (r *recordingAPOCStorage) CreateRelationship(context.Context, int64, int64, string, map[string]interface{}) (*apocstorage.Relationship, error) {
	return &apocstorage.Relationship{}, nil
}

func (r *recordingAPOCStorage) UpdateRelationship(context.Context, int64, map[string]interface{}) error {
	return nil
}

func (r *recordingAPOCStorage) DeleteRelationship(context.Context, int64) error { return nil }

func (r *recordingAPOCStorage) UpdateRelationshipType(context.Context, int64, string) error {
	return nil
}

func (r *recordingAPOCStorage) AllRelationships(ctx context.Context) ([]*apocstorage.Relationship, error) {
	if err := r.record(apocBulkReadRelationships, ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *recordingAPOCStorage) GetNodeRelationships(context.Context, int64, string, apocstorage.Direction) ([]*apocstorage.Relationship, error) {
	return nil, nil
}

func (r *recordingAPOCStorage) GetNodeNeighbors(context.Context, int64, string, apocstorage.Direction) ([]*apocstorage.Node, error) {
	return nil, nil
}

func (r *recordingAPOCStorage) GetNodeDegree(context.Context, int64, string, apocstorage.Direction) (int, error) {
	return 0, nil
}

func (r *recordingAPOCStorage) FindShortestPath(context.Context, int64, int64, string, int) (*apocstorage.Path, error) {
	return nil, apocstorage.ErrPathNotFound
}

func (r *recordingAPOCStorage) FindAllPaths(context.Context, int64, int64, string, int) ([]*apocstorage.Path, error) {
	return nil, nil
}

func (r *recordingAPOCStorage) BFS(context.Context, int64, string, int, func(*apocstorage.Node) bool) error {
	return nil
}

func (r *recordingAPOCStorage) DFS(context.Context, int64, string, int, func(*apocstorage.Node) bool) error {
	return nil
}

// installRecordingStorage publishes a registry bound to the recorder, so every
// helper the registry admits answers from it, and restores the registry the
// test found afterwards.
func installRecordingStorage(t *testing.T, recorder *recordingAPOCStorage) {
	t.Helper()
	previous := GetStorage()
	require.NotNil(t, previous, "the default registry must be published first")
	require.NoError(t, SetStorage(recorder))
	t.Cleanup(func() {
		require.NoError(t, SetStorage(previous))
	})
}

// apocBulkReadArguments binds one call's arguments: the recorder returns no
// nodes, so the values only have to satisfy the declared parameter types, and
// a label list is non-empty so a per-label search still reaches the read.
func apocBulkReadArguments(t *testing.T, signature reflect.Type) []interface{} {
	t.Helper()
	require.Greater(t, signature.NumIn(), 0)
	arguments := make([]interface{}, 0, signature.NumIn()-1)
	for index := 1; index < signature.NumIn(); index++ {
		if signature.IsVariadic() && index == signature.NumIn()-1 {
			// An omitted variadic argument keeps the helper's own default.
			continue
		}
		parameter := signature.In(index)
		switch parameter.Kind() {
		case reflect.Slice:
			if parameter.Elem().Kind() == reflect.String {
				arguments = append(arguments, []string{"L"})
				continue
			}
			arguments = append(arguments, nil)
		case reflect.Map, reflect.Interface:
			arguments = append(arguments, nil)
		default:
			arguments = append(arguments, reflect.Zero(parameter).Interface())
		}
	}
	return arguments
}

// TestAPOCBulkReadHandlersReceiveStatementContext invokes every statement
// helper through the same checked binding the Cypher adapter uses, with a live
// caller, and requires every whole-graph read it makes to receive that caller's
// own lifetime object rather than a replacement.
func TestAPOCBulkReadHandlersReceiveStatementContext(t *testing.T) {
	require.NoError(t, Initialize(apocstorage.NewInMemoryStorage(), nil))
	recorder := &recordingAPOCStorage{}
	installRecordingStorage(t, recorder)

	helpers := apocStatementHelpers(t, apocPinnedHandlers(apocRegisteredSources(t)))
	require.NotEmpty(t, helpers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relationshipReads := 0
	for _, helper := range helpers {
		recorder.reset()
		_, callErr := helper.handler.CallWithPolicy(
			apocBulkReadArguments(t, helper.handler.Signature()),
			[]reflect.Value{reflect.ValueOf(ctx)},
			registry.CypherArguments,
		)
		require.NoError(t, callErr, helper.name)
		observed := fixtureowner.Snapshot(&recorder.mu, &recorder.observations)
		require.NotEmpty(t, observed, "%s must read the whole graph", helper.name)
		for _, read := range observed {
			require.True(t, read.ctx == ctx,
				"%s did not hand its statement context to its %s read", helper.name, read.kind)
			require.NoError(t, read.ctx.Err(),
				"%s handed its %s read a lifetime that had already stopped", helper.name, read.kind)
			if read.kind == apocBulkReadRelationships {
				relationshipReads++
			}
		}
	}
	require.Positive(t, relationshipReads,
		"the statement population must reach the relationship read, not only the node read")
}

// TestAPOCBulkReadGuardedHelpersStopAfterInFlightCancel stops the caller inside
// its first whole-graph read, with that caller's own CancelFunc, and requires the
// guarded helpers to publish nothing and to make no further read. A stopped
// caller is not asked to read storage here: the probe asserts what the helper
// does with the lifetime it was given, not that it keeps working after it.
func TestAPOCBulkReadGuardedHelpersStopAfterInFlightCancel(t *testing.T) {
	require.NoError(t, Initialize(apocstorage.NewInMemoryStorage(), nil))
	recorder := &recordingAPOCStorage{}
	installRecordingStorage(t, recorder)

	helpers := apocStatementHelpers(t, apocPinnedHandlers(apocRegisteredSources(t)))
	guarded := map[string]bool{}
	for _, function := range apocBulkReadContextCallers["search"] {
		guarded["search."+function] = true
	}
	require.NotEmpty(t, guarded)

	checked := 0
	for _, helper := range helpers {
		if !guarded[helper.directory+"."+helper.function] {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		recorder.reset()
		recorder.afterRead = cancel
		result, callErr := helper.handler.CallWithPolicy(
			apocBulkReadArguments(t, helper.handler.Signature()),
			[]reflect.Value{reflect.ValueOf(ctx)},
			registry.CypherArguments,
		)
		cancel()
		require.ErrorIs(t, callErr, context.Canceled, helper.name)
		require.Nil(t, result, helper.name)
		require.ErrorIs(t, ctx.Err(), context.Canceled, helper.name)
		observed := fixtureowner.Snapshot(&recorder.mu, &recorder.observations)
		require.Len(t, observed, 1,
			"%s stops reading once its caller has stopped", helper.name)
		require.True(t, observed[0].ctx == ctx, helper.name)
		require.Empty(t, apocResultLength(t, result),
			"%s must not publish what it collected before its caller stopped", helper.name)
		checked++
	}
	require.Positive(t, checked, "the guarded population must be exercised")
}

// TestAPOCAdmissionCanceledSearchHelpersReadNothing passes a caller that had
// already stopped when the helper was admitted: the pre-read guard means the
// helper performs no whole-graph read at all and returns its documented empty
// result, for the node and the string helpers alike.
func TestAPOCAdmissionCanceledSearchHelpersReadNothing(t *testing.T) {
	require.NoError(t, Initialize(apocstorage.NewInMemoryStorage(), nil))
	recorder := &recordingAPOCStorage{}
	installRecordingStorage(t, recorder)

	helpers := apocStatementHelpers(t, apocPinnedHandlers(apocRegisteredSources(t)))
	guarded := map[string]bool{}
	for _, function := range apocBulkReadContextCallers["search"] {
		guarded["search."+function] = true
	}

	checked := 0
	for _, helper := range helpers {
		if !guarded[helper.directory+"."+helper.function] {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		recorder.reset()
		result, callErr := helper.handler.CallWithPolicy(
			apocBulkReadArguments(t, helper.handler.Signature()),
			[]reflect.Value{reflect.ValueOf(ctx)},
			registry.CypherArguments,
		)
		require.ErrorIs(t, callErr, context.Canceled, helper.name)
		require.Nil(t, result, helper.name)
		require.Empty(t, fixtureowner.Snapshot(&recorder.mu, &recorder.observations),
			"%s must not read storage for a caller that had already stopped", helper.name)
		require.Empty(t, apocResultLength(t, result), helper.name)
		checked++
	}
	require.Positive(t, checked, "the guarded population must be exercised")
}

// apocResultLength returns the whole slice a helper published, so a test can
// require it to be empty without knowing whether the helper answers with nodes,
// strings or maps.
func apocResultLength(t *testing.T, result interface{}) []interface{} {
	t.Helper()
	value := reflect.ValueOf(result)
	if !value.IsValid() {
		return nil
	}
	if value.Kind() != reflect.Slice {
		t.Fatalf("a whole-graph helper publishes a slice, got %T", result)
	}
	items := make([]interface{}, value.Len())
	for index := 0; index < value.Len(); index++ {
		items[index] = value.Index(index).Interface()
	}
	return items
}

// TestAPOCBulkReadHelpersPropagateStoppedCaller pins the outputs a
// stopped caller cannot produce successful zero values or partial results.
func TestAPOCBulkReadHelpersPropagateStoppedCaller(t *testing.T) {
	require.NoError(t, Initialize(apocstorage.NewInMemoryStorage(), nil))
	recorder := &recordingAPOCStorage{}
	installRecordingStorage(t, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The recorder-bound instances are the ones an admitted caller holds: the
	// documented empty result comes from the helper's own backend.
	bound := newBoundFunctions(recorder)
	labelCallValue1, labelCallError1 := bound.label.Exists(ctx, "Person")
	require.ErrorIs(t, labelCallError1, context.Canceled)
	require.False(t, labelCallValue1)
	labelCallValue2, labelCallError2 := bound.label.Count(ctx, "Person")
	require.ErrorIs(t, labelCallError2, context.Canceled)
	require.Zero(t, labelCallValue2)
	labelCallValue3, labelCallError3 := bound.label.List(ctx)
	require.ErrorIs(t, labelCallError3, context.Canceled)
	require.Empty(t, labelCallValue3)
	apocCallValue1, apocCallError1 := bound.meta.NodeLabels(ctx)
	require.ErrorIs(t, apocCallError1, context.Canceled)
	require.Empty(t, apocCallValue1)
	apocCallValue2, apocCallError2 := bound.meta.PropertyKeys(ctx)
	require.ErrorIs(t, apocCallError2, context.Canceled)
	require.Empty(t, apocCallValue2)
	apocCallValue3, apocCallError3 := bound.schema.Labels(ctx)
	require.ErrorIs(t, apocCallError3, context.Canceled)
	require.Empty(t, apocCallValue3)
	apocCallValue4, apocCallError4 := bound.search.Fuzzy(ctx, "Person", "name", "Alise", 2)
	require.ErrorIs(t, apocCallError4, context.Canceled)
	require.Empty(t, apocCallValue4)
	apocCallValue5, apocCallError5 := bound.search.NodeReduced(ctx, "Person", map[string]interface{}{"name": "Alice"}, 10)
	require.ErrorIs(t, apocCallError5, context.Canceled)
	require.Empty(t, apocCallValue5)
	apocCallValue6, apocCallError6 := bound.search.MultiSearchAny(ctx, []string{"Person", "Company"}, map[string]interface{}{"name": "Alice"})
	require.ErrorIs(t, apocCallError6, context.Canceled)
	require.Empty(t, apocCallValue6)
	apocCallValue7, apocCallError7 := bound.search.MultiSearchAll(ctx, []string{"Person", "Company"}, map[string]interface{}{"name": "Alice"})
	require.ErrorIs(t, apocCallError7, context.Canceled)
	require.Empty(t, apocCallValue7)
	apocCallValue8, apocCallError8 := bound.search.Parallel(ctx, []string{"Person", "Company"}, "name", "Alice")
	require.ErrorIs(t, apocCallError8, context.Canceled)
	require.Empty(t, apocCallValue8)
	apocCallValue9, apocCallError9 := bound.search.Suggest(ctx, "Person", "name", "Al", 5)
	require.ErrorIs(t, apocCallError9, context.Canceled)
	require.Empty(t, apocCallValue9)
	apocCallValue10, apocCallError10 := bound.search.DidYouMean(ctx, "Person", "name", "Alise")
	require.ErrorIs(t, apocCallError10, context.Canceled)
	require.Empty(t, apocCallValue10)
	require.Empty(t, bound.search.Score(ctx, []*apocstorage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
	}, "name", "Alice"),
		"a post-processing helper observes the stopped caller too")
	apocCallValue11, apocCallError11 := bound.refactor.RenameLabel(ctx, "OldLabel", "NewLabel")
	require.ErrorIs(t, apocCallError11, context.Canceled)
	require.Zero(t, apocCallValue11)
	apocCallValue12, apocCallError12 := bound.meta.RelTypes(ctx)
	require.ErrorIs(t, apocCallError12, context.Canceled)
	require.Empty(t, apocCallValue12)
	apocCallValue13, apocCallError13 := bound.schema.Types(ctx)
	require.ErrorIs(t, apocCallError13, context.Canceled)
	require.Empty(t, apocCallValue13)
	apocCallValue14, apocCallError14 := bound.warmup.Relationships(ctx, []string{"KNOWS"})
	require.ErrorIs(t, apocCallError14, context.Canceled)
	require.Nil(t, apocCallValue14)
	apocCallValue15, apocCallError15 := bound.refactor.RenameType(ctx, "OLD", "NEW")
	require.ErrorIs(t, apocCallError15, context.Canceled)
	require.Zero(t, apocCallValue15)
	require.Empty(t, fixtureowner.Snapshot(&recorder.mu, &recorder.observations), "a stopped caller is rejected before a backend read")
}
