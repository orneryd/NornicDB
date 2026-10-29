// Package capabilitycensus interprets storage contracts on #1667's canonical
// source census. Loading, traversal, source identity and manifest validation
// remain workcensus responsibilities; this client owns only domain semantics.
package capabilitycensus

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"go/ast"
	"go/token"
	"go/types"
	"golang.org/x/tools/go/packages"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

const storagePackage = "github.com/orneryd/nornicdb/pkg/storage"

// SiteEvidence is domain review input copied from the canonical selected
// member. It supplies actual typed subjects/targets rather than name-based
// explanations, without creating a second source identity or enumeration.
type SiteEvidence struct {
	OwnerDigest   string   `json:"owner_digest,omitempty"`
	State         string   `json:"state,omitempty"`
	OperandType   string   `json:"operand_type,omitempty"`
	AssertionType string   `json:"assertion_type,omitempty"`
	Signature     string   `json:"signature,omitempty"`
	ReceiverType  string   `json:"receiver_type,omitempty"`
	Callees       []string `json:"callees,omitempty"`
	ArgTypes      []string `json:"arg_types,omitempty"`
	ElementType   string   `json:"element_type,omitempty"`
}

type Site struct {
	Disposition string       `json:"disposition"`
	Evidence    SiteEvidence `json:"evidence"`
	Population  string       `json:"population"`
	Key         string       `json:"key"`
	Path        string       `json:"path"`
	Function    string       `json:"function"`
	Line        int          `json:"line"`
	Contract    string       `json:"contract"`
	Proof       string       `json:"proof"`
	Detail      string       `json:"detail"`
	Digest      string       `json:"digest"`
	Kind        string       `json:"kind"`
	Reason      string       `json:"reason"`
}
type Result struct {
	ErrorlessReadAPIs []ErrorlessReadAPI         `json:"errorless_read_apis"`
	Mechanisms        []Site                     `json:"mechanisms"`
	Sites             []Site                     `json:"sites"`
	Engines           []string                   `json:"engines"`
	Packages          []string                   `json:"packages"`
	Types             map[string]*types.TypeName `json:"-"`
	StoredViews       map[string]*types.TypeName `json:"-"`
	Inventory         workcensus.Inventory       `json:"-"`
}

var lookupPolicy = map[string]string{
	"BindEntityIdentityLookup": "binding", "BindEdgeHeaderReads": "binding", "BindEdgeHeaderRead": "binding", "nativeMemberFor": "membership", "transactionMemberForEngine": "membership", "identityLookup": "view-metadata", "BindReadOperation": "binding", "ExecuteBoundRead": "read", "RebindReadView": "binding", "PublishReadCapabilityBinding": "binding",
	"ReadStored": "stored-mode", "ReadStoredMetadata": "stored-mode", "StoredNodePresent": "stored-mode", "StoredReadPolicyForEngine": "stored-mode",
	"BindReadCapabilityGuard": "binding", "BindQueryNodeRead": "binding", "BindTemporalNodeRead": "binding", "bindExecutionEnforcement": "wrapper-metadata",

	"OwnedCapability": "exact", "engineCapability": "cursor", "readCapabilityForEngine": "read",
	"HasReadCapability": "read", "ReadWithCapability": "read", "ReadValueWithCapability": "read",
	"VisitWithCapability": "read", "VisitWhileWithCapability": "read",
	"ViewMetadataForEngine": "view-metadata", "MetadataCapabilityForEngine": "wrapper-metadata",
	"MembershipForEngine": "membership", "BindMembership": "membership", "StorageMembership": "membership",
	"BindReadView": "binding", "ReadCapabilityBinding": "binding", "ReadCapabilitySource": "binding",
	"ReadCapabilityAvailable": "binding", "ReadCapabilityScoped": "binding", "ReadCapabilityPolicy": "binding",
	"CapabilityOwnerSource": "binding", "EngineCapabilityPolicy": "binding", "UnwrapEngine": "physical",
	"GetInnerEngine": "physical", "NewStorageExecutor": "issuance", "cloneWithStorage": "issuance",
	"cloneForStorage": "issuance", "NewDatabaseScopedExecutor": "issuance", "FunctionStorage": "issuance",
	"graphFunctionStorage": "issuance", "storageMembership": "membership", "boundExecutionContext": "membership",
	"observeResolvedStatementMode": "membership", "executorReadView": "binding", "viewMetadataForExecutor": "view-metadata",
	"queryLimitProvider": "wrapper-metadata", "resolveWALAndDatabase": "wrapper-metadata",
}
var populationRead = regexp.MustCompile(`^(AllNodes|AllEdges|GetAllNodes|GetNode|GetEdge|GetNodes|GetOutgoingEdges|GetIncomingEdges|GetEdges|GetFirstNode|BatchGetNodes|Stream|Iterate|ForEachNode|NodeCount|EdgeCount|VisitNodes|VisitNodeIDs|CollectStored|FirstNodeID|NodeIDsByLabel|CountNodes|FindNodeNeedingEmbedding|Next|Visit|Collect)`)

func callBase(expr ast.Expr) ast.Expr {
	switch x := expr.(type) {
	case *ast.IndexExpr:
		return callBase(x.X)
	case *ast.IndexListExpr:
		return callBase(x.X)
	}
	return expr
}
func CallName(expr ast.Expr) string {
	switch x := callBase(expr).(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	}
	return ""
}
func object(info *types.Info, expr ast.Expr) types.Object {
	if info == nil {
		return nil
	}
	switch x := callBase(expr).(type) {
	case *ast.Ident:
		return info.Uses[x]
	case *ast.SelectorExpr:
		if selection := info.Selections[x]; selection != nil {
			return selection.Obj()
		}
		return info.Uses[x.Sel]
	}
	return nil
}
func engineType(t types.Type, core *types.Interface) bool {
	if t == nil || core == nil {
		return false
	}
	t = types.Unalias(t)
	if types.Implements(t, core) {
		return true
	}
	if _, pointer := t.(*types.Pointer); pointer {
		return false
	}
	return types.Implements(types.NewPointer(t), core)
}
func namedType(t types.Type) *types.Named {
	if t == nil {
		return nil
	}
	t = types.Unalias(t)
	if pointer, ok := t.(*types.Pointer); ok {
		t = types.Unalias(pointer.Elem())
	}
	named, _ := t.(*types.Named)
	return named
}
func storageType(t types.Type) bool {
	named := namedType(t)
	return named != nil && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == storagePackage
}

// A method name is not a storage contract: parser listeners and other
// services have Next/Visit/Get methods too. Compare complete signatures with
// the independently discovered Engine method surface, including promoted and
// optional methods. Unrelated value/service assertions remain source cross-checks.
func capabilityType(typ types.Type, core *types.Interface, methods map[string][]types.Type) bool {
	if typ == nil {
		return false
	}
	if engineType(typ, core) {
		return true
	}
	set := types.NewMethodSet(typ)
	for i := 0; i < set.Len(); i++ {
		method := set.At(i).Obj()
		for _, contract := range methods[method.Name()] {
			if types.Identical(method.Type(), contract) {
				return true
			}
		}
	}
	return false
}

func capabilityAssertion(info *types.Info, x *ast.TypeAssertExpr, core *types.Interface, methods map[string][]types.Type) bool {
	if info == nil {
		return false
	}
	if engineType(info.TypeOf(x.X), core) {
		return true
	}
	return x.Type != nil && capabilityType(info.TypeOf(x.Type), core, methods)
}

func proof(contract string) string {
	switch contract {
	case "typed-node-batch", "typed-edge-batch":
		return "batch-point-equivalence"
	case "typed-node-id-batch-error", "typed-edge-id-batch-error", "typed-entity-id-batch-error":
		return "entity-id-batch-operation"
	case "stored-mode", "unindexed-request-membership":
		return "stored-fact-operation"
	case "read", "read-call", "read-owner", "scoped-loop", "operation-fallback", "refusal-contract":
		return "read-operation"
	case "native-loop", "intentional-population":
		return "native-and-full-read"
	case "membership", "issuance":
		return "membership-binding"
	case "commit", "commit-call", "commit-owner", "commit-capability-call", "commit-reference-producer", "commit-reference-call", "commit-consumer":
		return "commit-owner"
	case "observed", "observed-owner", "core-mutation":
		return "observed-mutation"
	case "view-metadata", "wrapper-metadata":
		return "view-and-enforcement"
	case "binding", "cursor", "physical", "engine-operation", "independent-physical-walk", "transaction-operation-undecided", "transaction-reference-undecided", "transaction-consumer-undecided":
		return "structural-policy"
	case "exact", "exact-effect":
		return "exact-owner"
	case "capability-assertion":
		return "capability-boundary"
	default:
		return "excluded-value-or-service"
	}
}

// Config keeps the complete source cross-check. Only compiled typed domain
// members become the review population; no manifest controls discovery.
func Config(root string) workcensus.Config {
	c := workcensus.Config{SourceOnly: true, Dir: root, Patterns: []string{"./..."}, IncludeSyntax: true, IncludeInactive: true, SourceRoots: []workcensus.SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}, ExcludeDirs: []string{".git", "node_modules", "vendor"}, ExcludeFiles: []string{"*_test.go"}}}}
	for _, node := range []string{"declaration", "file", "call", "identifier", "for", "range", "go", "type-assert", "type-switch", "assign", "composite"} {
		c.Selectors = append(c.Selectors, workcensus.Selector{Kind: node, Node: node, Reachability: "all"})
	}
	return c
}

var discovery sync.Map

// Discover shares the immutable engine result among domain gates in one process.
func Discover(root string) (Result, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Result{}, err
	}
	if result, ok := discovery.Load(absolute); ok {
		return result.(Result), nil
	}
	inventory, err := workcensus.Extract(Config(absolute))
	if err != nil {
		return Result{}, err
	}
	result, err := Interpret(inventory)
	if err == nil {
		discovery.Store(absolute, result)
	}
	return result, err
}
func owner(m workcensus.Member) string {
	// Closures retain the operation owner's semantic disposition. The canonical
	// identity still distinguishes every closure and lexical member.
	return strings.Split(m.Owner, "$")[0]
}
func memberPath(m workcensus.Member) string { p, _, _ := strings.Cut(m.ID, ":"); return p }
func Interpret(inventory workcensus.Inventory) (Result, error) {
	result := Result{Inventory: inventory, Types: map[string]*types.TypeName{}, StoredViews: map[string]*types.TypeName{}}
	var core *types.Interface
	packages := map[string]bool{}
	for _, source := range inventory.Sources {
		if source.Package != "" && strings.HasPrefix(source.State, "compiled") {
			packages[source.Package] = true
		}
	}
	for _, m := range inventory.Members {
		if m.Syntax == nil || m.Syntax.Info == nil {
			continue
		}
		spec, ok := m.Syntax.Node.(*ast.TypeSpec)
		if !ok {
			continue
		}
		obj, _ := m.Syntax.Info.Defs[spec.Name].(*types.TypeName)
		if obj == nil || obj.Pkg() == nil {
			continue
		}
		packages[obj.Pkg().Path()] = true
		if obj.Pkg().Path() == storagePackage && obj.Name() == "Engine" {
			core, _ = obj.Type().Underlying().(*types.Interface)
		}
	}
	if core == nil && len(inventory.Sources) > 0 {
		for _, m := range inventory.Members {
			if m.Syntax != nil && m.Syntax.Info != nil {
				return result, fmt.Errorf("canonical typed population lacks storage Engine")
			}
		}
	}
	for _, m := range inventory.Members {
		if m.Syntax == nil {
			continue
		}
		if spec, ok := m.Syntax.Node.(*ast.TypeSpec); ok && m.Syntax.Info != nil {
			obj, _ := m.Syntax.Info.Defs[spec.Name].(*types.TypeName)
			if obj != nil && obj.Pkg() != nil {
				if _, iface := obj.Type().Underlying().(*types.Interface); !iface && engineType(obj.Type(), core) {
					key := obj.Pkg().Path() + "." + obj.Name()
					if obj.Parent() != obj.Pkg().Scope() {
						key = m.ID
					}
					result.Types[key] = obj
				}
			}
		}
	}
	capabilityMethods := map[string][]types.Type{}
	addMethod := func(method *types.Func) {
		// Error/String/Close/Context alone do not describe storage authority.
		// Keep storage-valued signatures, actual read/count operations, and the
		// explicit structural/metadata/lifecycle protocol. A connection, duration
		// or response context must not become a storage provider by coincidence.
		_, policy := lookupPolicy[method.Name()]
		semantic := policy || populationRead.MatchString(method.Name()) || strings.Contains(types.TypeString(method.Type(), nil), storagePackage+".")
		switch method.Name() {
		case "Namespace", "CompositeName", "IsComposite", "IsLeader", "LifecycleStatus", "CheckQueryLimits", "GetQueryLimitChecker":
			semantic = true
		}
		if !semantic {
			return
		}
		for _, previous := range capabilityMethods[method.Name()] {
			if types.Identical(previous, method.Type()) {
				return
			}
		}
		capabilityMethods[method.Name()] = append(capabilityMethods[method.Name()], method.Type())
	}
	if core != nil {
		for i := 0; i < core.NumMethods(); i++ {
			addMethod(core.Method(i))
		}
	}
	// A declared storage capability can be consumed before any Engine has a
	// concrete implementation (and narrower retained views are not Engines).
	// Include its actual interface contract, not every value in that package.
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		spec, ok := member.Syntax.Node.(*ast.TypeSpec)
		if !ok {
			continue
		}
		object := member.Syntax.Info.Defs[spec.Name]
		if object == nil || object.Pkg() == nil || object.Pkg().Path() != storagePackage {
			continue
		}
		if contract, ok := object.Type().Underlying().(*types.Interface); ok {
			for i := 0; i < contract.NumMethods(); i++ {
				addMethod(contract.Method(i))
			}
		}
	}
	for _, object := range result.Types {
		typ := types.Unalias(object.Type())
		if _, pointer := typ.(*types.Pointer); !pointer {
			typ = types.NewPointer(typ)
		}
		set := types.NewMethodSet(typ)
		for i := 0; i < set.Len(); i++ {
			if method, ok := set.At(i).Obj().(*types.Func); ok {
				addMethod(method)
			}
		}
	}
	// The classified population is compiled domain code. Source documents and
	// text candidates remain in Inventory for cross-checks, never as approvals.
	if err := compiledPopulation(&result, core, capabilityMethods); err != nil {
		return result, err
	}
	result.Mechanisms = DirectMechanisms(result)
	result.ErrorlessReadAPIs = ErrorlessReadAPIs(result)
	for key := range result.Types {
		result.Engines = append(result.Engines, key)
	}
	for key := range packages {
		result.Packages = append(result.Packages, key)
	}
	sort.Strings(result.Engines)
	sort.Strings(result.Packages)
	sort.Slice(result.Sites, func(i, j int) bool {
		a, b := result.Sites[i], result.Sites[j]
		if a.Population != b.Population {
			return a.Population < b.Population
		}
		return a.Key < b.Key
	})
	return result, nil
}

// CoverageProblems delegates identity, source-change, stale/duplicate and
// classification consistency to the same engine as every other B0 client.
func CoverageProblems(actual, registered []Site) []string {
	convert := func(sites []Site) workcensus.Inventory {
		inv := workcensus.Inventory{}
		for _, s := range sites {
			ownerDigest := s.Evidence.OwnerDigest
			if s.Kind == "file" {
				ownerDigest = s.Digest
			} // the canonical selected file is its own review boundary
			inv.Members = append(inv.Members, workcensus.Member{ID: s.Key, Digest: s.Digest, OwnerDigest: ownerDigest, Kind: s.Kind, Class: s.Disposition, Reason: s.Reason, Facts: map[string][]string{"contract": {s.Contract}, "proof": {s.Proof}}})
		}
		return inv
	}
	// Discovery proposes domain contracts; only an individual read-based review
	// supplies a semantic disposition/reason. A candidate never approves itself.
	classes := []string{"consumes owner", "distinct responsibility", "fixed here"}
	failures := workcensus.Validate(convert(actual), convert(registered), workcensus.Policy{Classes: classes, ClassKinds: map[string]string{"consumes owner": "review", "distinct responsibility": "review", "fixed here": "review"}, FactKeys: []string{"contract", "proof"}, RequireOwnerDigest: true})
	byKey := map[string]Site{}
	for _, site := range actual {
		byKey[site.Key] = site
	}
	for _, review := range registered {
		if site, ok := byKey[review.Key]; ok && strings.TrimSpace(review.Reason) == site.Function+": "+site.Contract {
			failures = append(failures, "CANDIDATE LABEL IS NOT A SEMANTIC REASON "+review.Key)
		}
	}
	sort.Strings(failures)
	return failures
}

// Physical wrapper progression is a domain policy over canonical call/lexical
// facts. A new private chain loop fails independently of any semantic approval.
// Per-child range iteration with an explicit namespace hop is a different
// operation; it is still enumerated and individually reviewed in the ledger.
func independentPhysicalProgression(member workcensus.Member, core *types.Interface) bool {
	if member.Syntax == nil || owner(member) == storagePackage+".engineCapability" && memberPath(member) == "pkg/storage/read_capability.go" {
		return false
	}
	unwrap := false
	switch node := member.Syntax.Node.(type) {
	case *ast.CallExpr:
		unwrap = CallName(node.Fun) == "GetInnerEngine"
		for _, callee := range member.Callees {
			unwrap = unwrap || strings.HasSuffix(callee, ".GetInnerEngine")
		}
		if member.Syntax.Info != nil && !engineType(member.Syntax.Info.TypeOf(node), core) {
			return false
		}
	case *ast.Ident:
		if node.Name != "GetInnerEngine" {
			return false
		}
		unwrap = true
		if member.Syntax.Info != nil {
			function, ok := member.Syntax.Info.ObjectOf(node).(*types.Func)
			if !ok {
				return false
			}
			signature := function.Type().(*types.Signature)
			if signature.Results().Len() != 1 || !engineType(signature.Results().At(0).Type(), core) {
				return false
			}
		}
	}
	if !unwrap {
		return false
	}
	for _, ancestor := range member.Syntax.Ancestors {
		if _, ok := ancestor.(*ast.ForStmt); ok {
			return true
		}
	}
	return false
}

// Problems enforces causal policy independently of the frozen coverage ledger.
// Updating the inventory cannot bless a second commit cursor, scalar goroutine,
// whole-container materialization or mutation
// unwrap/replay boundary.
func Problems(sites []Site) []string {
	var problems []string
	for _, site := range sites {
		if site.Contract == "independent-physical-walk" {
			problems = append(problems, site.Key+": independent physical wrapper progression bypasses engineCapability")
		}
		if site.Contract == "unindexed-request-membership" {
			problems = append(problems, site.Key+": stored request membership scan inside an entity loop; index the request once")
		}
		detail := site.Detail
		function := strings.ToLower(site.Function)
		if site.Population == "lookups" && strings.Contains(detail, "GetInnerEngine") && (strings.Contains(function, "commitstoragetransaction") || strings.Contains(function, "resolveimplicittxengines") || strings.Contains(function, "withstoretransaction")) {
			problems = append(problems, site.Key+": independent transaction/commit discovery walk")
		}
		if site.Kind == "go" && strings.Contains(function, "compositeengine") && strings.Contains(function, "count") {
			problems = append(problems, site.Key+": scalar composite count starts a goroutine")
		}
		if site.Population == "fallbacks_streams" && strings.Contains(function, "compositeengine") && (strings.Contains(function, "stream") || strings.Contains(function, "iteratenodes") || strings.Contains(function, "count")) && (strings.Contains(detail, "c.AllNodes(") || strings.Contains(detail, "c.AllEdges(")) {
			problems = append(problems, site.Key+": whole-container ordinary fallback")
		}
		if site.Population == "observed_mutations" && (strings.Contains(detail, "GetInnerEngine(") || strings.Contains(detail, "UnwrapEngine(")) {
			problems = append(problems, site.Key+": observed mutation unwraps its original operation owner")
		}
		if site.Population == "observed_mutations" && strings.Contains(site.Key, "/type-assert#") {
			problems = append(problems, site.Key+": observed dispatch bypasses exact mutation operation")
		}
	}
	sort.Strings(problems)
	return problems
}

// Packages is a compatibility view for method-set and execution-dominance domain
// analyses. Every file and type here was already loaded by the shared engine.
func Packages(root string) ([]*packages.Package, *token.FileSet, error) {
	result, err := Discover(root)
	if err != nil {
		return nil, nil, err
	}
	byPath := map[string]*packages.Package{}
	var fset *token.FileSet
	for _, m := range result.Inventory.Members {
		if m.Syntax == nil || m.Syntax.Info == nil {
			continue
		}
		file, ok := m.Syntax.Node.(*ast.File)
		if !ok {
			continue
		}
		path, _, _ := strings.Cut(m.Owner, ".file:")
		pkg := byPath[path]
		if pkg == nil {
			if m.Syntax.Package == nil {
				return nil, nil, fmt.Errorf("canonical typed source lacks loaded package scope: %s", m.ID)
			}
			pkg = &packages.Package{PkgPath: path, Types: m.Syntax.Package, TypesInfo: m.Syntax.Info, Fset: m.Syntax.FileSet}
			byPath[path] = pkg
		}
		pkg.Syntax = append(pkg.Syntax, file)
		fset = m.Syntax.FileSet
	}
	var out []*packages.Package
	for _, pkg := range byPath {
		out = append(out, pkg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PkgPath < out[j].PkgPath })
	return out, fset, nil
}

func SourceProblems(actual, approved []workcensus.SourceBoundary) []string {
	return workcensus.Validate(workcensus.Inventory{Sources: actual}, workcensus.Inventory{Sources: approved}, workcensus.Policy{RequireSources: true})
}

// Only provider/result adapters and operations accepting an Engine or opaque
// owner participate in lookup alias propagation. Query callers do not become
// new structural lookup owners merely because they invoke such an operation.
func lookupAdapterCandidate(obj types.Object, core *types.Interface) bool {
	signature, ok := obj.Type().(*types.Signature)
	if !ok {
		return false
	}
	for i := 0; i < signature.Results().Len(); i++ {
		typ := signature.Results().At(i).Type()
		if engineType(typ, core) {
			return true
		}
		if _, ok := typ.(*types.TypeParam); ok {
			return true
		}
		if inter, ok := typ.Underlying().(*types.Interface); ok && inter.NumMethods() > 0 {
			// Returning error (or another service interface) does not turn a
			// query caller into a capability provider. Propagate only an actual
			// storage interface or an anonymous structural/read contract.
			if storageType(typ) {
				return true
			}
			for j := 0; j < inter.NumMethods(); j++ {
				name := inter.Method(j).Name()
				if populationRead.MatchString(name) {
					return true
				}
				switch name {
				case "ReadCapabilitySource", "ReadCapabilityPolicy", "EngineCapabilityPolicy", "StorageMembership", "CommitTransaction", "GetInnerEngine", "Namespace", "ConstituentStorage", "ReadStored", "CheckQueryLimits":
					return true
				}
			}
		}
		if named := namedType(typ); named != nil && storageType(typ) {
			switch named.Obj().Name() {
			case "MembershipBinding", "MembershipHandle":
				return true
			}
		}
	}
	for i := 0; i < signature.Params().Len(); i++ {
		typ := signature.Params().At(i).Type()
		if engineType(typ, core) {
			return true
		}
		if inter, ok := typ.Underlying().(*types.Interface); ok && inter.NumMethods() == 0 {
			return true
		}
	}
	return false
}

func storedFactType(typ types.Type) bool {
	named := namedType(typ)
	if named == nil || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != storagePackage {
		return false
	}
	switch named.Obj().Name() {
	case "StoredReadMode", "StoredReadIdentity", "StoredReference", "StoredNodeFact", "StoredEdgeFact", "StoredAdjacencyRequest", "StoredReadWork", "StoredReadRequest", "StoredMetadata", "StoredNodeProjection", "StoredEdgeProjection", "StoredProjection", "StoredFullRead", "StoredReadResult", "StoredReadPolicy", "StoredReadDeclaration", "StoredReadProvider":
		return true
	}
	return false
}

// This domain property uses the canonical call, type and lexical ancestry.
// A linear request-array lookup in an entity loop makes a batch quadratic;
// a ledger update cannot authorize it, whatever the function or file name.
func unindexedRequestMembership(member workcensus.Member) bool {
	if member.Syntax == nil || member.Syntax.Info == nil {
		return false
	}
	call, ok := member.Syntax.Node.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return false
	}
	function := object(member.Syntax.Info, call.Fun)
	if function == nil || function.Pkg() == nil || function.Pkg().Path() != "slices" || function.Name() != "Contains" {
		return false
	}
	field, ok := call.Args[0].(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "Nodes" && field.Sel.Name != "Edges" {
		return false
	}
	request := namedType(member.Syntax.Info.TypeOf(field.X))
	if request == nil || request.Obj().Pkg() == nil || request.Obj().Pkg().Path() != storagePackage || request.Obj().Name() != "StoredReadRequest" {
		return false
	}
	for _, ancestor := range member.Syntax.Ancestors {
		switch ancestor.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			return true
		}
	}
	return false
}
