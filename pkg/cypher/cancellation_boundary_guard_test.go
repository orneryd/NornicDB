package cypher

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The cancellation boundary is an allowlist. In the production files of
// pkg/cypher and pkg/cypher/fn, a pass whose size comes from the query's
// data is legal only when it runs through the cancellation owner, or when a
// reviewed marker states its bound:
//
//	// bounded: <reason>
//
// on the line before the pass. A marker states a bound the reviewer can
// check, so it never covers a pass over data: it covers only a sort or copy
// of values that are not the query's data (a schema's keys, a registry).
//
// Data-sized is decided from type information, not from how a pass is
// spelled:
//   - a loop ranging over, or counting to the length of, a population
//     (rows, entities, IDs, paths, results), a Cypher list or map value, or a
//     data string;
//   - a call that does a whole pass in one standard-library step: a sort, a
//     copy or append of all of a population or value, strings.Split /
//     SplitN / Replace / ReplaceAll / Join / Repeat / Fields over a data
//     string, and JSON or CSV encoding of a value outside an observed pass.
//
// A data string is a string (or a list of strings) derived from a Cypher
// value: a type assertion or conversion of an interface value, anything
// computed from one, and every string parameter some caller in the package
// passes one to (a fixed point over the package's calls). Statement text is
// not data: its passes are bounded by what the client wrote.
//
// A pass over a data string is data-sized whatever its body does: its speed
// per byte bounds nothing, as a parse of statement text a procedure builds
// from data shows.
//
// A pass runs through the owner when its body observes the statement (a
// checkpoint's due/visit/canceled, a scan or walk observation, ctx.Err, a
// bulk primitive) or when it ranges over an owner batch.

// boundaryFinding is one data-sized pass outside the owner.
type boundaryFinding struct {
	File     string `json:"file"`
	Function string `json:"function"`
	Key      string `json:"key"`
	Line     int    `json:"line"`
	Kind     string `json:"kind"`   // loop, call, go, lock or escape
	Detail   string `json:"detail"` // the pass
	Scope    string `json:"scope"`  // what the function can observe: ctx, fnctx, walk, statement, field, row, none
	// Observe is the Go expression of the statement's context in the
	// function (or, for a walk, the walk itself), for remediation tooling.
	Observe string `json:"observe,omitempty"`
}

func (f boundaryFinding) String() string {
	return fmt.Sprintf("%s:%d %s: %s %s", f.File, f.Line, f.Function, f.Kind, f.Detail)
}

// boundaryStringPasses are the strings functions that make one pass over
// their whole input (or build a data-sized output) in a single call.
var boundaryStringPasses = map[string]bool{
	"Split": true, "SplitN": true, "SplitAfter": true, "SplitAfterN": true,
	"Replace": true, "ReplaceAll": true, "Join": true, "Repeat": true,
	"Fields": true, "FieldsFunc": true,
	// Searches, comparisons and transforms of a whole string.
	"Index": true, "IndexAny": true, "IndexByte": true, "IndexRune": true, "IndexFunc": true,
	"LastIndex": true, "LastIndexAny": true, "LastIndexByte": true, "LastIndexFunc": true,
	"Contains": true, "ContainsAny": true, "ContainsRune": true, "ContainsFunc": true, "Count": true,
	"EqualFold": true, "ToUpper": true, "ToLower": true, "ToTitle": true, "ToValidUTF8": true, "Title": true,
	"Trim": true, "TrimFunc": true, "TrimLeft": true, "TrimLeftFunc": true, "TrimRight": true, "TrimRightFunc": true,
	"TrimSpace": true, "Map": true, "Compare": true, "Cut": true,
}

// boundarySortsAndCopies are the standard-library sorts and copies.
var boundarySortsAndCopies = map[string]bool{
	"sort.Slice": true, "sort.SliceStable": true, "sort.Sort": true, "sort.Stable": true,
	"sort.Strings": true, "sort.Ints": true, "sort.Float64s": true,
	"slices.Sort": true, "slices.SortFunc": true, "slices.SortStableFunc": true,
	"slices.Clone": true, "slices.Concat": true, "maps.Clone": true, "maps.Copy": true,
}

// boundaryEncoders encode a whole value in one call.
var boundaryEncoders = map[string]bool{
	"encoding/json.Marshal": true, "encoding/json.MarshalIndent": true,
	"(*encoding/json.Encoder).Encode": true, "(*encoding/csv.Writer).WriteAll": true,
}

type boundaryAnalysis struct {
	pkg     *guardPackage
	tainted map[types.Object]bool
	decls   map[*types.Func]*ast.FuncDecl
	// sources records, for an inventory, where each parameter was tainted.
	sources map[types.Object]string
	// stopVia is, for each function that can stop, the call it stops
	// through, and stopNext the package function it calls there.
	stopVia  map[*types.Func]string
	stopNext map[*types.Func]*types.Func
	// paramCalls records which function parameters a function calls.
	paramCalls map[boundaryParam]bool
	// lengths are the integer variables assigned a data length.
	lengths map[types.Object]bool
	// passParams are, per function, the parameters it makes an unobserved
	// pass over (a loop, a whole-text call, or a hand-off to a function that
	// does): a call handing data to one is the pass, charged to the caller.
	passParams map[*types.Func]map[int]bool
	// passLoops are the lines of those passes' loops, for remediation.
	passLoops map[*types.Func]map[int]bool
	// passHandoffs are, per function, the calls that hand a passed-over
	// parameter on to a function that passes over it (line -> callee).
	passHandoffs map[*types.Func]map[int]*types.Func
	// markersUsed are the bounded: markers some pass consulted.
	markersUsed map[string]bool
	// ledgerLimitations are the stated limitations the package reaches.
	ledgerLimitations map[string]string
	// provenance tells which values may hold the query's data (nil: all).
	provenance *valueProvenance
	// statementText is set while a statement entry is analyzed: the text it
	// reads and rewrites is the statement's (held to the request and dynamic
	// statement limits), not a pass over data.
	statementText bool
}

// boundaryParam is a function's parameter, by position.
type boundaryParam struct {
	decl  *ast.FuncDecl
	index int
}

// cancellationBoundaryFindings reports every data-sized pass in files that
// is neither observed nor marked bounded.
func cancellationBoundaryFindings(pkg *guardPackage, files []*ast.File) []boundaryFinding {
	a := &boundaryAnalysis{pkg: pkg, tainted: map[types.Object]bool{}, decls: map[*types.Func]*ast.FuncDecl{}}
	if os.Getenv("NORNICDB_BOUNDARY_INVENTORY") != "" {
		a.sources = map[types.Object]string{}
		defer func() {
			for _, source := range a.sources {
				fmt.Println("TAINTSRC " + source)
			}
		}()
	}
	all := append(append([]*ast.File{}, pkg.files...), pkg.fixtures...)
	for _, file := range all {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				if obj, ok := pkg.info.Defs[fn.Name].(*types.Func); ok {
					a.decls[obj] = fn
				}
			}
		}
	}
	a.solveProvenance(all)
	a.noteLengths(all)
	a.summarizePasses()
	a.markersUsed = map[string]bool{}
	var findings []boundaryFinding
	for _, file := range files {
		name := filepath.Base(pkg.fset.Position(file.Pos()).Filename)
		if name == "population_bulk.go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			findings = append(findings, a.function(file, name, fn)...)
			findings = append(findings, a.goroutines(name, fn)...)
			findings = append(findings, a.recoveryFindings(name, fn)...)
			findings = append(findings, a.workerCheckpointFindings(name, fn)...)
			findings = append(findings, a.duplicateOwnerFindings(name, fn)...)
		}
		for line, reason := range a.markedLines(file) {
			if !a.markersUsed[name+":"+strconv.Itoa(line)] {
				findings = append(findings, boundaryFinding{File: name, Line: line, Kind: "marker", Detail: "a bounded: marker on no pass (stale): " + reason})
			}
		}
	}
	findings = append(findings, a.recursionFindings(files)...)
	findings = append(findings, a.stopFindings(files)...)
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].File != findings[j].File {
			return findings[i].File < findings[j].File
		}
		return findings[i].Line < findings[j].Line
	})
	return findings
}

// markedBounded reports a comment group with a "bounded: <reason>" line.
func markedBounded(group *ast.CommentGroup) bool {
	if group == nil {
		return false
	}
	for _, line := range strings.Split(group.Text(), "\n") {
		if reason, ok := strings.CutPrefix(strings.TrimSpace(line), "bounded:"); ok && strings.TrimSpace(reason) != "" {
			return true
		}
	}
	return false
}

// stringLike reports a string, a list of strings, or bytes.
func stringLike(typ types.Type) bool {
	if typ == nil {
		return false
	}
	switch u := typ.Underlying().(type) {
	case *types.Basic:
		return u.Info()&types.IsString != 0
	case *types.Slice:
		if b, ok := u.Elem().Underlying().(*types.Basic); ok {
			return b.Info()&types.IsString != 0 || b.Kind() == types.Byte
		}
	}
	return false
}

// cypherValueType reports the type of a Cypher value: the empty interface.
// (A context, an error or an engine is an interface too, but no data.)
func cypherValueType(typ types.Type) bool {
	if typ == nil {
		return false
	}
	iface, ok := typ.Underlying().(*types.Interface)
	return ok && iface.NumMethods() == 0
}

// dataExpr reports an expression whose value is derived from Cypher data.
func (a *boundaryAnalysis) dataExpr(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if found {
			return false
		}
		switch x := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.TypeAssertExpr:
			if x.Type != nil {
				// A comma-ok assertion records a tuple type: read the asserted type.
				if cypherValueType(a.pkg.info.TypeOf(x.X)) && stringLike(a.pkg.info.TypeOf(x.Type)) && a.holds(x.X) {
					found = true
				}
			}
		case *ast.Ident:
			if obj := a.pkg.info.Uses[x]; obj != nil && a.tainted[obj] {
				found = true
			}
		case *ast.CallExpr:
			// A package function's string result some data reaches
			// (provenance follows its returns).
			if f := calledFunc(a.pkg.info, x); f != nil && a.provenance != nil && a.decls[f.Origin()] != nil &&
				a.provenance.results[f.Origin()] && stringResult(f) {
				found = true
			}
			// A call that makes a string from a Cypher value or from data.
			if !found && stringLike(a.pkg.info.TypeOf(x)) {
				if tv, ok := a.pkg.info.Types[x.Fun]; !ok || !tv.IsType() {
					for _, arg := range x.Args {
						if cypherValueType(a.pkg.info.TypeOf(arg)) && a.holds(arg) {
							found = true
						}
					}
				}
			}
			// A conversion of an interface value to a string type.
			if tv, ok := a.pkg.info.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 && stringLike(tv.Type) {
				if cypherValueType(a.pkg.info.TypeOf(x.Args[0])) && a.holds(x.Args[0]) {
					found = true
				}
			}
			// fmt's text of an interface value.
			if f := calledFunc(a.pkg.info, x); f != nil && f.Pkg() != nil && f.Pkg().Path() == "fmt" && strings.HasPrefix(f.Name(), "Sprint") {
				for _, arg := range x.Args {
					if cypherValueType(a.pkg.info.TypeOf(arg)) && a.holds(arg) {
						found = true
					}
				}
			}
		}
		return !found
	})
	return found
}

// propagate taints, to a fixed point, the string-like variables assigned a
// data expression and the string-like parameters some call passes one to.
func (a *boundaryAnalysis) propagate(files []*ast.File) {
	for changed := true; changed; {
		changed = false
		mark := func(obj types.Object) {
			if obj != nil && !a.tainted[obj] && stringLike(obj.Type()) {
				a.tainted[obj] = true
				changed = true
			}
		}
		for _, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for i, lhs := range x.Lhs {
						rhs := x.Rhs[0]
						if len(x.Rhs) == len(x.Lhs) {
							rhs = x.Rhs[i]
						}
						if id, ok := lhs.(*ast.Ident); ok && a.dataExpr(rhs) {
							obj := a.pkg.info.Defs[id]
							if obj == nil {
								obj = a.pkg.info.Uses[id]
							}
							mark(obj)
						}
					}
				case *ast.ValueSpec:
					for i, id := range x.Names {
						if i < len(x.Values) && a.dataExpr(x.Values[i]) {
							mark(a.pkg.info.Defs[id])
						}
					}
				case *ast.RangeStmt:
					if a.dataExpr(x.X) {
						if id, ok := x.Value.(*ast.Ident); ok {
							mark(a.pkg.info.Defs[id])
						}
					}
				case *ast.CallExpr:
					f := calledFunc(a.pkg.info, x)
					if f == nil {
						return true
					}
					decl := a.decls[f]
					if decl == nil || boundaryStatementEntries[funcKey(a.pkg, f)] {
						// Statement text is bounded where it enters: the
						// request limits, or the limit on text a procedure runs.
						return true
					}
					var params []*ast.Ident
					for _, field := range decl.Type.Params.List {
						if len(field.Names) == 0 {
							params = append(params, nil)
						}
						params = append(params, field.Names...)
					}
					_ = params
				}
				return true
			})
		}
	}
}

// dataSized reports a value whose passes are data-sized.
func (a *boundaryAnalysis) dataSized(expr ast.Expr) bool {
	if expr == nil {
		return false
	}
	if isQuerySizedValue(a.pkg.info.TypeOf(expr)) {
		return a.holds(expr)
	}
	return stringLike(a.pkg.info.TypeOf(expr)) && a.dataExpr(expr)
}

// receiverHasContext reports a struct (or pointer to one) with a ctx field.
func receiverHasContext(typ types.Type) bool {
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	st, ok := typ.Underlying().(*types.Struct)
	if !ok {
		return false
	}
	for i := 0; i < st.NumFields(); i++ {
		if f := st.Field(i); f.Name() == "ctx" && f.Type().String() == "context.Context" {
			return true
		}
	}
	return false
}

// observeExpr is the expression that reaches fn's statement (see scope).
func (a *boundaryAnalysis) observeExpr(fn *ast.FuncDecl) string {
	recv := ""
	if fn.Recv != nil && len(fn.Recv.List) > 0 && len(fn.Recv.List[0].Names) > 0 {
		recv = fn.Recv.List[0].Names[0].Name
	}
	var walk, ctx, fnctx, row string
	for _, field := range fn.Type.Params.List {
		typ := a.pkg.info.TypeOf(field.Type)
		if typ == nil || len(field.Names) == 0 {
			continue
		}
		name := field.Names[0].Name
		switch t := typ.String(); {
		case t == "context.Context" && ctx == "":
			ctx = name
		case strings.HasSuffix(t, "pkg/cypher/fn.Context") && fnctx == "":
			fnctx = name
		case isCheckpointType(typ) && walk == "":
			walk = name
		}
		if named, ok := typ.(*types.Named); ok && named.Obj().Name() == "pipelineRow" && row == "" {
			row = name
		}
		if m, ok := typ.Underlying().(*types.Map); ok && cypherValueType(m.Elem()) && row == "" {
			switch name {
			case "values", "row", "scope", "bindings", "vals":
				row = name
			}
		}
	}
	switch a.scope(fn) {
	case "ctx":
		return ctx
	case "fnctx":
		return fnctx + ".StatementContext()"
	case "walk":
		return "walk:" + walk
	case "statement":
		return recv + ".statement()"
	case "field":
		return recv + ".ctx"
	case "row":
		return "rowStatementContextOrBackground(" + row + ")"
	}
	return ""
}

// scope is what fn can observe the statement through.
func (a *boundaryAnalysis) scope(fn *ast.FuncDecl) string {
	scope := "none"
	check := func(name string, typ types.Type) {
		switch t := typ.String(); {
		case t == "context.Context":
			scope = "ctx"
		case strings.HasSuffix(t, "pkg/cypher/fn.Context") && scope == "none":
			scope = "fnctx"
		case isCheckpointType(typ) && scope == "none":
			scope = "walk"
		}
	}
	if fn.Recv != nil {
		for _, field := range fn.Recv.List {
			typ := a.pkg.info.TypeOf(field.Type)
			if typ != nil && strings.Contains(typ.String(), "TraversalContext") {
				scope = "statement"
			} else if typ != nil && receiverHasContext(typ) {
				scope = "field"
			}
		}
	}
	for _, field := range fn.Type.Params.List {
		typ := a.pkg.info.TypeOf(field.Type)
		for _, id := range field.Names {
			check(id.Name, typ)
		}
	}
	if scope == "none" {
		// A row's values carry the statement (rowStatementContextKey).
		for _, field := range fn.Type.Params.List {
			typ := a.pkg.info.TypeOf(field.Type)
			if typ == nil {
				continue
			}
			if named, ok := typ.(*types.Named); ok && named.Obj().Name() == "pipelineRow" {
				scope = "row"
			}
			if m, ok := typ.Underlying().(*types.Map); ok && cypherValueType(m.Elem()) && len(field.Names) > 0 {
				switch field.Names[0].Name {
				case "values", "row", "scope", "bindings", "vals":
					scope = "row"
				}
			}
		}
	}
	return scope
}

func (a *boundaryAnalysis) function(file *ast.File, name string, fn *ast.FuncDecl) []boundaryFinding {
	var findings []boundaryFinding
	scope := a.scope(fn)
	marked := a.markedLines(file)
	obj, _ := a.pkg.info.Defs[fn.Name].(*types.Func)
	key := funcKey(a.pkg, obj)
	a.statementText = boundaryStatementEntries[key]
	defer func() { a.statementText = false }()
	// report records a pass outside the owner; data says its size comes from
	// the query's data, which no "bounded:" marker can bound.
	report := func(at ast.Node, kind, detail string, data bool) {
		line := a.pkg.fset.Position(at.Pos()).Line
		for _, at := range []int{line - 1, line} {
			reason, ok := marked[at]
			if !ok {
				continue
			}
			a.markersUsed[name+":"+strconv.Itoa(at)] = true
			if data {
				detail += " (a bounded: marker cannot bound a pass over data)"
			} else if !a.markerBound(reason) {
				detail += " (a bounded: marker must state a number or an integer constant: " + reason + ")"
			} else {
				return
			}
			break
		}
		findings = append(findings, boundaryFinding{File: name, Function: fn.Name.Name, Key: key, Line: line, Kind: kind, Detail: detail, Scope: scope, Observe: a.observeExpr(fn)})
	}
	var walk func(node ast.Node, observedPass bool)
	walk = func(node ast.Node, observedPass bool) {
		ast.Inspect(node, func(n ast.Node) bool {
			if n == node {
				return true
			}
			switch x := n.(type) {
			case *ast.RangeStmt:
				observed := a.ownerBatch(x.X) || a.ownerIteration(x.X) || a.dominates(x.Body) || spawnsWorkers(x.Body)
				_, channel := a.pkg.info.TypeOf(x.X).Underlying().(*types.Chan)
				if !observed && !channel && a.rangeSize(x.X) {
					report(x, "loop", "range "+bulkWorkExpr(x.X), true)
				}
				if x.X != nil {
					walk(x.X, observedPass)
				}
				walk(x.Body, observed || observedPass)
				return false
			case *ast.ForStmt:
				observed := a.dominates(x.Body) || spawnsWorkers(x.Body)
				if detail, data := a.forSize(x); !observed && data {
					report(x, "loop", "for "+detail, true)
				}
				walk(x.Body, observed || observedPass)
				return false
			case *ast.CallExpr:
				if owner := backgroundToOwner(a.pkg.info, x); owner != "" && scope != "none" {
					report(x, "context", "context.Background() handed to "+owner+" while the statement's context is in scope", true)
				}
				a.call(x, observedPass, report)
				if !boundaryOutsideStatement[key] {
					a.nilWalks(x, report)
				}
			}
			return true
		})
	}
	walk(fn.Body, false)
	return findings
}

// basicString reports a string type (no slice of them).
func basicString(typ types.Type) bool {
	if typ == nil {
		return false
	}
	basic, ok := typ.Underlying().(*types.Basic)
	return ok && basic.Info()&types.IsString != 0
}

// boundaryTextStops are the owner's text functions that stop like Check.
var boundaryTextStops = map[string]bool{
	"TrimSpace": true, "Trim": true, "TrimFunc": true, "EqualFold": true, "Index": true, "Contains": true,
	"ContainsAny": true, "ToUpper": true, "ToLower": true, "String": true, "Bytes": true,
	"UpperASCII": true, "LowerASCII": true,
}

// boundaryOwnerConstructors start an owner's observation of a statement.
var boundaryOwnerConstructors = map[string]bool{
	"newStorageScan": true, "valueWalk": true, "materializedBatches": true,
	"NewCheckpoint": true, "RowsCheckpoint": true, "WalkCheckpoint": true,
}

// backgroundToOwner names the owner constructor a call hands
// context.Background() or context.TODO() (an observation of no statement),
// or "".
func backgroundToOwner(info *types.Info, call *ast.CallExpr) string {
	f := calledFunc(info, call)
	if f == nil || f.Pkg() == nil || !boundaryOwnerConstructors[f.Name()] ||
		(f.Pkg().Path() != cypherPackagePath && f.Pkg().Path() != cypherPackagePath+"/bulk") {
		return ""
	}
	for _, arg := range call.Args {
		inner, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		if g := calledFunc(info, inner); g != nil && g.Pkg() != nil && g.Pkg().Path() == "context" && (g.Name() == "Background" || g.Name() == "TODO") {
			return f.Name()
		}
	}
	return ""
}

// stringResult reports a function with a string-like result.
func stringResult(f *types.Func) bool {
	sig, ok := f.Type().(*types.Signature)
	if !ok {
		return false
	}
	for i := 0; i < sig.Results().Len(); i++ {
		if stringLike(sig.Results().At(i).Type()) {
			return true
		}
	}
	return false
}

// boundaryOutsideStatement are the functions that run outside any statement
// (the HTTP decoder's JSON numbers, the public result cache's keys): their
// walks have no statement to observe.
var boundaryOutsideStatement = map[string]bool{
	"json_values.go:JSONNumbers":              true,
	"cache.go:*QueryCache.cacheKey":           true,
	"cache.go:resultCacheEntryKey":            true,
	"cache.go:*SmartQueryCache.Get":           true,
	"cache.go:*SmartQueryCache.PutWithLabels": true,
}

// nilWalks reports a call that hands a walk-taking function no walk (nil):
// the walk it makes over its value observes no statement.
func (a *boundaryAnalysis) nilWalks(call *ast.CallExpr, report func(ast.Node, string, string, bool)) {
	callee := calledFunc(a.pkg.info, call)
	if callee == nil {
		return
	}
	sig, ok := callee.Type().(*types.Signature)
	if !ok {
		return
	}
	data := false
	for _, arg := range call.Args {
		if a.dataValue(arg) {
			data = true
		}
	}
	if !data {
		// Statement text or no value: the walk has nothing data-sized to observe.
		return
	}
	for i, arg := range call.Args {
		if i >= sig.Params().Len() {
			break
		}
		ident, ok := arg.(*ast.Ident)
		if !ok || ident.Name != "nil" {
			continue
		}
		if isCheckpointType(sig.Params().At(i).Type()) {
			report(call, "walk", "nil walk to "+callee.Name(), true)
		}
	}
}

// markedLines are the lines of the file's "bounded: <reason>" comments.
func (a *boundaryAnalysis) markedLines(file *ast.File) map[int]string {
	lines := map[int]string{}
	for _, group := range file.Comments {
		if reason := boundedReason(group); reason != "" {
			lines[a.pkg.fset.Position(group.End()).Line] = reason
		}
	}
	return lines
}

// boundedReason is a comment group's "bounded: <reason>", or "".
func boundedReason(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	for _, line := range strings.Split(group.Text(), "\n") {
		if reason, ok := strings.CutPrefix(strings.TrimSpace(line), "bounded:"); ok && strings.TrimSpace(reason) != "" {
			return strings.TrimSpace(reason)
		}
	}
	return ""
}

// lengthOperand is the operand of a len() a loop condition compares with.
func (a *boundaryAnalysis) lengthOperand(cond ast.Expr) ast.Expr {
	var operand ast.Expr
	if cond == nil {
		return nil
	}
	ast.Inspect(cond, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && operand == nil {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "len" && len(call.Args) == 1 {
				operand = call.Args[0]
			}
		}
		return operand == nil
	})
	return operand
}

// ownerBatch reports a range over the owner's batches, or over one batch of
// a population.
func (a *boundaryAnalysis) ownerBatch(expr ast.Expr) bool {
	if call, ok := expr.(*ast.CallExpr); ok {
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "materializedBatches" {
			return true
		}
		if f := calledFunc(a.pkg.info, call); f != nil && f.Pkg() != nil && f.Pkg().Path() == cypherPackagePath+"/bulk" {
			return true
		}
	}
	if slice, ok := expr.(*ast.SliceExpr); ok {
		if selector, ok := slice.Low.(*ast.SelectorExpr); ok {
			if named, ok := a.pkg.info.TypeOf(selector.X).(*types.Named); ok && named.Obj().Name() == "materializedBatch" {
				return true
			}
		}
	}
	return false
}

// observes reports a body that checks the statement: a checkpoint, a scan
// or walk observation, a context's Err, or an owner primitive.
func (a *boundaryAnalysis) observes(body ast.Node) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		if _, lit := n.(*ast.FuncLit); lit {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			switch fun.Name {
			case "materializedBatches", "sortPopulation", "sortPopulationStable", "sortPopulationFunc", "stableSortRows", "sortStrings",
				"sortPopulationInterfaceStable", "appendPopulation", "copyPopulation", "clonePopulation", "clonePopulationMap":
				found = true
			}
		case *ast.SelectorExpr:
			switch fun.Sel.Name {
			case "check", "Check", "CheckBytes", "due", "canceled", "visit", "observe", "Observe", "ObserveSteps", "Err":
				found = true
			}
			if f := calledFunc(a.pkg.info, call); f != nil && f.Pkg() != nil && f.Pkg().Path() == cypherPackagePath+"/bulk" {
				found = true
			}
		}
		return !found
	})
	return found
}

func (a *boundaryAnalysis) call(call *ast.CallExpr, inObservedPass bool, report func(ast.Node, string, string, bool)) {
	info := a.pkg.info
	// A function that passes over a parameter, handed data there.
	if callee := calledFunc(info, call); callee != nil && len(a.passParams[callee]) > 0 {
		entry := boundaryStatementEntries[funcKey(a.pkg, callee)]
		for i, arg := range call.Args {
			if (entry || a.statementText) && stringLike(info.TypeOf(arg)) {
				// Statement text enters at a statement entry, held to the
				// request and dynamic statement limits: reading it is the
				// statement's own work (a stated limitation of the front end).
				continue
			}
			if a.passParams[callee][i] && a.dataSized(arg) {
				report(call, "pass", callee.Name()+" over data ("+bulkWorkExpr(arg)+") @"+funcKey(a.pkg, callee), true)
				break
			}
		}
	}
	// A conversion of a data string or its bytes copies all of it; one
	// between string types (storage.NodeID(s)) copies nothing.
	if tv, ok := info.Types[call.Fun]; ok && !a.statementText && tv.IsType() && len(call.Args) == 1 && stringLike(tv.Type) && a.dataSized(call.Args[0]) &&
		!(basicString(tv.Type) && basicString(info.TypeOf(call.Args[0]))) {
		report(call, "call", "conversion "+types.ExprString(call.Fun)+"("+bulkWorkExpr(call.Args[0])+")", true)
		return
	}
	if f := calledFunc(info, call); f != nil && f.Pkg() != nil {
		data := false
		typeOnly := fmtTypeOnlyArguments(info, f, call)
		for index, arg := range call.Args {
			if typeOnly[index] {
				// Formatted by %T (its type's name) or %p: no pass over it.
				continue
			}
			if a.dataValue(arg) {
				data = true
			}
		}
		switch {
		case f.Pkg().Path() == "fmt" && (strings.HasPrefix(f.Name(), "Sprint") || strings.HasPrefix(f.Name(), "Fprint") || f.Name() == "Errorf") && data:
			report(call, "call", "fmt."+f.Name()+" of a value", true)
			return
		case (strings.HasPrefix(f.FullName(), "(*strings.Builder).") || strings.HasPrefix(f.FullName(), "(*bytes.Buffer).")) && data:
			switch f.Name() {
			case "WriteString", "Write", "Grow":
				report(call, "call", f.FullName()+"("+bulkWorkExpr(call.Args[0])+")", true)
				return
			}
		}
		if reason, limited := boundaryLimitations[f.Pkg().Path()]; limited && data && a.ledgerLimitations != nil {
			a.ledgerLimitations[f.Pkg().Path()+"."+f.Name()] = reason
		}
	}
	if builtin, ok := call.Fun.(*ast.Ident); ok {
		switch info.Uses[builtin] {
		case types.Universe.Lookup("append"):
			if call.Ellipsis.IsValid() && len(call.Args) == 2 && isQuerySizedValue(info.TypeOf(call.Args[1])) && a.holds(call.Args[1]) {
				report(call, "call", "append of all of "+bulkWorkExpr(call.Args[1]), true)
			}
		case types.Universe.Lookup("copy"):
			if len(call.Args) == 2 && isQuerySizedValue(info.TypeOf(call.Args[1])) && a.holds(call.Args[1]) {
				report(call, "call", "copy of "+bulkWorkExpr(call.Args[1]), true)
			}
		}
		return
	}
	f := calledFunc(info, call)
	if f == nil || f.Pkg() == nil {
		return
	}
	kind := f.Pkg().Name() + "." + f.Name()
	switch {
	case boundarySortsAndCopies[kind]:
		if len(call.Args) > 0 && (strings.HasPrefix(kind, "sort.") || strings.HasPrefix(kind, "slices.Sort") || a.dataSized(call.Args[0])) {
			report(call, "call", kind+"("+bulkWorkExpr(call.Args[0])+")", a.dataSized(call.Args[0]))
		}
	case f.Pkg().Path() == "strings" && boundaryStringPasses[f.Name()] && !a.statementText:
		data := false
		for _, arg := range call.Args {
			if a.dataSized(arg) {
				data = true
			}
		}
		// Repeat's output is sized by its count.
		if f.Name() == "Repeat" && len(call.Args) == 2 {
			if tv, ok := info.Types[call.Args[1]]; !ok || tv.Value == nil {
				data = true
			}
		}
		if data {
			report(call, "call", kind+"("+bulkWorkExpr(call.Args[0])+")", true)
		}
	case boundaryEncoders[f.FullName()]:
		for _, arg := range call.Args {
			typ := info.TypeOf(arg)
			if (cypherValueType(typ) || isQuerySizedValue(typ)) && a.holds(arg) {
				report(call, "call", f.FullName()+"("+bulkWorkExpr(arg)+")", true)
				break
			}
		}
	}
}

// recoversStop reports a body that defers bulk.RecoverStop.
func (a *boundaryAnalysis) recoversStop(body *ast.BlockStmt) bool {
	if body == nil {
		return false
	}
	for _, statement := range body.List {
		if deferred, ok := statement.(*ast.DeferStmt); ok {
			if f := calledFunc(a.pkg.info, deferred.Call); f != nil && f.Pkg() != nil && f.Pkg().Path() == cypherPackagePath+"/bulk" && f.Name() == "RecoverStop" {
				return true
			}
		}
	}
	return false
}

// goroutines reports a go statement whose goroutine does not run inside the
// owner's boundary: a stop cannot cross goroutines, so each goroutine
// recovers its own and hands the error to the goroutine that waits for it.
func (a *boundaryAnalysis) goroutines(name string, fn *ast.FuncDecl) []boundaryFinding {
	var findings []boundaryFinding
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		statement, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}
		var body *ast.BlockStmt
		if lit, ok := statement.Call.Fun.(*ast.FuncLit); ok {
			body = lit.Body
		} else if f := calledFunc(a.pkg.info, statement.Call); f != nil && a.decls[f] != nil {
			body = a.decls[f].Body
		}
		if !a.recoversStop(body) {
			findings = append(findings, boundaryFinding{File: name, Function: fn.Name.Name, Line: a.pkg.fset.Position(statement.Pos()).Line,
				Kind: "go", Detail: "goroutine outside the owner's boundary (defer bulk.RecoverStop)", Scope: a.scope(fn)})
		}
		return true
	})
	return findings
}

// ownerStop reports a call that raises the owner's stop: its check, or a
// panic re-raising bulk.Stopped after a worker handed it over.
func (a *boundaryAnalysis) ownerStop(call *ast.CallExpr) bool {
	if callee := calledFunc(a.pkg.info, call); callee != nil {
		switch callee.FullName() {
		case "(*" + cypherPackagePath + "/bulk.Checkpoint).Check", "(*" + cypherPackagePath + "/bulk.Checkpoint).CheckBytes":
			return true
		}
		return callee.Pkg() != nil && callee.Pkg().Path() == cypherPackagePath+"/bulk" && boundaryTextStops[callee.Name()]
	}
	if ident, ok := call.Fun.(*ast.Ident); !ok || ident.Name != "panic" || len(call.Args) != 1 {
		return false
	}
	literal, ok := call.Args[0].(*ast.CompositeLit)
	if !ok {
		return false
	}
	named, ok := a.pkg.info.TypeOf(literal).(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == cypherPackagePath+"/bulk" && named.Obj().Name() == "Stopped"
}

// stopCalls visits each call in body that runs outside a boundary: a
// function literal handed to insideStopBoundary, and a goroutine (which
// recovers its own, see goroutines), are boundaries. A function literal
// stored for later (in a composite literal, returned, or handed to a package
// function that never calls that parameter, such as a registration) runs
// when it is called, not here.
func (a *boundaryAnalysis) stopCalls(body ast.Node, visit func(*ast.CallExpr)) {
	stored := a.storedFuncLits(body)
	ast.Inspect(body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.GoStmt:
			return false
		case *ast.FuncLit:
			return !stored[node]
		case *ast.CallExpr:
			visit(node)
			if callee := calledFunc(a.pkg.info, node); callee != nil && callee.Name() == "insideStopBoundary" && callee.Pkg() != nil && callee.Pkg().Path() == cypherPackagePath {
				return false
			}
		}
		return true
	})
}

// storedFuncLits are the function literals in body stored for later: in a
// composite literal, returned, or handed to a package function that never
// calls that parameter (callsParam), such as a registration.
func (a *boundaryAnalysis) storedFuncLits(body ast.Node) map[*ast.FuncLit]bool {
	stored := map[*ast.FuncLit]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CompositeLit:
			for _, element := range node.Elts {
				if pair, ok := element.(*ast.KeyValueExpr); ok {
					element = pair.Value
				}
				if lit, ok := element.(*ast.FuncLit); ok {
					stored[lit] = true
				}
			}
		case *ast.ReturnStmt:
			for _, result := range node.Results {
				if lit, ok := result.(*ast.FuncLit); ok {
					stored[lit] = true
				}
			}
		case *ast.CallExpr:
			callee := calledFunc(a.pkg.info, node)
			decl := a.decls[callee]
			if callee == nil || decl == nil {
				return true
			}
			for i, arg := range node.Args {
				if lit, ok := arg.(*ast.FuncLit); ok && !a.callsParam(decl, i) {
					stored[lit] = true
				}
			}
		}
		return true
	})
	return stored
}

// callsParam reports a package function that calls its index-th parameter
// (a function value): itself, or by handing it to a function that does (any
// function outside the package is assumed to). One that only stores it, as a
// registration does, does not.
func (a *boundaryAnalysis) callsParam(decl *ast.FuncDecl, index int) bool {
	if a.paramCalls == nil {
		a.paramCalls = map[boundaryParam]bool{}
	}
	k := boundaryParam{decl, index}
	if called, seen := a.paramCalls[k]; seen {
		return called
	}
	// Assumed not called while it is being decided (a recursive hand-off).
	a.paramCalls[k] = false
	var param *ast.Ident
	at := 0
	for _, field := range decl.Type.Params.List {
		names := field.Names
		if len(names) == 0 {
			names = []*ast.Ident{nil}
		}
		for _, name := range names {
			if at == index {
				param = name
			}
			at++
		}
	}
	if param == nil || index >= at {
		// An unnamed parameter, or a variadic one: assume it is called.
		a.paramCalls[k] = true
		return true
	}
	obj := a.pkg.info.Defs[param]
	called := false
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || called {
			return !called
		}
		if ident, ok := call.Fun.(*ast.Ident); ok && a.pkg.info.Uses[ident] == obj {
			called = true
			return false
		}
		for i, arg := range call.Args {
			if ident, ok := arg.(*ast.Ident); ok && a.pkg.info.Uses[ident] == obj {
				callee := calledFunc(a.pkg.info, call)
				if next := a.decls[callee]; callee == nil || next == nil || a.callsParam(next, i) {
					called = true
				}
			}
		}
		return !called
	})
	a.paramCalls[k] = called
	return called
}

// stoppers is the fixed point of the package functions that can stop: a
// function stops when, outside a boundary, it raises the owner's stop or
// calls a function that can. A function that defers bulk.RecoverStop is
// itself a boundary and never stops.
func (a *boundaryAnalysis) stoppers() map[*types.Func]bool {
	stops := map[*types.Func]bool{}
	a.stopVia, a.stopNext = map[*types.Func]string{}, map[*types.Func]*types.Func{}
	for changed := true; changed; {
		changed = false
		for f, decl := range a.decls {
			if stops[f] || a.recoversStop(decl.Body) {
				continue
			}
			a.stopCalls(decl.Body, func(call *ast.CallExpr) {
				if stops[f] {
					return
				}
				if callee := calledFunc(a.pkg.info, call); callee != nil && stops[callee] || a.ownerStop(call) {
					stops[f] = true
					changed = true
					if !a.ownerStop(call) {
						a.stopNext[f] = callee
					}
					at := a.pkg.fset.Position(call.Pos())
					a.stopVia[f] = fmt.Sprintf("%s:%d %s", filepath.Base(at.Filename), at.Line, types.ExprString(call.Fun))
				}
			})
		}
	}
	return stops
}

// stopChain is the calls from f down to the owner's stop, for a finding.
func (a *boundaryAnalysis) stopChain(f *types.Func) string {
	var chain []string
	seen := map[*types.Func]bool{}
	for f != nil && !seen[f] && len(chain) < 12 {
		seen[f] = true
		via, ok := a.stopVia[f]
		if !ok {
			break
		}
		chain = append(chain, via)
		f = a.stopNext[f]
	}
	return strings.Join(chain, " -> ")
}

// stopFindings reports the two places an unwinding stop must never reach:
// a caller outside the package (an exported entry that can stop), and a
// held lock or a shared cache, index or registry partly updated (a function
// that takes a lock and can stop).
func (a *boundaryAnalysis) stopFindings(files []*ast.File) []boundaryFinding {
	stops := a.stoppers()
	var findings []boundaryFinding
	for _, file := range files {
		name := filepath.Base(a.pkg.fset.Position(file.Pos()).Filename)
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			f, _ := a.pkg.info.Defs[fn.Name].(*types.Func)
			if f != nil && stops[f] && fn.Name.IsExported() && !boundaryStopAPI[funcKey(a.pkg, f)] {
				findings = append(findings, boundaryFinding{File: name, Function: fn.Name.Name, Line: a.pkg.fset.Position(fn.Pos()).Line,
					Kind: "escape", Detail: "an exported entry can stop: the stop must not leave the package (defer bulk.RecoverStop) via " + a.stopChain(f), Scope: a.scope(fn)})
			}
			if at, via := a.stopUnderLock(fn.Body, stops); at != token.NoPos {
				findings = append(findings, boundaryFinding{File: name, Function: fn.Name.Name, Line: a.pkg.fset.Position(at).Line,
					Kind: "lock", Detail: "can stop while it holds a lock or updates shared state via " + via, Scope: a.scope(fn)})
			}
		}
	}
	return findings
}

// stopUnderLock returns the first stop raised while body holds a lock: from
// Lock or RLock to its Unlock, or to the end of the function when the unlock
// is deferred (the unwind releases it, but the state it guards may be partly
// updated).
func (a *boundaryAnalysis) stopUnderLock(body *ast.BlockStmt, stops map[*types.Func]bool) (token.Pos, string) {
	type region struct {
		from, to token.Pos
		block    *ast.BlockStmt
		// recorder: the expression failure slot's lock, which recording a
		// failure (Observe, Check, recordExpressionFailure) takes again.
		recorder bool
	}
	var held []region
	open := map[string]int{}
	// An unlock ends the region only in the lock's own block: one in a
	// branch (an early return's) leaves the lock held on the other path.
	blockOf := map[*ast.CallExpr]*ast.BlockStmt{}
	var stack []ast.Node
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if call, ok := n.(*ast.CallExpr); ok {
			for i := len(stack) - 1; i >= 0; i-- {
				if block, ok := stack[i].(*ast.BlockStmt); ok {
					blockOf[call] = block
					break
				}
			}
		}
		stack = append(stack, n)
		return true
	})
	a.stopCalls(body, func(call *ast.CallExpr) {
		callee := calledFunc(a.pkg.info, call)
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if callee == nil || !ok {
			return
		}
		mutex := types.ExprString(selector.X)
		switch callee.FullName() {
		case "(*sync.Mutex).Lock", "(*sync.RWMutex).Lock", "(*sync.RWMutex).RLock":
			open[mutex] = len(held)
			held = append(held, region{call.Pos(), body.End(), blockOf[call], a.failureSlotLock(selector.X)})
		case "(*sync.Mutex).Unlock", "(*sync.RWMutex).Unlock", "(*sync.RWMutex).RUnlock":
			if index, ok := open[mutex]; ok && !a.deferred(body, call) && blockOf[call] == held[index].block {
				held[index].to = call.Pos()
				delete(open, mutex)
			}
		}
	})
	at, via := token.NoPos, ""
	a.stopCalls(body, func(call *ast.CallExpr) {
		callee := calledFunc(a.pkg.info, call)
		records := callee != nil && (callee.Name() == "recordExpressionFailure" ||
			callee.FullName() == "(*"+cypherPackagePath+"/bulk.Checkpoint).Observe")
		if at != token.NoPos {
			return
		}
		for _, r := range held {
			if records && r.recorder && call.Pos() > r.from && call.Pos() < r.to {
				at, via = call.Pos(), types.ExprString(call.Fun)+" (records the failure under the slot's own lock: a self-deadlock)"
				return
			}
		}
		if !(callee != nil && stops[callee] || a.ownerStop(call)) {
			return
		}
		for _, r := range held {
			if call.Pos() > r.from && call.Pos() < r.to {
				at, via = call.Pos(), types.ExprString(call.Fun)
				if callee != nil && stops[callee] {
					via += " -> " + a.stopChain(callee)
				}
				return
			}
		}
	})
	return at, via
}

// failureSlotLock reports a mutex that is an expression failure slot's
// lock (failure.mu): the lock the stop recorder takes.
func (a *boundaryAnalysis) failureSlotLock(mutex ast.Expr) bool {
	selector, ok := mutex.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	typ := a.pkg.info.TypeOf(selector.X)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = pointer.Elem()
	}
	named, ok := typ.(*types.Named)
	return ok && named.Obj().Name() == "expressionFailure"
}

// deferred reports a call that is the call of a defer statement in body.
func (a *boundaryAnalysis) deferred(body ast.Node, call *ast.CallExpr) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if statement, ok := n.(*ast.DeferStmt); ok && statement.Call == call {
			found = true
		}
		return !found
	})
	return found
}

// TestCancellationBoundary: every data-sized pass in production pkg/cypher
// and pkg/cypher/fn runs through the owner or is marked bounded.
func TestCancellationBoundary(t *testing.T) {
	for _, spec := range []struct{ pattern, path string }{{".", cypherPackagePath}, {"./fn", cypherPackagePath + "/fn"}} {
		t.Run(filepath.Base(spec.path), func(t *testing.T) {
			pkg := loadPackageForGuard(t, spec.pattern, spec.path, nil)
			findings := cancellationBoundaryFindings(pkg, pkg.files)
			if os.Getenv("NORNICDB_BOUNDARY_INVENTORY") != "" {
				for _, finding := range findings {
					encoded, _ := json.Marshal(finding)
					t.Logf("BOUNDARY %s %s", spec.path, encoded)
				}
			}
			if os.Getenv("NORNICDB_BOUNDARY_INVENTORY") != "" {
				for _, line := range passLoopInventory(pkg) {
					t.Logf("PASSLOOP %s %s", spec.path, line)
				}
				for _, edge := range boundaryCallGraph(pkg) {
					encoded, _ := json.Marshal(edge)
					t.Logf("CALLEDGE %s %s", spec.path, encoded)
				}
			}
			// The baseline lists the passes still to convert, by identity
			// (file, function, kind, pass): a finding outside it fails, and so
			// does a baseline entry no finding matches any more, so the list
			// only shrinks.
			fresh, stale := baselineViolations(findings, cancellationBoundaryBaseline(t, filepath.Base(spec.path)))
			require.Empty(t, fresh, "data-sized passes in %s must run through the cancellation owner or carry a reviewed `// bounded: <reason>` marker", spec.path)
			require.Empty(t, stale, "baseline entries in %s that no pass matches any more: remove them from %s", spec.path, cancellationBoundaryBaselineFile)
		})
	}
}

// baselineViolations are the findings the baseline does not list (fresh)
// and the baseline entries no finding matches any more (stale).
func baselineViolations(findings []boundaryFinding, baseline map[string]int) (fresh, stale []string) {
	left := make(map[string]int, len(baseline))
	for id, count := range baseline {
		left[id] = count
	}
	for _, finding := range findings {
		id := finding.baselineID()
		if left[id] > 0 {
			left[id]--
			continue
		}
		fresh = append(fresh, finding.String())
	}
	for id, count := range left {
		if count > 0 {
			stale = append(stale, id)
		}
	}
	sort.Strings(stale)
	return fresh, stale
}

// cancellationBoundaryBaselineFile lists, per package, the passes the
// cancellation owner does not cover yet.
const cancellationBoundaryBaselineFile = "testdata/cancellation_boundary_baseline.txt"

// baselineID is a finding's identity in the baseline: its line number is not
// part of it, so unrelated edits leave the baseline as it is.
func (f boundaryFinding) baselineID() string {
	return f.File + "|" + f.Function + "|" + f.Kind + "|" + f.Detail
}

// cancellationBoundaryBaseline reads the baseline entries of one package
// ("cypher", "fn"), counted, since a function can hold the same pass twice.
func cancellationBoundaryBaseline(t *testing.T, pkg string) map[string]int {
	t.Helper()
	data, err := os.ReadFile(cancellationBoundaryBaselineFile)
	require.NoError(t, err)
	entries := map[string]int{}
	for _, line := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if id, ok := strings.CutPrefix(line, pkg+"|"); ok {
			entries[id]++
		}
	}
	return entries
}

// callerOf is the function fn declares.
func callerOf(pkg *guardPackage, fn *ast.FuncDecl) *types.Func {
	f, _ := pkg.info.Defs[fn.Name].(*types.Func)
	return f
}

// funcLitContext is the name of a function literal's context.Context
// parameter, or "".
func funcLitContext(pkg *guardPackage, lit *ast.FuncLit) string {
	for _, field := range lit.Type.Params.List {
		if named, ok := pkg.info.TypeOf(field.Type).(*types.Named); ok && named.Obj().Pkg() != nil &&
			named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context" && len(field.Names) > 0 && field.Names[0].Name != "_" {
			return field.Names[0].Name
		}
	}
	return ""
}

// boundaryCallEdge is one call between package functions, with the scope
// the caller can observe the statement through.
type boundaryCallEdge struct {
	CalleeKey   string `json:"calleeKey"`
	CallerKey   string `json:"callerKey"`
	Column      int    `json:"column"`
	Caller      string `json:"caller"`
	CallerFile  string `json:"callerFile"`
	CallerScope string `json:"callerScope"`
	// CallerObserve reaches the caller's statement (see observeExpr).
	CallerObserve string `json:"callerObserve,omitempty"`
	// Value is set for a use of the function as a value, not a call.
	Value  bool   `json:"value,omitempty"`
	Callee string `json:"callee"`
	Line   int    `json:"line"`
}

// boundaryCallGraph lists the package's calls to its own functions.
func boundaryCallGraph(pkg *guardPackage) []boundaryCallEdge {
	a := &boundaryAnalysis{pkg: pkg, tainted: map[types.Object]bool{}, decls: map[*types.Func]*ast.FuncDecl{}}
	for _, file := range pkg.files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				if obj, ok := pkg.info.Defs[fn.Name].(*types.Func); ok {
					a.decls[obj] = fn
				}
			}
		}
	}
	var edges []boundaryCallEdge
	path := ""
	for _, obj := range pkg.info.Defs {
		if f, ok := obj.(*types.Func); ok && f.Pkg() != nil {
			path = f.Pkg().Path()
			break
		}
	}
	for _, file := range pkg.files {
		name := filepath.Base(pkg.fset.Position(file.Pos()).Filename)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			scope := a.scope(fn)
			observe := a.observeExpr(fn)
			stored := a.storedFuncLits(fn.Body)
			called := map[*ast.Ident]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						called[fun] = true
					case *ast.SelectorExpr:
						called[fun.Sel] = true
					}
				}
				return true
			})
			callerKey := funcKey(pkg, callerOf(pkg, fn))
			var inspect func(n ast.Node) bool
			inspect = func(n ast.Node) bool {
				// A stored literal runs later, as its own caller: one with a
				// context parameter reaches its statement through it.
				if lit, ok := n.(*ast.FuncLit); ok && stored[lit] {
					outerKey, outerScope, outerObserve := callerKey, scope, observe
					at := pkg.fset.Position(lit.Pos())
					callerKey, scope, observe = fmt.Sprintf("%s:func@%d", name, at.Line), "none", ""
					if ctxName := funcLitContext(pkg, lit); ctxName != "" {
						scope, observe = "ctx", ctxName
					}
					ast.Inspect(lit.Body, inspect)
					callerKey, scope, observe = outerKey, outerScope, outerObserve
					return false
				}
				if id, ok := n.(*ast.Ident); ok && !called[id] {
					if f, ok := pkg.info.Uses[id].(*types.Func); ok && f.Pkg() != nil && f.Pkg().Path() == path {
						at := pkg.fset.Position(id.Pos())
						edges = append(edges, boundaryCallEdge{CalleeKey: funcKey(pkg, f), CallerKey: callerKey, Column: at.Column, Value: true,
							Caller: fn.Name.Name, CallerFile: name, CallerScope: scope, CallerObserve: observe, Callee: f.Name(), Line: at.Line})
					}
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if f := calledFunc(pkg.info, call); f != nil && f.Pkg() != nil && f.Pkg().Path() == path {
					at := pkg.fset.Position(call.Lparen)
					edges = append(edges, boundaryCallEdge{CalleeKey: funcKey(pkg, f), CallerKey: callerKey, Column: at.Column,
						Caller: fn.Name.Name, CallerFile: name, CallerScope: scope, CallerObserve: observe, Callee: f.Name(), Line: at.Line})
				}
				return true
			}
			ast.Inspect(fn.Body, inspect)
		}
	}
	return edges
}

// funcKey names a function unambiguously: its file, receiver type and name.
func funcKey(pkg *guardPackage, f *types.Func) string {
	if f == nil {
		return ""
	}
	recv := ""
	if sig, ok := f.Type().(*types.Signature); ok && sig.Recv() != nil {
		recv = types.TypeString(sig.Recv().Type(), func(*types.Package) string { return "" }) + "."
	}
	return filepath.Base(pkg.fset.Position(f.Pos()).Filename) + ":" + recv + f.Name()
}

var _ = token.NoPos
