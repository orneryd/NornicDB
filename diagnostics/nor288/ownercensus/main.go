// Author: NOR-288 lane (Claude), 2026-10-30.
// Behaviour-defined owner census over the compiled module (production code,
// whole module ./...). Membership is resolved by type and value, never by a
// callee's name: the module is type-checked, built into SSA, and a VTA call
// graph (refining CHA) resolves static calls, interface calls, method values
// and calls through function values or parameters to every function they may
// reach. Four contracts:
//
//	A. DROP-state decision, outside pkg/multidb (the owner). A deleting
//	   function is the owner's DropDatabase, DropCompositeDatabase or
//	   FinishPendingDrop, or any function (adapter, method value, bound or
//	   interface wrapper, closure) that calls a deleting function with a
//	   name taken from its own inputs: a string parameter or captured
//	   variable, or a value loaded from one through fields, pointers or
//	   conversions (a wrapper that reads the name from its receiver). A name
//	   produced by a call (parsing a statement) does not bind the caller. A deleting call is a
//	   call site whose resolved callees include a deleting function, whatever
//	   the called value is named. A probe is a call that may reach the owner's
//	   availability checks (Exists, ExistsOrIsConstituent, GetStorage,
//	   GetStorageWithAuth, GetDatabase, ResolveDatabase, IsCompositeDatabase)
//	   or a function whose every call is such a probe (a thin wrapper). A
//	   decider is an if whose condition tests a probe result and whose two
//	   paths differ in reaching a deleting call, where the probe and a
//	   deleting call take the same name argument. Expected: 0.
//	B. Accepted-outcome barrier construction: a composite literal whose type
//	   is storage.AcceptedCommitError (type identity). Expected: only
//	   storage.AcceptedOutcome.
//	C. Accepted-application endpoint decision: a function that uses the field
//	   BadgerTransaction.acceptedApplication and calls the method
//	   revalidateEndpointsLocked (object identity). Expected: only that method.
//	D. Lifetime or background-work primitive on a line added by
//	   git diff <base>...HEAD (base = the layer this PR stacks on): a go
//	   statement, a sync.WaitGroup variable or field, an errgroup call, or a
//	   context.With* lifetime call (object identity). The one owner is
//	   lifecycle.WorkLifetime: a launch is allowed only where the enclosing
//	   function enrolls through WorkLifetime.Acquire (object identity); any
//	   added WaitGroup or context lifetime outside pkg/lifecycle is a finding.
//
//	E. Second closing decider: on a line the diff adds, a read of a bool field
//	   of a struct that also holds a lifecycle.WorkLifetime (lifecycle state
//	   kept beside the owner; type and field-object identity). Closing state
//	   is read from the owner (WorkLifetime.Closing, Acquire). Expected: 0.
//
// The census runs on any head: FinishPendingDrop, AcceptedOutcome and
// WorkLifetime are optional, and a control that needs an absent API is
// adapted or omitted, which the output records.
//
// Planted controls enter only as compiler overlays; a missed or misreported
// control invalidates the run.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

const nornic = "github.com/orneryd/nornicdb/"

type control struct {
	path, marker, class, source string
	report                      bool
}

var controls = []control{
	{"pkg/server/zz_nor288_a_early.go", "nor288PlantedEarlyReturnDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedEarlyReturnDecider(ctx context.Context, m *multidb.DatabaseManager) error {
	if !m.Exists("planted") {
		return nil
	}
	return m.DropDatabase(ctx, "planted")
}
`, true},
	{"pkg/server/zz_nor288_a_storage.go", "nor288PlantedStorageProbeDecider", "A", `package server

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

type nor288PlantedStore struct {
	m      *multidb.DatabaseManager
	remove func(context.Context, string) error
}

func nor288NewPlantedStore(m *multidb.DatabaseManager) *nor288PlantedStore {
	return &nor288PlantedStore{m: m, remove: m.DropDatabase}
}

func (s *nor288PlantedStore) nor288PlantedStorageProbeDecider(ctx context.Context, name string) error {
	engine, err := s.m.GetStorage(name)
	if err != nil {
		return errors.New("not found")
	}
	_ = engine
	return s.remove(ctx, name)
}
`, true},
	{"pkg/server/zz_nor288_a_branch.go", "nor288PlantedBranchDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedBranchDecider(ctx context.Context, m *multidb.DatabaseManager) error {
	if m.Exists("planted") {
		return m.DropDatabase(ctx, "planted")
	}
	return nil
}
`, true},
	// A positive: the deletion is reached through a differently named
	// function value; only the value, not its name, makes it a deletion.
	{"pkg/server/zz_nor288_a_value.go", "nor288PlantedValueDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedValueDecider(ctx context.Context, m *multidb.DatabaseManager) error {
	purge := m.DropDatabase
	if !m.Exists("planted") {
		return nil
	}
	return purge(ctx, "planted")
}
`, true},
	// A positive: a wrapper derives the name from its receiver and deletes;
	// its caller probes that name and calls the wrapper without passing it.
	{"pkg/server/zz_nor288_a_derived.go", "nor288PlantedDerivedDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

type nor288PlantedTarget struct {
	m    *multidb.DatabaseManager
	name string
}

func (t *nor288PlantedTarget) remove(ctx context.Context) error {
	return t.m.DropDatabase(ctx, t.name)
}

func nor288PlantedDerivedDecider(ctx context.Context, t *nor288PlantedTarget) error {
	if !t.m.Exists(t.name) {
		return nil
	}
	return t.remove(ctx)
}
`, true},
	{"pkg/server/zz_nor288_a_replace.go", "nor288PlantedReplaceShape", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedReplaceShape(ctx context.Context, m *multidb.DatabaseManager, replace bool) error {
	if replace {
		return m.DropDatabase(ctx, "planted")
	} else if m.Exists("planted") {
		return nil
	}
	return nil
}
`, false},
	// A negative (original control): both paths reach a deletion; the
	// fall-through deletes through a parameter named drop, which its caller
	// binds to DropDatabase.
	{"pkg/server/zz_nor288_a_owner.go", "nor288PlantedOwnerRouted", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedOwnerRouted(ctx context.Context, m *multidb.DatabaseManager, drop func(context.Context, string) error) error {
	if _, err := m.GetStorage("planted"); err != nil {
		return m.FinishPendingDrop(ctx, "planted")
	}
	return drop(ctx, "planted")
}

func nor288PlantedOwnerRoutedCaller(ctx context.Context, m *multidb.DatabaseManager) error {
	return nor288PlantedOwnerRouted(ctx, m, m.DropDatabase)
}
`, false},
	{"pkg/server/zz_nor288_b_literal.go", "nor288PlantedBarrierLiteral", "B", `package server

import "github.com/orneryd/nornicdb/pkg/storage"

func nor288PlantedBarrierLiteral(err error) error {
	return &storage.AcceptedCommitError{State: storage.CommitRecoveryPending, Err: err}
}
`, true},
	{"pkg/server/zz_nor288_b_decoy.go", "nor288PlantedBarrierDecoy", "B", `package server

type AcceptedCommitError struct{ Err error }

func nor288PlantedBarrierDecoy(err error) error {
	_ = &AcceptedCommitError{Err: err}
	return err
}
`, false},
	{"pkg/storage/zz_nor288_c_twin.go", "nor288PlantedEndpointTwin", "C", `package storage

func (tx *BadgerTransaction) nor288PlantedEndpointTwin() error {
	if tx.acceptedApplication {
		return nil
	}
	return tx.revalidateEndpointsLocked()
}
`, true},
	{"pkg/multidb/zz_nor288_d_lifetime.go", "nor288PlantedLifetime", "D", `package multidb

import (
	"context"
	"sync"
)

type nor288PlantedRunner struct{ wg sync.WaitGroup }

func (r *nor288PlantedRunner) nor288PlantedLifetime(fn func(context.Context)) {
	ctx, cancel := context.WithCancel(context.Background())
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer cancel()
		fn(ctx)
	}()
}
`, true},
	{"pkg/server/zz_nor288_d_owner.go", "nor288PlantedOwnerLaunch", "D", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

func nor288PlantedOwnerLaunch(w *lifecycle.WorkLifetime, fn func(context.Context)) bool {
	ctx, release, ok := w.Acquire(w.Context())
	if !ok {
		return false
	}
	go func() {
		defer release()
		fn(ctx)
	}()
	return true
}
`, false},
	// E: a second closing decider beside the engine's lifecycle owner.
	{"pkg/storage/zz_nor288_e_flag.go", "nor288PlantedClosingDecider", "E", `package storage

func (b *BadgerEngine) nor288PlantedClosingDecider() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closing
}
`, true},
	// E negative: closing state read from the owner itself.
	{"pkg/storage/zz_nor288_e_owner.go", "nor288PlantedOwnerClosing", "E", `package storage

func (b *BadgerEngine) nor288PlantedOwnerClosing() bool {
	return b.work.Closing()
}
`, false},
	// F: a write admitted by the owner's late phase (admission sealed) instead
	// of the engine's write decider; Close rejects writers before that phase.
	{"pkg/storage/zz_nor288_f_phase.go", "nor288PlantedLatePhaseWrite", "F", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedLatePhaseWrite(p *PreparedEntityCommit) error {
	if b.work.Closing() {
		return ErrStorageClosed
	}
	_, err := b.SubmitEntityCommit(context.Background(), p, nil)
	return err
}
`, true},
	// F negative: the write asks the engine's one write decider.
	{"pkg/storage/zz_nor288_f_decider.go", "nor288PlantedWriteDecider", "F", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedWriteDecider(p *PreparedEntityCommit) error {
	if err := b.ensureOpen(true); err != nil {
		return err
	}
	_, err := b.SubmitEntityCommit(context.Background(), p, nil)
	return err
}
`, false},
	// G: enrolled accepted work reaching a call with a context Close cannot
	// cancel, directly and one call away (the shape of the WAL initial submit).
	{"pkg/storage/zz_nor288_g_direct.go", "nor288PlantedEnrolledBackground", "G", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedEnrolledBackground(p *PreparedEntityCommit) bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) { _, _ = b.SubmitEntityCommit(context.Background(), p, nil) })
}
`, true},
	{"pkg/storage/zz_nor288_g_hop.go", "nor288PlantedEnrolledHop", "G", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedEnrolledHopHelper(p *PreparedEntityCommit) {
	_, _ = b.SubmitEntityCommit(context.Background(), p, nil)
}

func (b *BadgerEngine) nor288PlantedEnrolledHop(p *PreparedEntityCommit) bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) { b.nor288PlantedEnrolledHopHelper(p) })
}
`, true},
	// G negative: enrolled work passes the owner's work context.
	{"pkg/storage/zz_nor288_g_owner.go", "nor288PlantedEnrolledOwnerContext", "G", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedEnrolledOwnerContext(p *PreparedEntityCommit) bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) { _, _ = b.SubmitEntityCommit(ctx, p, nil) })
}
`, false},
}

type finding struct {
	Class string `json:"class"`
	Func  string `json:"func"`
	File  string `json:"file"`
	Line  int    `json:"line"`
	Shape string   `json:"shape,omitempty"`
	Path  []string `json:"path,omitempty"`
	Added bool     `json:"added_line"`
}

var hunk = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

func addedLines(root, base string) map[string]map[int]bool {
	out := map[string]map[int]bool{}
	data, err := exec.Command("git", "-C", root, "diff", "-U0", "--no-color", base+"...HEAD", "--", "*.go").Output()
	if err != nil {
		panic(fmt.Sprintf("git diff: %v", err))
	}
	file := ""
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1<<20), 1<<26)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "+++ b/") {
			file = strings.TrimPrefix(line, "+++ b/")
			out[file] = map[int]bool{}
			continue
		}
		if m := hunk.FindStringSubmatch(line); m != nil && file != "" {
			start, _ := strconv.Atoi(m[1])
			count := 1
			if m[2] != "" {
				count, _ = strconv.Atoi(m[2])
			}
			for i := 0; i < count; i++ {
				out[file][start+i] = true
			}
		}
	}
	return out
}

func method(pkg *types.Package, typeName, name string) *types.Func {
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		panic("missing type " + pkg.Path() + "." + typeName)
	}
	m, _, _ := types.LookupFieldOrMethod(types.NewPointer(obj.Type()), true, pkg, name)
	fn, ok := m.(*types.Func)
	if !ok {
		panic("missing method " + typeName + "." + name)
	}
	return fn
}

func field(pkg *types.Package, typeName, name string) *types.Var {
	obj := pkg.Scope().Lookup(typeName)
	f, _, _ := types.LookupFieldOrMethod(obj.Type(), true, pkg, name)
	v, ok := f.(*types.Var)
	if !ok {
		panic("missing field " + typeName + "." + name)
	}
	return v
}

func objOf(fn *ssa.Function) *types.Func {
	if fn == nil {
		return nil
	}
	o, _ := fn.Object().(*types.Func)
	return o
}

func main() {
	root, base := os.Args[1], os.Args[2]
	overlay := map[string][]byte{}
	planted := map[string]bool{}
	// Controls that need an API this head lacks are adapted or omitted.
	hasFinish := sourceHas(root, "pkg/multidb", "func (m *DatabaseManager) FinishPendingDrop(")
	hasLifetime := sourceHas(root, "pkg/lifecycle", "func (w *WorkLifetime) Acquire(")
	hasEngineOwner := sourceHas(root, "pkg/storage", "work lifecycle.WorkLifetime")
	adaptations := map[string]string{}
	var active []control
	for _, c := range controls {
		if !hasFinish && strings.Contains(c.source, "m.FinishPendingDrop(") {
			c.source = strings.ReplaceAll(c.source, "m.FinishPendingDrop(", "m.DropDatabase(")
			adaptations[c.marker] = "FinishPendingDrop absent on this head: the routed branch deletes through DropDatabase"
		}
		if (c.class == "E" || c.class == "F" || c.class == "G") && !hasEngineOwner {
			adaptations[c.marker] = "the engine has no lifecycle owner on this head: control omitted"
			continue
		}
		if !hasLifetime && strings.Contains(c.source, "lifecycle.WorkLifetime") {
			adaptations[c.marker] = "WorkLifetime absent on this head: control omitted"
			continue
		}
		active = append(active, c)
		path := filepath.Join(root, filepath.FromSlash(c.path))
		overlay[path] = []byte(c.source)
		planted[path] = true
	}
	fset := token.NewFileSet()
	cfg := &packages.Config{Mode: packages.LoadAllSyntax, Dir: root, Fset: fset, Overlay: overlay, Tests: false,
		Env: append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=1", "GOWORK=off", "GOFLAGS=-p=2"), BuildFlags: []string{"-mod=readonly", "-race", "-tags=noui,nolocalllm"}}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		panic(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		panic("package load errors")
	}
	prog, _ := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	funcs := sourceFunctions(prog)
	cg := vta.CallGraph(funcs, cha.CallGraph(prog))
	graph := buildGraph(cg, funcs)

	byPath := map[string]*packages.Package{}
	packages.Visit(pkgs, nil, func(p *packages.Package) { byPath[p.PkgPath] = p })
	multidbPkg, storagePkg := byPath[nornic+"pkg/multidb"].Types, byPath[nornic+"pkg/storage"].Types
	owners := map[*types.Func]bool{}
	for _, n := range []string{"DropDatabase", "DropCompositeDatabase", "FinishPendingDrop"} {
		if fn := methodOpt(multidbPkg, "DatabaseManager", n); fn != nil {
			owners[fn] = true
		}
	}
	probeOwners := map[*types.Func]bool{}
	for _, n := range []string{"Exists", "ExistsOrIsConstituent", "GetStorage", "GetStorageWithAuth", "GetDatabase", "ResolveDatabase", "IsCompositeDatabase"} {
		probeOwners[method(multidbPkg, "DatabaseManager", n)] = true
	}
	constructor, _ := storagePkg.Scope().Lookup("AcceptedOutcome").(*types.Func)
	barrierType := storagePkg.Scope().Lookup("AcceptedCommitError").Type()
	acceptedField := field(storagePkg, "BadgerTransaction", "acceptedApplication")
	revalidate := method(storagePkg, "BadgerTransaction", "revalidateEndpointsLocked")
	var acquire, closingFn *types.Func
	var engineWork *types.Var
	if v := storagePkg.Scope().Lookup("BadgerEngine"); v != nil {
		engineWork = field(storagePkg, "BadgerEngine", "work")
	}
	writeDecider := method(storagePkg, "BadgerEngine", "ensureOpen")
	var lifetimeType types.Type
	if lp := byPath[nornic+"pkg/lifecycle"]; lp != nil {
		acquire = methodOpt(lp.Types, "WorkLifetime", "Acquire")
		closingFn = methodOpt(lp.Types, "WorkLifetime", "Closing")
		if obj := lp.Types.Scope().Lookup("WorkLifetime"); obj != nil {
			lifetimeType = obj.Type()
		}
	}
	// E: bool fields of every struct that also holds a lifecycle owner.
	stateFields := map[*types.Var]bool{}
	if lifetimeType != nil {
		for _, p := range byPath {
			if !strings.HasPrefix(p.PkgPath, nornic) || p.Types == nil {
				continue
			}
			for _, name := range p.Types.Scope().Names() {
				tn, ok := p.Types.Scope().Lookup(name).(*types.TypeName)
				if !ok {
					continue
				}
				st, ok := tn.Type().Underlying().(*types.Struct)
				if !ok {
					continue
				}
				hosts := false
				for i := 0; i < st.NumFields(); i++ {
					t := st.Field(i).Type()
					if ptr, ok := t.(*types.Pointer); ok {
						t = ptr.Elem()
					}
					hosts = hosts || types.Identical(t, lifetimeType)
				}
				if !hosts {
					continue
				}
				for i := 0; i < st.NumFields(); i++ {
					if basic, ok := st.Field(i).Type().Underlying().(*types.Basic); ok && basic.Kind() == types.Bool {
						stateFields[st.Field(i)] = true
					}
				}
			}
		}
	}

	// Deleting functions: the owner deletions, then every caller that passes
	// a name taken from its own inputs through to one of them.
	deleting := map[*ssa.Function]bool{}
	var queue []*ssa.Function
	for fn := range graph.funcs {
		if owners[objOf(fn)] {
			deleting[fn] = true
			queue = append(queue, fn)
		}
	}
	for len(queue) > 0 {
		callee := queue[0]
		queue = queue[1:]
		for _, e := range graph.in[callee] {
			if deleting[e.caller] || !passesInputName(e.site.Common(), e.caller) {
				continue
			}
			deleting[e.caller] = true
			queue = append(queue, e.caller)
		}
	}
	// Probe set: owner probes plus thin wrappers whose every call is a probe.
	probe := map[*ssa.Function]bool{}
	for fn := range graph.funcs {
		if probeOwners[objOf(fn)] {
			probe[fn] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for fn := range graph.funcs {
			out := graph.out[fn]
			if probe[fn] || len(out) == 0 {
				continue
			}
			thin := true
			for _, e := range out {
				thin = thin && probe[e.callee]
			}
			if thin {
				probe[fn] = true
				changed = true
			}
		}
	}
	deleteSite, probeSite := map[token.Pos]bool{}, map[token.Pos]bool{}
	calleesAt := map[token.Pos][]*ssa.Function{}
	for _, edges := range graph.out {
		for _, e := range edges {
			pos := e.site.Common().Pos()
			calleesAt[pos] = append(calleesAt[pos], e.callee)
			if deleting[e.callee] {
				deleteSite[pos] = true
			}
			if probe[e.callee] {
				probeSite[pos] = true
			}
		}
	}

	added := addedLines(root, base)
	rel := func(file string) string {
		r, err := filepath.Rel(root, file)
		if err != nil || strings.HasPrefix(r, "..") {
			return filepath.ToSlash(file)
		}
		return filepath.ToSlash(r)
	}
	var findings, lifetimeSites, receiptWaits []finding
	var candidatesA []string
	record := func(class, file string, fn *ast.FuncDecl, shape string) {
		findings = append(findings, finding{Class: class, Func: funcName(fn), File: rel(file), Line: fset.Position(fn.Pos()).Line, Shape: shape})
	}
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, nornic) {
			continue
		}
		info := p.TypesInfo
		for i, file := range p.Syntax {
			path := p.CompiledGoFiles[i]
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			inOwner := p.PkgPath == nornic+"pkg/multidb"
			isPlanted := planted[path]
			onAdded := func(pos token.Pos) bool { return isPlanted || added[rel(path)][fset.Position(pos).Line] }
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				fnObj, _ := info.Defs[fn.Name].(*types.Func)
				// A
				if !inOwner && hasSite(fn.Body, probeSite) {
					candidatesA = append(candidatesA, rel(path)+":"+funcName(fn))
					if decides(fn, probeSite, deleteSite) {
						record("A", path, fn, "probe result changes whether a deletion of the same name is reached")
					}
				}
				// B
				if fnObj != constructor {
					reported := false
					ast.Inspect(fn.Body, func(x ast.Node) bool {
						if lit, ok := x.(*ast.CompositeLit); ok && !reported {
							if tv, ok := info.Types[lit]; ok && types.Identical(tv.Type, barrierType) {
								record("B", path, fn, "constructs the accepted-outcome barrier")
								reported = true
							}
						}
						return !reported
					})
				}
				// C
				if fnObj != revalidate {
					usesField, callsRevalidate := false, false
					ast.Inspect(fn.Body, func(x ast.Node) bool {
						switch n := x.(type) {
						case *ast.SelectorExpr:
							if sel := info.Selections[n]; sel != nil && sel.Obj() == acceptedField {
								usesField = true
							}
						case *ast.CallExpr:
							for _, c := range calleesAt[n.Lparen] {
								if objOf(c) == revalidate {
									callsRevalidate = true
								}
							}
						}
						return true
					})
					if usesField && callsRevalidate {
						record("C", path, fn, "uses acceptedApplication and calls endpoint revalidation")
					}
				}
				// D
				inLifecycle := p.PkgPath == nornic+"pkg/lifecycle"
				enrolls := false
				ast.Inspect(fn.Body, func(x ast.Node) bool {
					if sel, ok := x.(*ast.SelectorExpr); ok && acquire != nil && info.Uses[sel.Sel] == acquire {
						enrolls = true
					}
					return true
				})
				ast.Inspect(fn.Body, func(x ast.Node) bool {
					switch n := x.(type) {
					case *ast.GoStmt:
						if !onAdded(n.Pos()) {
							return true
						}
						site := finding{Class: "D", Func: funcName(fn), File: rel(path), Line: fset.Position(n.Pos()).Line, Shape: "go"}
						if enrolls || inLifecycle {
							site.Shape += " (enrolled in lifecycle.WorkLifetime)"
						} else {
							findings = append(findings, site)
						}
						lifetimeSites = append(lifetimeSites, site)
					case *ast.CallExpr:
						var obj types.Object
						switch f := n.Fun.(type) {
						case *ast.SelectorExpr:
							obj = info.Uses[f.Sel]
						case *ast.Ident:
							obj = info.Uses[f]
						}
						fo, ok := obj.(*types.Func)
						if !ok || fo.Pkg() == nil || !onAdded(n.Pos()) || inLifecycle {
							return true
						}
						path := fo.Pkg().Path()
						if (path == "context" && strings.HasPrefix(fo.Name(), "With") && fo.Name() != "WithValue" && fo.Name() != "WithoutCancel") || path == "golang.org/x/sync/errgroup" {
							site := finding{Class: "D", Func: funcName(fn), File: rel(p.CompiledGoFiles[i]), Line: fset.Position(n.Pos()).Line, Shape: fo.Pkg().Name() + "." + fo.Name()}
							findings = append(findings, site)
							lifetimeSites = append(lifetimeSites, site)
						}
					}
					return true
				})
			}
			// E: reads of lifecycle state kept beside the owner, on added lines.
			if p.PkgPath != nornic+"pkg/lifecycle" {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || fn.Body == nil {
						continue
					}
					reported := false
					ast.Inspect(fn.Body, func(x ast.Node) bool {
						sel, ok := x.(*ast.SelectorExpr)
						if !ok || reported {
							return !reported
						}
						if s := info.Selections[sel]; s != nil {
							if v, ok := s.Obj().(*types.Var); ok && stateFields[v] && onAdded(sel.Pos()) {
								record("E", path, fn, "reads lifecycle state "+v.Name()+" kept beside the owner")
								reported = true
							}
						}
						return !reported
					})
				}
			}
			// F (whole unit, every line): a read of the engine owner's sealed
			// phase (BadgerEngine.work.Closing) outside the engine's one write
			// decider (ensureOpen). Close refuses writers before that phase;
			// no-write work enrolls through Acquire, which decides itself.
			// (Type and object identity.)
			if closingFn != nil && engineWork != nil && p.PkgPath != nornic+"pkg/lifecycle" {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || fn.Body == nil {
						continue
					}
					fnObj, _ := info.Defs[fn.Name].(*types.Func)
					reported := false
					ast.Inspect(fn.Body, func(x ast.Node) bool {
						sel, ok := x.(*ast.SelectorExpr)
						if !ok || reported {
							return !reported
						}
						if obj, ok := info.Uses[sel.Sel].(*types.Func); ok && obj == closingFn && fnObj != writeDecider {
							if recv, ok := sel.X.(*ast.SelectorExpr); ok {
								if rs := info.Selections[recv]; rs != nil && rs.Obj() == types.Object(engineWork) {
									record("F", path, fn, "decides on the engine owner's sealed phase instead of the write decider")
									findings[len(findings)-1].Added = onAdded(sel.Pos())
									findings[len(findings)-1].Line = fset.Position(sel.Pos()).Line
									reported = true
								}
							}
						}
						return !reported
					})
				}
			}
			// D: WaitGroup variables and fields declared on added lines (type identity).
			ast.Inspect(file, func(x ast.Node) bool {
				id, ok := x.(*ast.Ident)
				if !ok {
					return true
				}
				v, ok := info.Defs[id].(*types.Var)
				if !ok || v == nil || !isWaitGroup(v.Type()) || !onAdded(id.Pos()) {
					return true
				}
				site := finding{Class: "D", Func: "WaitGroup " + id.Name, File: rel(path), Line: fset.Position(id.Pos()).Line, Shape: "sync.WaitGroup"}
				if isPlanted {
					site.Func = "nor288PlantedLifetime WaitGroup " + id.Name
				}
				if p.PkgPath == nornic+"pkg/lifecycle" {
					site.Shape += " (the lifecycle owner)"
				} else {
					findings = append(findings, site)
				}
				lifetimeSites = append(lifetimeSites, site)
				return true
			})
		}
	}
	// G: enrolled accepted work (functions passed to RunAcceptedContinuation,
	// or launched by go in a function that enrolls through Acquire) and every
	// module function they reach (call graph) passing context.Background or
	// context.TODO to a call on an added line: Close's cancellation cannot
	// reach that call, so Close may wait on it without bound.
	if ctxPkg := byPath["context"]; ctxPkg != nil && hasEngineOwner {
		background := map[types.Object]bool{ctxPkg.Types.Scope().Lookup("Background"): true, ctxPkg.Types.Scope().Lookup("TODO"): true}
		runCont := map[types.Object]bool{storagePkg.Scope().Lookup("RunAcceptedContinuation"): true}
		if m := methodOpt(storagePkg, "BadgerEngine", "RunAcceptedContinuation"); m != nil {
			runCont[m] = true
		}
		staticObj := func(c *ssa.CallCommon) types.Object {
			if f := c.StaticCallee(); f != nil && f.Object() != nil {
				return f.Object()
			}
			return nil
		}
		fnValue := func(v ssa.Value) *ssa.Function {
			switch x := v.(type) {
			case *ssa.MakeClosure:
				f, _ := x.Fn.(*ssa.Function)
				return f
			case *ssa.Function:
				return x
			}
			return nil
		}
		var receiptType types.Type
		if obj := storagePkg.Scope().Lookup("CommitReceipt"); obj != nil {
			receiptType = types.NewPointer(obj.Type())
		}
		reached := map[*ssa.Function]bool{}
		type step struct {
			from *ssa.Function
			kind string
			pos  token.Pos
		}
		via := map[*ssa.Function]step{}
		var work []*ssa.Function
		enqueueVia := func(f, from *ssa.Function, kind string, pos token.Pos) {
			if f != nil && !reached[f] && f.Pkg != nil && strings.HasPrefix(f.Pkg.Pkg.Path(), nornic) {
				reached[f] = true
				via[f] = step{from, kind, pos}
				work = append(work, f)
			}
		}
		pathTo := func(f *ssa.Function) []string {
			var out []string
			for n := 0; f != nil && n < 64; n++ {
				st := via[f]
				where := ""
				if p := fset.Position(st.pos); p.Filename != "" {
					where = " @ " + rel(p.Filename) + ":" + strconv.Itoa(p.Line)
				}
				out = append([]string{f.String() + " [" + st.kind + where + "]"}, out...)
				f = st.from
			}
			return out
		}
		for f := range graph.funcs {
			enrolls := false
			for _, block := range f.Blocks {
				for _, instr := range block.Instrs {
					if c, ok := instr.(ssa.CallInstruction); ok && acquire != nil && staticObj(c.Common()) == types.Object(acquire) {
						enrolls = true
					}
				}
			}
			for _, block := range f.Blocks {
				for _, instr := range block.Instrs {
					c, ok := instr.(ssa.CallInstruction)
					if !ok {
						continue
					}
					if o := staticObj(c.Common()); o != nil && runCont[o] {
						for _, a := range c.Common().Args {
							enqueueVia(fnValue(a), f, "root: argument of RunAcceptedContinuation", c.Pos())
						}
					}
					if g, ok := instr.(*ssa.Go); ok && enrolls {
						enqueueVia(g.Call.StaticCallee(), f, "root: go in a function enrolling through Acquire", g.Pos())
					}
				}
			}
		}
		for len(work) > 0 {
			f := work[0]
			work = work[1:]
			for _, e := range graph.out[f] {
				kind := "call"
				if _, isGo := e.site.(*ssa.Go); isGo {
					kind = "go"
				}
				if graph.fallback[e.site] {
					kind = "signature fallback (" + kind + ")"
				}
				enqueueVia(e.callee, f, kind, e.site.Pos())
			}
			for _, anon := range f.AnonFuncs {
				enqueueVia(anon, f, "anonymous function of", anon.Pos())
			}
		}
		seen := map[string]bool{}
		for f := range reached {
			for _, block := range f.Blocks {
				for _, instr := range block.Instrs {
					c, ok := instr.(ssa.CallInstruction)
					if !ok {
						continue
					}
					// A wait on a commit receipt is bounded by the enrolled work
					// that resolves it (Close cancels that work, which resolves
					// the receipt); only outgoing calls are in scope.
					if callee := c.Common().StaticCallee(); callee != nil && callee.Signature.Recv() != nil && receiptType != nil && types.Identical(callee.Signature.Recv().Type(), receiptType) {
						if pos := fset.Position(c.Pos()); pos.Filename != "" && !planted[pos.Filename] {
							name := f.String()
							if i := strings.LastIndex(name, "."); i >= 0 {
								name = name[i+1:]
							}
							receiptWaits = append(receiptWaits, finding{Class: "G-wait", Func: name + " -> " + callee.Name(), File: rel(pos.Filename), Line: pos.Line, Path: pathTo(f), Added: added[rel(pos.Filename)][pos.Line]})
						}
						continue
					}
					for _, a := range c.Common().Args {
						call, ok := a.(*ssa.Call)
						if !ok {
							continue
						}
						o := staticObj(call.Common())
						if o == nil || !background[o] {
							continue
						}
						pos := fset.Position(call.Pos())
						if pos.Filename == "" {
							continue
						}
						key := rel(pos.Filename) + ":" + strconv.Itoa(pos.Line)
						if seen[key] {
							continue
						}
						seen[key] = true
						name := f.String()
						if i := strings.LastIndex(name, "."); i >= 0 {
							name = name[i+1:]
						}
						findings = append(findings, finding{Class: "G", Func: name, File: rel(pos.Filename), Line: pos.Line, Shape: "context." + o.Name() + " in enrolled work", Path: pathTo(f), Added: added[rel(pos.Filename)][pos.Line]})
					}
				}
			}
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Class != findings[j].Class {
			return findings[i].Class < findings[j].Class
		}
		return findings[i].File+findings[i].Func < findings[j].File+findings[j].Func
	})
	sort.Strings(candidatesA)
	verdicts, valid := map[string]string{}, true
	for _, c := range active {
		found := false
		for _, f := range findings {
			found = found || (f.Class == c.class && strings.Contains(f.Func, c.marker))
		}
		ok := found == c.report
		valid = valid && ok
		verdicts[c.class+":"+c.marker] = fmt.Sprintf("expected reported=%v found=%v pass=%v", c.report, found, ok)
	}
	beyond := map[string][]finding{"A": {}, "B": {}, "C": {}, "D": {}, "E": {}, "F": {}, "G": {}}
	for _, f := range findings {
		if !strings.Contains(f.Func, "nor288Planted") {
			beyond[f.Class] = append(beyond[f.Class], f)
		}
	}
	counts := map[string]int{}
	for k, v := range beyond {
		counts[k] = len(v)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"author":                    "NOR-288 lane (Claude),30October2026",
		"method":                    "packages.Load whole module ./... (production, overlays), SSA, VTA call graph refining CHA; membership by object/type identity and call-graph reachability, never by callee name; diff scope git diff " + base + "...HEAD; planted controls via compiler overlay only; no product execution",
		"callgraph_nodes":           len(cg.Nodes),
		"call_sites":                len(graph.sites),
		"sites_via_signature":       graph.fallbackSites,
		"deleting_functions":        len(deleting),
		"probe_functions":           len(probe),
		"controls":                  verdicts,
		"control_adaptations":       adaptations,
		"valid":                     valid,
		"beyond_owner_count":        counts,
		"beyond_owner":              beyond,
		"receipt_waits":             receiptWaits,
		"scope_FG":                  "whole causal unit, every line (audit #30); added_line marks this diff",
		"findings":                  findings,
		"probe_candidates_A":        candidatesA,
		"lifetime_sites_introduced": lifetimeSites,
	}); err != nil {
		panic(err)
	}
	if !valid {
		fmt.Fprintln(os.Stderr, "NOR288_OWNER_CONTROLS_FAILED")
		os.Exit(3)
	}
}

type edge struct {
	caller, callee *ssa.Function
	site           ssa.CallInstruction
}

type graphIndex struct {
	funcs         map[*ssa.Function]bool
	out, in       map[*ssa.Function][]edge
	sites         map[ssa.CallInstruction]bool
	fallback      map[ssa.CallInstruction]bool
	fallbackSites int
}

// buildGraph indexes the VTA call graph by function. A dynamic call (through
// a function value) inside an analysed function that VTA leaves without any
// callee is resolved conservatively to every analysed function value with an
// identical signature (plain functions, closures, bound-method and interface
// wrappers): an unresolved call never counts as reaching nothing.
func buildGraph(cg *callgraph.Graph, funcs map[*ssa.Function]bool) *graphIndex {
	g := &graphIndex{funcs: map[*ssa.Function]bool{}, out: map[*ssa.Function][]edge{}, in: map[*ssa.Function][]edge{}, sites: map[ssa.CallInstruction]bool{}, fallback: map[ssa.CallInstruction]bool{}}
	add := func(e edge) {
		g.funcs[e.caller], g.funcs[e.callee] = true, true
		g.out[e.caller] = append(g.out[e.caller], e)
		g.in[e.callee] = append(g.in[e.callee], e)
		g.sites[e.site] = true
	}
	for _, node := range cg.Nodes {
		if node.Func != nil {
			g.funcs[node.Func] = true
		}
		for _, e := range node.Out {
			if e.Site != nil && e.Caller.Func != nil && e.Callee.Func != nil {
				add(edge{e.Caller.Func, e.Callee.Func, e.Site})
			}
		}
	}
	for f := range funcs {
		g.funcs[f] = true
	}
	bySignature := map[string][]*ssa.Function{}
	for f := range g.funcs {
		if f.Signature.Recv() == nil {
			key := signatureKey(f.Signature)
			bySignature[key] = append(bySignature[key], f)
		}
	}
	for f := range funcs {
		for _, block := range f.Blocks {
			for _, instr := range block.Instrs {
				site, ok := instr.(ssa.CallInstruction)
				if !ok || g.sites[site] {
					continue
				}
				common := site.Common()
				if common.IsInvoke() || common.StaticCallee() != nil {
					continue
				}
				if _, builtin := common.Value.(*ssa.Builtin); builtin {
					continue
				}
				sig, ok := common.Value.Type().Underlying().(*types.Signature)
				if !ok {
					continue
				}
				targets := bySignature[signatureKey(sig)]
				if len(targets) > 0 {
					g.fallbackSites++
				}
				for _, callee := range targets {
					add(edge{f, callee, site})
					g.fallback[site] = true
				}
			}
		}
	}
	return g
}

// signatureKey identifies a function type by its parameter and result types
// only: a field or parameter declared without names has the same type as a
// function or bound method value whose signature carries names.
func signatureKey(sig *types.Signature) string {
	var b strings.Builder
	b.WriteString("func(")
	for i := 0; i < sig.Params().Len(); i++ {
		b.WriteString(types.TypeString(sig.Params().At(i).Type(), nil) + ",")
	}
	if sig.Variadic() {
		b.WriteString("...")
	}
	b.WriteString(")(")
	for i := 0; i < sig.Results().Len(); i++ {
		b.WriteString(types.TypeString(sig.Results().At(i).Type(), nil) + ",")
	}
	b.WriteString(")")
	return b.String()
}

// sourceFunctions is every function of the program's source: package-level
// functions, every method of every named type (value and pointer receivers),
// their anonymous functions, plus the reachability set, so that dead code is
// covered too.
func sourceFunctions(prog *ssa.Program) map[*ssa.Function]bool {
	funcs := ssautil.AllFunctions(prog)
	var add func(f *ssa.Function)
	add = func(f *ssa.Function) {
		if f == nil {
			return
		}
		funcs[f] = true
		for _, anon := range f.AnonFuncs {
			if !funcs[anon] {
				add(anon)
			}
		}
	}
	for _, pkg := range prog.AllPackages() {
		for _, member := range pkg.Members {
			switch m := member.(type) {
			case *ssa.Function:
				add(m)
			case *ssa.Type:
				named, ok := m.Type().(*types.Named)
				if !ok || named.TypeParams().Len() > 0 {
					continue
				}
				for _, t := range []types.Type{named, types.NewPointer(named)} {
					mset := prog.MethodSets.MethodSet(t)
					for i := 0; i < mset.Len(); i++ {
						add(methodValue(prog, mset.At(i)))
					}
				}
			}
		}
	}
	return funcs
}

// methodValue builds a method's function; a selection SSA cannot build
// (abstract, generic) yields nil.
func methodValue(prog *ssa.Program, sel *types.Selection) (fn *ssa.Function) {
	defer func() {
		if recover() != nil {
			fn = nil
		}
	}()
	return prog.MethodValue(sel)
}

// passesInputName reports whether a call in f passes a string argument taken
// from f's own inputs: a parameter or captured variable, or a value loaded
// from one through field, pointer, index or conversion steps.
func passesInputName(call *ssa.CallCommon, f *ssa.Function) bool {
	if f == nil {
		return false
	}
	own := map[ssa.Value]bool{}
	for _, p := range f.Params {
		own[p] = true
	}
	for _, v := range f.FreeVars {
		own[v] = true
	}
	var fromInput func(v ssa.Value, depth int) bool
	fromInput = func(v ssa.Value, depth int) bool {
		if depth > 8 || v == nil {
			return false
		}
		if own[v] {
			return true
		}
		switch x := v.(type) {
		case *ssa.UnOp:
			return fromInput(x.X, depth+1)
		case *ssa.FieldAddr:
			return fromInput(x.X, depth+1)
		case *ssa.Field:
			return fromInput(x.X, depth+1)
		case *ssa.IndexAddr:
			return fromInput(x.X, depth+1)
		case *ssa.Index:
			return fromInput(x.X, depth+1)
		case *ssa.ChangeType:
			return fromInput(x.X, depth+1)
		case *ssa.Convert:
			return fromInput(x.X, depth+1)
		case *ssa.Phi:
			for _, edge := range x.Edges {
				if fromInput(edge, depth+1) {
					return true
				}
			}
		}
		return false
	}
	for _, arg := range call.Args {
		if basic, ok := arg.Type().Underlying().(*types.Basic); ok && basic.Info()&types.IsString != 0 && fromInput(arg, 0) {
			return true
		}
	}
	return false
}

func isWaitGroup(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "sync" && n.Obj().Name() == "WaitGroup"
}

// sourceHas reports whether a production file of dir under root contains text.
func sourceHas(root, dir, text string) bool {
	files, _ := filepath.Glob(filepath.Join(root, filepath.FromSlash(dir), "*.go"))
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if data, err := os.ReadFile(f); err == nil && bytes.Contains(data, []byte(text)) {
			return true
		}
	}
	return false
}

func methodOpt(pkg *types.Package, typeName, name string) *types.Func {
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		return nil
	}
	m, _, _ := types.LookupFieldOrMethod(types.NewPointer(obj.Type()), true, pkg, name)
	fn, _ := m.(*types.Func)
	return fn
}

func funcName(d *ast.FuncDecl) string {
	if d.Recv != nil && len(d.Recv.List) > 0 {
		t := d.Recv.List[0].Type
		if s, ok := t.(*ast.StarExpr); ok {
			t = s.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return "(" + id.Name + ")." + d.Name.Name
		}
	}
	return d.Name.Name
}

// sitesIn returns the call expressions under n whose call sites are in set.
func sitesIn(n ast.Node, set map[token.Pos]bool) []*ast.CallExpr {
	var out []*ast.CallExpr
	if n == nil {
		return nil
	}
	ast.Inspect(n, func(x ast.Node) bool {
		if call, ok := x.(*ast.CallExpr); ok && set[call.Lparen] {
			out = append(out, call)
		}
		return true
	})
	return out
}

func hasSite(n ast.Node, set map[token.Pos]bool) bool { return len(sitesIn(n, set)) > 0 }

// rootIdent is the leftmost variable of a selector/index/star chain.
func rootIdent(e ast.Expr) string {
	for {
		switch x := e.(type) {
		case *ast.SelectorExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		case *ast.ParenExpr:
			e = x.X
		case *ast.Ident:
			return x.Name
		default:
			return ""
		}
	}
}

// argKeys names what calls take: each argument expression and its root
// variable; with receivers set, also the root of each call's receiver (a
// wrapper that reads the name from its receiver).
func argKeys(calls []*ast.CallExpr, receivers bool) map[string]bool {
	out := map[string]bool{}
	for _, c := range calls {
		for _, a := range c.Args {
			out[types.ExprString(a)] = true
			if r := rootIdent(a); r != "" && r != "ctx" {
				out["root:"+r] = true
			}
		}
		if sel, ok := c.Fun.(*ast.SelectorExpr); ok && receivers {
			if r := rootIdent(sel.X); r != "" {
				out["root:"+r] = true
			}
		}
	}
	return out
}

func returns(b ast.Stmt) bool {
	switch s := b.(type) {
	case *ast.BlockStmt:
		return len(s.List) > 0 && returns(s.List[len(s.List)-1])
	case *ast.ReturnStmt:
		return true
	case *ast.IfStmt:
		return s.Else != nil && returns(s.Body) && returns(s.Else)
	}
	return false
}

// decides reports whether a probe-guarded if in fn changes whether a deletion
// is reached, for a deletion that takes the probe's name argument.
func decides(fn *ast.FuncDecl, probeSite, deleteSite map[token.Pos]bool) bool {
	type env map[string]map[string]bool // name -> probe argument keys of its latest assignment
	clone := func(e env) env {
		out := make(env, len(e))
		for k, v := range e {
			out[k] = v
		}
		return out
	}
	assign := func(stmt ast.Stmt, e env) {
		as, ok := stmt.(*ast.AssignStmt)
		if !ok {
			return
		}
		var keys map[string]bool
		if probes := sitesIn(as, probeSite); len(probes) > 0 {
			keys = argKeys(probes, false)
		}
		for _, lhs := range as.Lhs {
			if id, ok := lhs.(*ast.Ident); ok && id.Name != "_" {
				e[id.Name] = keys
			}
		}
	}
	guardKeys := func(ifs *ast.IfStmt, e env) map[string]bool {
		keys := argKeys(sitesIn(ifs.Cond, probeSite), false)
		scope := clone(e)
		if ifs.Init != nil {
			for k := range argKeys(sitesIn(ifs.Init, probeSite), false) {
				keys[k] = true
			}
			assign(ifs.Init, scope)
		}
		ast.Inspect(ifs.Cond, func(x ast.Node) bool {
			if id, ok := x.(*ast.Ident); ok {
				for k := range scope[id.Name] {
					keys[k] = true
				}
			}
			return true
		})
		return keys
	}
	branchDrops := func(branch ast.Stmt, after bool) bool {
		if branch == nil {
			return after
		}
		return hasSite(branch, deleteSite) || (!returns(branch) && after)
	}
	deleteKeys := argKeys(sitesIn(fn.Body, deleteSite), true)
	found := false
	var walk func(list []ast.Stmt, e env)
	walk = func(list []ast.Stmt, e env) {
		for i, stmt := range list {
			after := false
			for _, later := range list[i+1:] {
				after = after || hasSite(later, deleteSite)
			}
			for link := stmt; link != nil && !found; {
				ifs, ok := link.(*ast.IfStmt)
				if !ok {
					break
				}
				shared := false
				for k := range guardKeys(ifs, e) {
					shared = shared || deleteKeys[k]
				}
				if shared && branchDrops(ifs.Body, after) != branchDrops(ifs.Else, after) {
					found = true
				}
				link = ifs.Else
			}
			inner := clone(e)
			if ifs, ok := stmt.(*ast.IfStmt); ok && ifs.Init != nil {
				assign(ifs.Init, inner)
			}
			ast.Inspect(stmt, func(x ast.Node) bool {
				switch b := x.(type) {
				case *ast.BlockStmt:
					walk(b.List, clone(inner))
					return false
				case *ast.CaseClause:
					walk(b.Body, clone(inner))
					return false
				case *ast.FuncLit:
					walk(b.Body.List, env{})
					return false
				}
				return true
			})
			assign(stmt, e)
		}
	}
	walk(fn.Body.List, env{})
	return found
}
