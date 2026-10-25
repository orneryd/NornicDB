package workcensus

// Author: #1667 implementation lane (Codex). This client joins direct work
// sites to typed observation and call facts. It never approves from a file,
// spelling, reachability, or the mere presence of a context parameter.

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type storageOwnerRelation struct {
	MemberID        string   `json:"member_id"`
	OwnerID         string   `json:"owner_id"`
	OwnerDigest     string   `json:"owner_digest"`
	Callees         []string `json:"callees,omitempty"`
	ContextValues   []string `json:"context_values,omitempty"`
	Checks          []string `json:"checks,omitempty"`
	IterationChecks []string `json:"iteration_checks,omitempty"`
	LoopSites       []string `json:"loop_sites,omitempty"`
	Guard           string   `json:"guard,omitempty"`
	GuardContext    string   `json:"guard_context,omitempty"`
	GuardError      string   `json:"guard_error,omitempty"`
	BoundKnown      bool     `json:"bound_known,omitempty"`
	Bound           int64    `json:"bound,omitempty"`
	ContextProof    []string `json:"context_proof,omitempty"`
}

func TestStorageOwnerClassDiscovery(t *testing.T) {
	if !strings.Contains(flag.Lookup("test.run").Value.String(), "TestStorageOwnerClassDiscovery") {
		t.Skip("explicit mechanism discovery selector required")
	}
	c := storageConfig("../..")
	c.IncludeProgram = true
	inv, err := Extract(c)
	if err != nil {
		t.Fatal(err)
	}
	emit := func(prefix string, value any) {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(prefix + string(raw))
	}
	relations := storageOwnerRelations(inv)
	for _, m := range inv.Members {
		if m.Kind == "owner-call" {
			emit("OWNER_CALL ", m)
			continue
		}
		emit("OWNER_RELATION ", relations[m.ID])
		emit("CENSUS_MEMBER ", m)
	}
	emit("CENSUS_ROOTS ", inv.Roots)
	for _, m := range storageContextBoundaries(inv).Members {
		emit("OWNER_BOUNDARY ", m)
	}
	t.Fatal("class discovery is evidence, never approval; deviations remain OPEN")
}

func storageOwnerRelations(inv Inventory) map[string]storageOwnerRelation {
	contextProofs := storageContextClassFacts(inv)
	byOwner := map[string][]Member{}
	definitions := map[string]Definition{}
	for _, d := range inv.Definitions {
		definitions[d.ID] = d
	}
	defaults := storageNilDefaultBindings(inv, definitions)
	for _, m := range inv.Members {
		if m.Kind == "owner-call" {
			byOwner[m.OwnerID] = append(byOwner[m.OwnerID], m)
		}
	}
	out := map[string]storageOwnerRelation{}
	for _, m := range inv.Members {
		if m.Kind == "owner-call" {
			continue
		}
		r := storageOwnerRelation{MemberID: m.ID, OwnerID: m.OwnerID, OwnerDigest: m.OwnerDigest, Callees: m.Callees, BoundKnown: m.BoundKnown, Bound: m.Bound}
		r.ContextProof = contextProofs[storagePhysicalSpan(m)]
		for _, v := range definitions[m.OwnerID].Values {
			if v.Type == "context.Context" {
				r.ContextValues = append(r.ContextValues, v.Reference)
			}
		}
		// Exact selected AST ancestry identifies loop ownership; no private
		// parser/walk and no source-text pattern can manufacture coverage.
		var loops []ast.Node
		if m.Syntax != nil {
			for _, n := range m.Syntax.Ancestors {
				switch n.(type) {
				case *ast.ForStmt, *ast.RangeStmt:
					loops = append(loops, n)
				}
			}
			if m.Node == "for" || m.Node == "range" {
				loops = append(loops, m.Syntax.Node)
			}
			for _, n := range loops {
				pos := m.Syntax.FileSet.PositionFor(n.Pos(), false)
				r.LoopSites = append(r.LoopSites, fmt.Sprintf("%s:%d", m.File, pos.Line))
			}
		}
		for _, call := range byOwner[m.OwnerID] {
			if !contextErrCall(call) {
				continue
			}
			r.Checks = append(r.Checks, call.SiteID)
			// This is an observation-location fact, not a dominance claim.
			// A class proof must additionally check its nil/error branch.
			for _, loop := range loops {
				if call.Syntax.Node.Pos() > loop.Pos() && call.Syntax.Node.End() < loop.End() {
					r.IterationChecks = append(r.IterationChecks, call.SiteID)
					break
				}
			}
		}
		if len(loops) != 0 {
			// Only the immediately containing iteration can own this work.
			// A checkpoint in an outer loop never approves an inner loop.
			nearest := loops[len(loops)-1]
			if witness, context, failure := storageIterationGuard(nearest, byOwner[m.OwnerID]); witness != "" {
				preserved := storageContextSource(m.OwnerID, context, definitions) || storageNilPreservedParameter(m.OwnerID, context, definitions, defaults)
				if (m.Node == "range" || m.Node == "for" || storageAfterGuard(nearest, m.Syntax.Node)) && preserved {
					r.Guard, r.GuardContext, r.GuardError = witness, context, failure
				}
			}
		}
		sort.Strings(r.ContextValues)
		out[m.ID] = r
	}
	return out
}

func storageAfterGuard(loop, node ast.Node) bool {
	var body *ast.BlockStmt
	switch n := loop.(type) {
	case *ast.ForStmt:
		body = n.Body
	case *ast.RangeStmt:
		body = n.Body
	default:
		return false
	}
	return len(body.List) > 0 && node.Pos() >= body.List[0].End() && node.End() < body.End()
}

func validateStorageIterationClass(actual, approved Inventory) []string {
	listed := map[string]Member{}
	for _, m := range approved.Members {
		if m.Class == "checked preparation iteration" {
			listed[m.ID] = m
		}
	}
	selected, reviewed := Inventory{Roots: actual.Roots}, Inventory{Roots: approved.Roots}
	for _, m := range actual.Members {
		a, found := listed[m.ID]
		if !found {
			continue
		}
		eligibility := "OPEN"
		if len(m.Facts["iteration guard"]) == 3 {
			eligibility = "checked preparation iteration"
		}
		m.Facts["class eligibility"] = []string{eligibility}
		selected.Members = append(selected.Members, m)
		reviewed.Members = append(reviewed.Members, a)
	}
	return Validate(selected, reviewed, Policy{Classes: []string{"checked preparation iteration"}, ClassKinds: map[string]string{"checked preparation iteration": "review"}, ClassFact: "class eligibility"})
}

// This is deliberately a partial qualification, not the complete preparation
// gate. The latter is TestStorageWorkCensus and still rejects every OPEN row.
func TestStorageOwnerClassQualifiedRows(t *testing.T) {
	if !strings.Contains(flag.Lookup("test.run").Value.String(), "TestStorageOwnerClassQualifiedRows") {
		t.Skip("partial qualification is explicit; TestStorageWorkCensus owns the complete gate")
	}
	inv, err := Extract(storageConfig("../.."))
	if err != nil {
		t.Fatal(err)
	}
	work, envelope := populations(inv)
	approvedWork := approved(t, "storage.json")
	previous := map[string]bool{}
	for _, m := range approvedWork.Members {
		if m.Class != "" {
			previous[m.ID] = true
		}
	}
	actualSubset, approvedSubset := Inventory{Roots: work.Roots, Definitions: work.Definitions}, Inventory{Roots: approvedWork.Roots}
	for _, m := range work.Members {
		if len(m.Facts["iteration guard"]) == 3 || m.BoundKnown && m.Bound >= 0 && m.Bound <= 32 || previous[m.ID] {
			actualSubset.Members = append(actualSubset.Members, m)
		}
	}
	for _, m := range approvedWork.Members {
		if m.Class != "" {
			approvedSubset.Members = append(approvedSubset.Members, m)
		}
	}
	for _, failure := range Validate(actualSubset, approvedSubset, storagePolicy()) {
		t.Error(failure)
	}
	for _, failure := range validateStorageIterationClass(actualSubset, approvedSubset) {
		t.Error(failure)
	}
	for _, failure := range validateStorageContextClasses(inv, approvedSubset) {
		t.Error(failure)
	}
	for _, failure := range Validate(envelope, approved(t, "envelope.json"), envelopePolicy()) {
		t.Error(failure)
	}
	boundaries := storageContextBoundaries(inv)
	boundaryPolicy := storageContextBoundaryPolicy()
	boundaryPolicy.RequireReviewOwner = false
	// Membership can be exact while every semantic decision remains OPEN.
	// Do not manufacture an approved class just to get a snapshot positive.
	boundaryFailures := Validate(boundaries, approved(t, "storage-context-boundaries.json"), boundaryPolicy)
	openBoundaries := 0
	for _, m := range approved(t, "storage-context-boundaries.json").Members {
		if m.Class == "" {
			openBoundaries++
		}
	}
	if len(boundaryFailures) != openBoundaries {
		t.Errorf("boundary snapshot: got %d failures for %d OPEN rows", len(boundaryFailures), openBoundaries)
	}
	for _, failure := range boundaryFailures {
		if !strings.HasPrefix(failure, "UNCLASSIFIED ") {
			t.Error(failure)
		}
	}
	for _, failure := range validateStorageContextClasses(inv, approved(t, "storage-context-boundaries.json")) {
		t.Error(failure)
	}
	t.Logf("context/phase boundary population %d, OPEN %d", len(boundaries.Members), openBoundaries)
	t.Logf("qualified work subset %d / %d; OPEN work %d; envelope %d; complete gate remains TestStorageWorkCensus", len(actualSubset.Members), len(work.Members), len(work.Members)-len(actualSubset.Members), len(envelope.Members))
}

// Context boundaries are direct mechanism users, not every descendant below
// them. Scope marks potential availability, never a proof that a call is wrong:
// nil defaults, accepted completion and capability fallbacks need class review.
func storageContextBoundaries(inv Inventory) Inventory {
	out := Inventory{Roots: inv.Roots, Definitions: inv.Definitions}
	proofs, handoffTargets := storageContextClassFacts(inv), storageImageTargets()
	for target := range storageDirectionalTargets() {
		handoffTargets[target] = true
	}
	for target := range storageNamespaceTargets() {
		handoffTargets[target] = true
	}
	for target := range storageSchemaTargets() {
		handoffTargets[target] = true
	}
	resets := map[string][]Member{}
	for _, m := range inv.Members {
		if m.Kind == "context-boundary" && m.Scope == "" {
			resets[m.OwnerSymbol] = append(resets[m.OwnerSymbol], m)
		}
	}
	for _, m := range inv.Members {
		var targets, bodies []string
		switch {
		case m.Kind == "context-boundary" && m.Scope != "":
			targets = append(targets, m.Callees...)
		case m.Kind == "owner-call" && m.Scope != "":
			for _, callee := range m.Callees {
				if handoffTargets[callee] {
					targets = append(targets, callee)
				}
				for _, reset := range resets[callee] {
					targets = append(targets, callee)
					bodies = append(bodies, reset.OwnerDigest)
				}
			}
		default:
			continue
		}
		if len(targets) == 0 {
			continue
		}
		m.Facts = map[string][]string{
			"operation": {m.Node, strings.Split(m.Code, "\n")[0]},
			"context":   {m.Scope}, "reset targets": targets,
			"reset bodies": bodies, "arguments": m.ArgReferences,
		}
		if proof := proofs[storagePhysicalSpan(m)]; len(proof) > 0 {
			m.Facts["context proof"] = proof
		}
		out.Members = append(out.Members, m)
	}
	return out
}

func storageContextBoundaryPolicy() Policy {
	return Policy{Classes: []string{"consumes owner", "distinct responsibility", "fixed", imageHandoffClass, nilContextClass, schemaHandoffClass, compatibilityContextClass, namespaceHandoffClass, directionalHandoffClass}, ClassKinds: map[string]string{"consumes owner": "review", "distinct responsibility": "review", "fixed": "review", imageHandoffClass: "review", nilContextClass: "review", schemaHandoffClass: "review", compatibilityContextClass: "review", namespaceHandoffClass: "review", directionalHandoffClass: "review"}, RequireOwnerDigest: true, RequireReviewOwner: true, Owners: []string{"#1667"}, FactKeys: []string{"operation", "context", "reset targets", "reset bodies", "arguments", "context proof"}}
}

func TestStorageContextBoundaryPopulationControl(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module boundary.fixture\n\ngo 1.26\n")
	write("entry.go", "package entry\nimport \"context\"\nfunc Execute(ctx context.Context) {}\n")
	c := Config{Dir: dir, Patterns: []string{"./..."}, Roots: []Root{{Package: "boundary.fixture", Name: "Execute", Required: true}}, Selectors: []Selector{{Kind: "context-boundary", Node: "call", Packages: []string{"context"}, Names: []string{"Background", "TODO"}}, {Kind: "owner-call", Node: "call"}}}
	baseline := storageContextBoundaries(mustExtract(t, c))
	if len(baseline.Members) != 0 {
		t.Fatal("unexpected baseline boundaries", baseline.Members)
	}
	write("plugins/foreign/reader.go", `package foreign
import "context"
func New(ctx context.Context) { legacy() }
func legacy() { _ = context.Background() }
`)
	write("entry.go", "package entry\nimport (\"context\"; foreign \"boundary.fixture/plugins/foreign\")\nfunc Execute(ctx context.Context) {foreign.New(ctx)}\n")
	planted := storageContextBoundaries(mustExtract(t, c))
	if len(planted.Members) != 1 || planted.Members[0].File != "plugins/foreign/reader.go" {
		t.Fatal("outside-package context loss was not enumerated", planted.Members)
	}
	failures := Validate(planted, baseline, storageContextBoundaryPolicy())
	if len(failures) != 1 || failures[0] != "UNLISTED "+planted.Members[0].ID {
		t.Fatal("same production boundary gate accepted planted member", failures)
	}
}

func storageContextSource(owner, reference string, definitions map[string]Definition) bool {
	// A nested closure must not rebind the selected ancestor's context.
	for _, d := range definitions {
		for _, unsafe := range append(append([]string(nil), d.Writes...), d.Escapes...) {
			if unsafe == reference {
				return false
			}
		}
	}
	for owner != "" {
		d := definitions[owner]
		for _, v := range d.Values {
			if v.Role == "parameter" && v.Type == "context.Context" && v.Reference == reference {
				return true
			}
			if v.Role == "receiver" && strings.HasPrefix(reference, v.Reference+".") {
				return true
			}
		}
		owner = d.ParentID
	}
	return false
}

func TestStorageIterationOwnerProofControls(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module owner.fixture\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "owner.go")
	raw := []byte(`package owner
import "context"
var sink int
func none() error { return nil }
func observed(ctx context.Context, values []int) error {
 for _, v := range values { if err := ctx.Err(); err != nil { return err }; sink += v }; return nil
}
func ignored(ctx context.Context, values []int) error {
 for _, v := range values { ctx.Err(); sink += v }; return nil
}
func borrowed(ctx context.Context, values []int) error {
 for range values { if err := ctx.Err(); err != nil { return err }; for _, v := range values { sink += v } }; return nil
}
func substituted(ctx context.Context, values []int) error {
 other := context.Background()
 for _, v := range values { if err := other.Err(); err != nil { return err }; sink += v }; return nil
}
func defaulted(ctx context.Context, values []int) error {
 if ctx == nil {ctx = context.Background()}
 for _, v := range values {if err := ctx.Err();err != nil{return err};sink += v};return nil
}
func resetAfterDefault(ctx context.Context, values []int) error {
 if ctx == nil {ctx = context.Background()};ctx = context.Background()
 for _, v := range values {if err := ctx.Err();err != nil{return err};sink += v};return nil
}
func escapedAfterDefault(ctx context.Context, values []int) error {
 if ctx == nil {ctx = context.Background()}; saved := &ctx; _ = saved
 for _, v := range values {if err := ctx.Err();err != nil{return err};sink += v};return nil
}
`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	c := Config{Dir: dir, Patterns: []string{"./..."}, SourceOnly: true, IncludeSyntax: true, Selectors: []Selector{{Kind: "range", Node: "range", Reachability: "all"}, {Kind: "owner-call", Node: "call", Reachability: "all"}, {Kind: "context-boundary", Node: "call", Packages: []string{"context"}, Names: []string{"Background", "TODO"}, Reachability: "all"}}}
	actual := mustExtract(t, c)
	classify := func(inv Inventory) (Inventory, Member) {
		relations := storageOwnerRelations(inv)
		var observation Member
		out := Inventory{Roots: inv.Roots}
		for _, m := range inv.Members {
			if m.Kind != "range" {
				continue
			}
			r := relations[m.ID]
			class := "OPEN"
			m.Facts = map[string][]string{}
			if r.Guard != "" {
				class = "checked preparation iteration"
				m.Facts["iteration guard"] = []string{r.Guard, r.GuardContext, r.GuardError}
				for _, call := range inv.Members {
					if call.SiteID == r.Guard && strings.HasSuffix(m.Owner, ".observed") {
						observation = call
					}
				}
			}
			m.Facts["owner class"] = []string{class}
			out.Members = append(out.Members, m)
		}
		return out, observation
	}

	actual, observation := classify(actual)
	for _, m := range actual.Members {
		want := "OPEN"
		if strings.HasSuffix(m.Owner, ".observed") || strings.HasSuffix(m.Owner, ".defaulted") || strings.HasSuffix(m.Owner, ".borrowed") && strings.Contains(m.Code, "for range values") {
			want = "checked preparation iteration"
		}
		if m.Facts["owner class"][0] != want {
			t.Fatalf("%s: got %v, want %s", m.ID, m.Facts, want)
		}
	}
	if observation.ID == "" {
		t.Fatal("no actual observation selected")
	}
	policy := Policy{Classes: []string{"checked preparation iteration", "OPEN"}, ClassKinds: map[string]string{"checked preparation iteration": "review", "OPEN": "review"}, ClassFact: "owner class"}
	approved := reviewed(actual)
	for i := range approved.Members {
		approved.Members[i].Class = actual.Members[i].Facts["owner class"][0]
	}
	if failures := Validate(actual, approved, policy); len(failures) != 0 {
		t.Fatal(failures)
	}
	if failures := validateStorageIterationClass(actual, approved); len(failures) != 0 {
		t.Fatal(failures)
	}

	mutant, err := ExtractWithControl(c, SourceControl{ID: "remove-check", MemberID: observation.ID, Replacement: "none()"})
	if err != nil {
		t.Fatal(err)
	}
	mutant, _ = classify(mutant)
	// Refresh ordinary source hashes to isolate the class contradiction.
	for i, m := range mutant.Members {
		approved.Members[i].Digest, approved.Members[i].OwnerDigest = m.Digest, m.OwnerDigest
	}
	if failures := validateStorageIterationClass(mutant, approved); !strings.Contains(strings.Join(failures, "\n"), "CLASS FACT CONTRADICTION") {
		t.Fatal("ordinary storage class gate accepted removal", failures)
	}

	if failures := Validate(mutant, approved, policy); !strings.Contains(strings.Join(failures, "\n"), "CLASS FACT CONTRADICTION") {
		t.Fatal("removed observation did not invalidate class", failures)
	}
}

// storageIterationGuard recognizes one reviewed owner mechanism, not every
// possible cancellation idiom: the first statement of this very loop checks
// its context.Err and returns that exact error on failure. Unknown forms stay
// deviations. The typed receiver and error object prevent same-spelled locals
// or ignored observations from supplying a witness.
func storageIterationGuard(loop ast.Node, calls []Member) (site, context, failure string) {
	var body *ast.BlockStmt
	switch n := loop.(type) {
	case *ast.ForStmt:
		body = n.Body
	case *ast.RangeStmt:
		body = n.Body
	default:
		return
	}
	if len(body.List) == 0 {
		return
	}
	guard, ok := body.List[0].(*ast.IfStmt)
	if !ok || guard.Else != nil || len(guard.Body.List) != 1 {
		return
	}
	init, ok := guard.Init.(*ast.AssignStmt)
	if !ok || init.Tok != token.DEFINE || len(init.Lhs) != 1 || len(init.Rhs) != 1 {
		return
	}
	name, ok := init.Lhs[0].(*ast.Ident)
	if !ok {
		return
	}
	observation, ok := init.Rhs[0].(*ast.CallExpr)
	if !ok {
		return
	}
	condition, ok := guard.Cond.(*ast.BinaryExpr)
	if !ok || condition.Op != token.NEQ {
		return
	}
	errName, ok := condition.X.(*ast.Ident)
	if !ok {
		return
	}
	nilName, ok := condition.Y.(*ast.Ident)
	if !ok || nilName.Name != "nil" {
		return
	}
	ret, ok := guard.Body.List[0].(*ast.ReturnStmt)
	if !ok {
		return
	}
	for _, call := range calls {
		if call.Syntax == nil || call.Syntax.Node != observation || !contextErrCall(call) || call.ReceiverReference == "" {
			continue
		}
		info := call.Syntax.Info
		binding := info.ObjectOf(name)
		if binding == nil || info.ObjectOf(errName) != binding || info.ObjectOf(nilName) != types.Universe.Lookup("nil") {
			continue
		}
		for _, result := range ret.Results {
			if id, ok := result.(*ast.Ident); ok && info.ObjectOf(id) == binding {
				return call.SiteID, call.ReceiverReference, binding.Name()
			}
		}
	}
	return
}

func contextErrCall(m Member) bool {
	if m.Syntax == nil || m.Syntax.Info == nil {
		return false
	}
	call, ok := m.Syntax.Node.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Err" {
		return false
	}
	selection := m.Syntax.Info.Selections[sel]
	if selection == nil {
		return false
	}
	method, ok := selection.Obj().(*types.Func)
	return ok && method.Pkg() != nil && method.Pkg().Path() == "context" && typeString(selection.Recv()) == "context.Context"
}
