package workcensus

// Author: #1667 implementation lane (Codex). Client semantics over the shared
// engine's selected typed nodes, binding facts and exact call targets.

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const imageHandoffClass = "checked contextual image handoff"
const nilContextClass = "nil-only compatibility context"
const schemaHandoffClass = "checked contextual schema handoff"
const namespaceHandoffClass = "checked contextual namespace preparation handoff"
const directionalHandoffClass = "checked contextual directional read handoff"

func storageImageTargets() map[string]bool {
	out := map[string]bool{}
	for _, name := range []string{"copyNodeWithContext", "copyNodeForCallerWithContext", "copyNodeWithoutEmbeddingsWithContext", "copyNodeProjectedWithoutEmbeddingsWithContext", "copyEdgeWithContext", "projectCachedNodeForReadWithContext"} {
		out[module+"pkg/storage."+name] = true
	}
	return out
}

func storageSchemaTargets() map[string]bool {
	return map[string]bool{
		"(*" + module + "pkg/storage.SchemaManager).GetConstraintsForLabelsWithContext":             true,
		"(*" + module + "pkg/storage.SchemaManager).GetPropertyTypeConstraintsForLabelsWithContext": true,
	}
}

func storageNamespaceTargets() map[string]bool {
	return map[string]bool{
		"(*" + module + "pkg/storage.NamespacedEngine).NodeWriteInputWithContext":         true,
		"(*" + module + "pkg/storage.NamespacedEngine).EdgeWriteInputWithContext":         true,
		"(*" + module + "pkg/storage.SchemaManager).RegisterTokensWithContext":            true,
		"(*" + module + "pkg/storage.SchemaManager).exportDefinitionLockedWithContext":    true,
		"(*" + module + "pkg/storage.SchemaManager).exportLookupIndexesLockedWithContext": true,
		module + "pkg/storage.appendTokenNamesWithContext":                                true,
		module + "pkg/storage.cloneConstraintContractWithContext":                         true,
		module + "pkg/storage.cloneConstraintContractEntryWithContext":                    true,
		module + "pkg/storage.copySliceWithContext":                                       true,
		module + "pkg/storage.sortIndexCandidatesWithContext":                             true,
	}
}

func storageDirectionalTargets() map[string]bool {
	out := map[string]bool{module + "pkg/storage.readAdjacencyWithContext": true, "(*" + module + "pkg/storage.NamespacedEngine).toUserEdgesInNamespaceWithContext": true}
	for _, name := range []string{"copyEdgeWithContext", "reverseSliceWithContext", "snapshotEdgeBytesWithContext", "snapshotLabelPrefixValueBytesWithContext"} {
		out[module+"pkg/storage."+name] = true
	}
	for _, name := range []string{"getCommittedAdjacentEdgesLockedWithContext", "readCachedSnapshotAdjacentEdgesLockedWithContext", "cacheSnapshotEdgeByIDLockedWithContext"} {
		out["(*"+module+"pkg/storage.BadgerTransaction)."+name] = true
	}
	for _, name := range []string{"collectVisibleAdjacencyEdgeIDsInTxnWithContext", "readAdjacentEdgesAtWithViewWithContext"} {
		out["(*"+module+"pkg/storage.BadgerEngine)."+name] = true
	}
	for _, name := range []string{"GetOutgoingEdgesWithContext", "GetIncomingEdgesWithContext", "GetAdjacentEdgesWithContext"} {
		out[module+"pkg/storage."+name] = true
		for _, owner := range []string{"pkg/storage.BadgerEngine", "pkg/storage.BadgerTransaction", "pkg/storage.NamespacedEngine", "pkg/storage.WALEngine", "pkg/multidb.sizeTrackingEngine", "pkg/replication.ReplicatedEngine", "pkg/cypher.graphIdentityStorage", "pkg/cypher.transactionStorageWrapper"} {
			out["(*"+module+owner+")."+name] = true
		}
	}
	return out
}

func storageDefinitions(inv Inventory) map[string]Definition {
	out := map[string]Definition{}
	for _, d := range inv.Definitions {
		out[d.ID] = d
	}
	return out
}

func storageContextHandoff(m Member, definitions map[string]Definition, targets map[string]bool, defaults ...map[string]int) []string {
	if m.Syntax == nil || m.Syntax.Info == nil || len(m.Callees) == 0 || m.UnresolvedTargets || len(m.ArgReferences) == 0 {
		return nil
	}
	for _, target := range m.Callees {
		if !targets[target] {
			return nil
		}
	}
	ctx := m.ArgReferences[0]
	if !storageContextSource(m.OwnerID, ctx, definitions) && (len(defaults) == 0 || !storageNilPreservedParameter(m.OwnerID, ctx, definitions, defaults[0])) {
		return nil
	}
	call, ok := m.Syntax.Node.(*ast.CallExpr)
	if !ok {
		return nil
	}
	if failure := storageCallFailure(m.Syntax, call); failure != "" {
		return []string{ctx, failure}
	}
	return nil
}

// A tuple is returned unchanged, or the call's actual error binding is checked
// by the next statement and returned unchanged. Unknown flow stays a deviation.
func storageCallFailure(s *GoSyntax, call *ast.CallExpr) string {
	if len(s.Ancestors) == 0 {
		return ""
	}
	parent := s.Ancestors[len(s.Ancestors)-1]
	if ret, ok := parent.(*ast.ReturnStmt); ok && len(ret.Results) == 1 && ret.Results[0] == call {
		return "returned call tuple"
	}
	assignment, ok := parent.(*ast.AssignStmt)
	if !ok || len(assignment.Rhs) != 1 || assignment.Rhs[0] != call || len(assignment.Lhs) < 1 {
		return ""
	}
	errorName, ok := assignment.Lhs[len(assignment.Lhs)-1].(*ast.Ident)
	if !ok || errorName.Name == "_" {
		return ""
	}
	binding := s.Info.ObjectOf(errorName)
	if binding == nil || !types.Identical(binding.Type(), types.Universe.Lookup("error").Type()) {
		return ""
	}
	if len(s.Ancestors) > 1 {
		if guard, ok := s.Ancestors[len(s.Ancestors)-2].(*ast.IfStmt); ok && guard.Init == assignment {
			if storageCheckedError(s.Info, guard, binding) {
				return "checked error " + errorName.Name
			}
			return ""
		}
	}
	for i := len(s.Ancestors) - 2; i >= 0; i-- {
		block, ok := s.Ancestors[i].(*ast.BlockStmt)
		if !ok {
			continue
		}
		var container ast.Stmt = assignment
		// The selected branch contains exactly the assignment. On its path,
		// control joins the enclosing if/switch without another error write.
		for j := len(s.Ancestors) - 2; j > i; j-- {
			switch node := s.Ancestors[j].(type) {
			case *ast.BlockStmt:
				if _, caseBody := container.(*ast.CaseClause); !caseBody && (len(node.List) != 1 || node.List[0] != container) {
					return ""
				}
			case *ast.CaseClause:
				if len(node.Body) != 1 || node.Body[0] != container {
					return ""
				}
				container = node
			case *ast.IfStmt:
				container = node
			case *ast.SwitchStmt:
				container = node
			default:
				return ""
			}
		}
		if _, alternative := container.(*ast.CaseClause); alternative {
			continue // another case is an alternative, not this path's successor
		}
		for n, stmt := range block.List {
			if stmt != container || n+1 >= len(block.List) {
				continue
			}
			if ret, ok := block.List[n+1].(*ast.ReturnStmt); ok {
				for _, result := range ret.Results {
					if id, ok := result.(*ast.Ident); ok && s.Info.ObjectOf(id) == binding {
						return "returned error " + errorName.Name
					}
				}
				return ""
			}
			guard, ok := block.List[n+1].(*ast.IfStmt)
			if ok && guard.Init == nil && storageCheckedError(s.Info, guard, binding) {
				return "checked error " + errorName.Name
			}
			return ""
		}
	}
	return ""
}

func storageCheckedError(info *types.Info, guard *ast.IfStmt, binding types.Object) bool {
	if guard.Else != nil || len(guard.Body.List) != 1 {
		return false
	}
	condition, ok := guard.Cond.(*ast.BinaryExpr)
	if !ok || condition.Op != token.NEQ {
		return false
	}
	left, lok := condition.X.(*ast.Ident)
	right, rok := condition.Y.(*ast.Ident)
	if !lok || !rok || info.ObjectOf(left) != binding || info.ObjectOf(right) != types.Universe.Lookup("nil") {
		return false
	}
	ret, ok := guard.Body.List[0].(*ast.ReturnStmt)
	if !ok {
		return false
	}
	for _, result := range ret.Results {
		if id, ok := result.(*ast.Ident); ok && info.ObjectOf(id) == binding {
			return true
		}
	}
	return false
}

// Definitions retain every write occurrence, including writes in closures.
// Credit a rebound parameter only when every write is the exact nil-only
// assignment above and its address never escapes. Extra writes remain OPEN.
func storageNilPreservedParameter(owner, reference string, definitions map[string]Definition, defaults map[string]int) bool {
	found := false
	for owner != "" {
		d := definitions[owner]
		for _, v := range d.Values {
			if v.Reference == reference && v.Type == "context.Context" && v.Role == "parameter" {
				found = true
			}
		}
		owner = d.ParentID
	}
	if !found {
		return false
	}
	writes := 0
	for _, d := range definitions {
		for _, ref := range d.Writes {
			if ref == reference {
				writes++
			}
		}
		if contains(d.Escapes, reference) {
			return false
		}
	}
	return writes > 0 && defaults[reference] == writes
}

func storageNilDefaultBindings(inv Inventory, definitions map[string]Definition) map[string]int {
	out := map[string]int{}
	for _, m := range inv.Members {
		if m.Kind != "context-boundary" || !storageNilContextDefault(m) {
			continue
		}
		assignment, ok := m.Syntax.Ancestors[len(m.Syntax.Ancestors)-1].(*ast.AssignStmt)
		if !ok {
			continue // Returned defaults do not write or rebind a parameter.
		}
		id := assignment.Lhs[0].(*ast.Ident)
		for owner := m.OwnerID; owner != ""; {
			d := definitions[owner]
			var candidates []ValueDefinition
			for _, v := range d.Values {
				if v.Name == id.Name {
					candidates = append(candidates, v)
				}
			}
			if len(candidates) > 0 {
				if len(candidates) == 1 && candidates[0].Role == "parameter" {
					out[candidates[0].Reference]++
				}
				break
			}
			owner = d.ParentID
		}
	}
	return out
}

// This default cannot overwrite a non-nil caller context: the exact binding
// assigned Background is guarded by comparison with the universe nil object.
func storageNilContextDefault(m Member) bool {
	if m.Syntax == nil || m.Syntax.Info == nil || len(m.Callees) != 1 || m.Callees[0] != "context.Background" && m.Callees[0] != "context.TODO" {
		return false
	}
	if storageNilContextReturn(m) {
		return true
	}
	s := m.Syntax
	if len(s.Ancestors) < 3 {
		return false
	}
	assignment, ok := s.Ancestors[len(s.Ancestors)-1].(*ast.AssignStmt)
	if !ok || assignment.Tok != token.ASSIGN || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 || assignment.Rhs[0] != s.Node {
		return false
	}
	id, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok || typeString(s.Info.TypeOf(id)) != "context.Context" {
		return false
	}
	block, ok := s.Ancestors[len(s.Ancestors)-2].(*ast.BlockStmt)
	if !ok || len(block.List) != 1 || block.List[0] != assignment {
		return false
	}
	guard, ok := s.Ancestors[len(s.Ancestors)-3].(*ast.IfStmt)
	if !ok || guard.Init != nil || guard.Else != nil || guard.Body != block {
		return false
	}
	condition, ok := guard.Cond.(*ast.BinaryExpr)
	if !ok || condition.Op != token.EQL {
		return false
	}
	left, lok := condition.X.(*ast.Ident)
	right, rok := condition.Y.(*ast.Ident)
	return lok && rok && s.Info.ObjectOf(left) == s.Info.ObjectOf(id) && s.Info.ObjectOf(right) == types.Universe.Lookup("nil")
}

// Recognize only the complete bounded getter, not a guard somewhere in an
// owner. Both branches must return the same typed context identity or the nil
// fallback; extra preparation, writes, calls and alternate returns remain OPEN.
func storageNilContextReturn(m Member) bool {
	s := m.Syntax
	if s == nil || s.Info == nil {
		return false
	}
	var body *ast.BlockStmt
	switch owner := s.Owner.(type) {
	case *ast.FuncDecl:
		body = owner.Body
	case *ast.FuncLit:
		body = owner.Body
	}
	if body == nil || len(body.List) != 2 {
		return false
	}
	guard, ok := body.List[0].(*ast.IfStmt)
	if !ok || guard.Init != nil || guard.Else != nil || len(guard.Body.List) != 1 {
		return false
	}
	condition, ok := guard.Cond.(*ast.BinaryExpr)
	if !ok || (condition.Op != token.EQL && condition.Op != token.NEQ) {
		return false
	}
	nilName, ok := condition.Y.(*ast.Ident)
	if !ok || s.Info.ObjectOf(nilName) != types.Universe.Lookup("nil") || typeString(s.Info.TypeOf(condition.X)) != "context.Context" {
		return false
	}
	branch, ok := guard.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(branch.Results) != 1 {
		return false
	}
	tail, ok := body.List[1].(*ast.ReturnStmt)
	if !ok || len(tail.Results) != 1 {
		return false
	}
	preserved, fallback := tail.Results[0], branch.Results[0]
	if condition.Op == token.NEQ {
		preserved, fallback = branch.Results[0], tail.Results[0]
	}
	return fallback == s.Node && storageSameContextBinding(s.Info, condition.X, preserved)
}

func storageSameContextBinding(info *types.Info, left, right ast.Expr) bool {
	switch a := left.(type) {
	case *ast.Ident:
		b, ok := right.(*ast.Ident)
		return ok && info.ObjectOf(a) != nil && info.ObjectOf(a) == info.ObjectOf(b)
	case *ast.SelectorExpr:
		b, ok := right.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		x, xok := a.X.(*ast.Ident)
		y, yok := b.X.(*ast.Ident)
		selection := info.Selections[a]
		return xok && yok && selection != nil && selection.Kind() == types.FieldVal && info.ObjectOf(x) != nil && info.ObjectOf(x) == info.ObjectOf(y) && info.ObjectOf(a.Sel) == info.ObjectOf(b.Sel)
	}
	return false
}

func storageContextClassFacts(inv Inventory) map[string][]string {
	definitions, targets := storageDefinitions(inv), storageImageTargets()
	schemaTargets := storageSchemaTargets()
	namespaceTargets := storageNamespaceTargets()
	directionalTargets := storageDirectionalTargets()
	calleeBodies := map[string]string{}
	for _, d := range inv.Definitions {
		calleeBodies[d.Symbol] = d.Digest
	}
	defaults := storageNilDefaultBindings(inv, definitions)
	out := map[string][]string{}
	for _, m := range inv.Members {
		if m.Kind == "owner-call" {
			if proof := storageContextHandoff(m, definitions, targets, defaults); proof != nil {
				out[storagePhysicalSpan(m)] = append([]string{imageHandoffClass}, proof...)
			}
			if proof := storageContextHandoff(m, definitions, schemaTargets, defaults); proof != nil {
				for _, target := range m.Callees {
					proof = append(proof, "callee body "+target+" "+calleeBodies[target])
				}
				out[storagePhysicalSpan(m)] = append([]string{schemaHandoffClass}, proof...)
			}
			if proof := storageContextHandoff(m, definitions, namespaceTargets, defaults); proof != nil {
				for _, target := range m.Callees {
					proof = append(proof, "callee body "+target+" "+calleeBodies[target])
				}
				out[storagePhysicalSpan(m)] = append([]string{namespaceHandoffClass}, proof...)
			}
		}
		if m.Node == "call" {
			if proof := storageContextHandoff(m, definitions, directionalTargets, defaults); proof != nil {
				for _, target := range m.Callees {
					proof = append(proof, "callee body "+target+" "+calleeBodies[target])
				}
				out[storagePhysicalSpan(m)] = append([]string{directionalHandoffClass}, proof...)
			}
		}
		if m.Kind == "context-boundary" && storageNilContextDefault(m) {
			out[storagePhysicalSpan(m)] = []string{nilContextClass, "exact binding == nil; only statement assigns the fallback"}
			if storageNilContextReturn(m) {
				out[storagePhysicalSpan(m)] = []string{nilContextClass, "complete typed nil-coalescing return; non-nil binding is returned unchanged"}
			}
		}
		if proof := storageCompatibilitySource(m, inv); len(proof) > 0 {
			out[storagePhysicalSpan(m)] = append([]string{compatibilityContextClass}, proof...)
		}
	}
	return out
}

// Join overlapping selectors on one engine-owned physical span. This is not
// a replacement entity identity: persisted rows retain the engine's ID/SiteID.
func storagePhysicalSpan(m Member) string {
	return fmt.Sprintf("%s:%d:%d", m.File, m.Offset, m.EndOffset)
}

func validateStorageContextClasses(actual, approved Inventory) []string {
	proofs := storageContextClassFacts(actual)
	selected, reviewed := Inventory{Roots: actual.Roots, Definitions: actual.Definitions}, Inventory{Roots: approved.Roots}
	for _, a := range approved.Members {
		if a.Class != imageHandoffClass && a.Class != nilContextClass && a.Class != schemaHandoffClass && a.Class != compatibilityContextClass && a.Class != namespaceHandoffClass && a.Class != directionalHandoffClass {
			continue
		}
		reviewed.Members = append(reviewed.Members, a)
		for _, m := range actual.Members {
			if m.ID != a.ID {
				continue
			}
			m.Facts = map[string][]string{"class eligibility": nil}
			if proof := proofs[storagePhysicalSpan(m)]; len(proof) > 0 {
				m.Facts["class eligibility"] = proof[:1]
			}
			selected.Members = append(selected.Members, m)
		}
	}
	return Validate(selected, reviewed, Policy{Classes: []string{imageHandoffClass, nilContextClass, schemaHandoffClass, compatibilityContextClass, namespaceHandoffClass, directionalHandoffClass}, ClassKinds: map[string]string{imageHandoffClass: "review", nilContextClass: "review", schemaHandoffClass: "review", compatibilityContextClass: "review", namespaceHandoffClass: "review", directionalHandoffClass: "review"}, ClassFact: "class eligibility"})
}

func TestStorageContextClassProofControls(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module image.fixture\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source := `package fixture
import "context"
func copyImage(ctx context.Context, n int) (int,error) { return n,ctx.Err() }
func Execute(ctx context.Context, n int) (int,error) {
 return guarded(ctx,n)
}
func guarded(ctx context.Context,n int)(int,error) {
 if ctx == nil { ctx = context.Background() }
 image,err := copyImage(ctx,n)
 if err != nil { return 0,err }
 return image,nil
}
`
	extract := func(source string) Inventory {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		return mustExtract(t, Config{Dir: dir, Patterns: []string{"./..."}, IncludeSyntax: true, Roots: []Root{{Package: "image.fixture", Name: "Execute", Required: true}}, Selectors: []Selector{{Kind: "owner-call", Node: "call"}, {Kind: "context-boundary", Node: "call", Packages: []string{"context"}, Names: []string{"Background", "TODO"}}}})
	}
	base := extract(source)
	targets := map[string]bool{"image.fixture.copyImage": true}
	find := func(inv Inventory) (Member, Member) {
		var image, fallback Member
		for _, m := range inv.Members {
			if m.Kind == "owner-call" && contains(m.Callees, "image.fixture.copyImage") {
				image = m
			}
			if m.Kind == "context-boundary" {
				fallback = m
			}
		}
		return image, fallback
	}
	image, fallback := find(base)
	if storageContextHandoff(image, storageDefinitions(base), targets, storageNilDefaultBindings(base, storageDefinitions(base))) == nil || !storageNilContextDefault(fallback) {
		t.Fatal("positive owner proof missing", image, fallback)
	}
	for _, tc := range []struct {
		name, old, replacement string
		image                  bool
	}{
		{"substituted context", "copyImage(ctx,n)", "copyImage(context.Background(),n)", true},
		{"discarded error", "image,err := copyImage(ctx,n)\n if err != nil { return 0,err }", "image,_ := copyImage(ctx,n)", true},
		{"swallowed error", "return 0,err", "return 0,nil", true},
		{"unconditional reset", "if ctx == nil { ctx = context.Background() }", "ctx = context.Background()", false},
		{"later reset", "image,err := copyImage(ctx,n)", "ctx = context.Background(); image,err := copyImage(ctx,n)", true},
		{"escaped context cell", "image,err := copyImage(ctx,n)", "saved := &ctx; _ = saved; image,err := copyImage(ctx,n)", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := extract(strings.Replace(source, tc.old, tc.replacement, 1))
			m, f := find(mutated)
			eligible := storageNilContextDefault(f)
			member := f
			class := nilContextClass
			if tc.image {
				eligible = storageContextHandoff(m, storageDefinitions(mutated), targets, storageNilDefaultBindings(mutated, storageDefinitions(mutated))) != nil
				member = m
				class = imageHandoffClass
			}
			if eligible {
				t.Fatal("planted contradiction supplied a class proof")
			}
			member.Facts = map[string][]string{"class eligibility": nil}
			reviewed := member
			reviewed.Class = class
			reviewed.Reason = "plant: retain false approval after refreshing ordinary body/member hashes"
			failures := Validate(Inventory{Roots: mutated.Roots, Definitions: mutated.Definitions, Members: []Member{member}}, Inventory{Roots: mutated.Roots, Members: []Member{reviewed}}, Policy{Classes: []string{class}, ClassKinds: map[string]string{class: "review"}, ClassFact: "class eligibility"})
			if len(failures) != 1 || !strings.Contains(failures[0], "CLASS FACT CONTRADICTION") {
				t.Fatal("same Validate missed planted contradiction", failures)
			}
		})
	}
	for _, shape := range []string{"if n>0 { image,err = copyImage(ctx,n) } else { image,err = copyImage(ctx,n) }", "switch { case n>0: image,err = copyImage(ctx,n); default: image,err = copyImage(ctx,n) }"} {
		branch := strings.Replace(source, "image,err := copyImage(ctx,n)", "var image int; var err error; "+shape, 1)
		inv := extract(branch)
		for _, m := range inv.Members {
			if m.Kind == "owner-call" && contains(m.Callees, "image.fixture.copyImage") && storageContextHandoff(m, storageDefinitions(inv), targets, storageNilDefaultBindings(inv, storageDefinitions(inv))) == nil {
				t.Fatal("branch error join was not proved", m.Code, m.Line)
			}
		}
		inv = extract(strings.Replace(branch, "if err != nil", "err = nil; if err != nil", 1))
		for _, m := range inv.Members {
			if m.Kind == "owner-call" && contains(m.Callees, "image.fixture.copyImage") && storageContextHandoff(m, storageDefinitions(inv), targets, storageNilDefaultBindings(inv, storageDefinitions(inv))) != nil {
				t.Fatal("overwritten branch error supplied a proof")
			}
		}
	}
}
