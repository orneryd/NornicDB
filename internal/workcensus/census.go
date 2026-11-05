package workcensus

// Context-owner convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
)

// Member identifies one source work site independently of its reviewed class.
// Digest pins formatted source; offsets and inferred bounds aid human review.
type Member struct {
	// SiteID pins this occurrence's byte position, independently of a kind's
	// ordinal. Source controls preserve coordinates of unaffected occurrences.
	SiteID            string                  `json:"site_id,omitempty"`
	UnresolvedTargets bool                    `json:"unresolved_targets,omitempty"`
	OwnerID           string                  `json:"owner_id,omitempty"`
	ContainingOwner   *ContainingOwnerWitness `json:"containing_owner,omitempty"`
	ArgReferences     []string                `json:"arg_references,omitempty"`
	ArgTypes          []string                `json:"arg_types,omitempty"`
	ReceiverReference string                  `json:"receiver_reference,omitempty"`
	ValueReference    string                  `json:"value_reference,omitempty"`
	File              string                  `json:"file,omitempty"`
	OwnerSymbol       string                  `json:"owner_symbol,omitempty"`
	Syntax            *GoSyntax               `json:"-"`
	Facts             map[string][]string     `json:"facts,omitempty"`
	Assignment        string                  `json:"assignment,omitempty"`
	OperandType       string                  `json:"operand_type,omitempty"`
	AssertionType     string                  `json:"assertion_type,omitempty"`
	CommaOK           bool                    `json:"comma_ok,omitempty"`
	EndLine           int                     `json:"end_line,omitempty"`
	EndColumn         int                     `json:"end_column,omitempty"`
	Node              string                  `json:"node,omitempty"`
	OwnerLayer        string                  `json:"owner_layer,omitempty"`
	BoundKnown        bool                    `json:"bound_known,omitempty"`
	Language          string                  `json:"language,omitempty"`
	EndOffset         int                     `json:"end_offset,omitempty"`
	UTF16Offset       int                     `json:"utf16_offset,omitempty"`
	UTF16End          int                     `json:"utf16_end,omitempty"`
	Column            int                     `json:"column,omitempty"`
	OwnerDigest       string                  `json:"owner_digest,omitempty"`
	ReviewOwner       string                  `json:"review_owner,omitempty"`
	Subjects          []string                `json:"subjects,omitempty"`
	Declaration       string                  `json:"declaration,omitempty"`
	State             string                  `json:"state,omitempty"`
	Typed             bool                    `json:"typed,omitempty"`
	Signature         string                  `json:"signature,omitempty"`
	UnderlyingType    string                  `json:"underlying_type,omitempty"`
	Callees           []string                `json:"callees,omitempty"`
	References        []string                `json:"references,omitempty"`
	Owner             string                  `json:"owner"`
	Kind              string                  `json:"kind"`
	Reachable         bool                    `json:"reachable,omitempty"`
	ID                string                  `json:"id"`
	Digest            string                  `json:"digest"`
	Class             string                  `json:"class"`
	Reason            string                  `json:"reason"`
	Line              int                     `json:"line"`
	Code              string                  `json:"code"`
	Scope             string                  `json:"scope,omitempty"`
	Bound             int64                   `json:"bound,omitempty"`
	BoundReason       string                  `json:"bound_reason,omitempty"`
	Offset            int                     `json:"offset,omitempty"`
	CalleeEnd         int                     `json:"callee_end,omitempty"`
	Open              int                     `json:"open,omitempty"`
	FunctionBody      int                     `json:"function_body,omitempty"`
	SliceType         string                  `json:"slice_type,omitempty"`
	ElementType       string                  `json:"element_type,omitempty"`
	Receiver          string                  `json:"receiver,omitempty"`
	ReceiverType      string                  `json:"receiver_type,omitempty"`
	CallSignature     string                  `json:"call_signature,omitempty"`
	StringInput       bool                    `json:"string_input,omitempty"`
	FailureReturn     string                  `json:"failure_return,omitempty"`
	Args              []string                `json:"args,omitempty"`
	Format            string                  `json:"format,omitempty"`
	ScalarArgs        []bool                  `json:"scalar_args,omitempty"`
}

// Inventory records the exact root and member populations for one composition.
type Inventory struct {
	Program         *Program         `json:"-"`
	Definitions     []Definition     `json:"definitions,omitempty"`
	Sources         []SourceBoundary `json:"sources,omitempty"`
	InvisibleBodies []InvisibleBody  `json:"invisible_bodies,omitempty"`
	Limitations     []string         `json:"limitations,omitempty"`
	Roots           []string         `json:"roots"`
	Members         []Member         `json:"members"`
}

// Root identifies a source entrypoint. Receiver optionally restricts a method
// by its declared named receiver (without pointer or generic arguments).
// Required roots fail extraction when no matching function is visible.
type Root struct {
	Package  string `json:"package,omitempty"`
	Name     string `json:"name,omitempty"`
	Receiver string `json:"receiver,omitempty"`
	Required bool   `json:"required,omitempty"`
}

// ContextType describes diagnostic context access, e.g. a standard Context
// parameter (empty Accessor), or a client's wrapper with ".Context()".
type ContextType struct {
	Type     string `json:"type"`
	Accessor string `json:"accessor,omitempty"`
}

// Config describes a composed source tree. Clients supply roots, package scope,
// selectors and optional diagnostic context types as data, never extractors.
type Config struct {
	SourceOnly       bool              `json:"source_only,omitempty"`
	IncludeProgram   bool              `json:"include_program,omitempty"`
	IncludeSyntax    bool              `json:"include_syntax,omitempty"`
	SourceRoots      []SourceRoot      `json:"source_roots,omitempty"`
	Documents        []Document        `json:"documents,omitempty"`
	Tests            bool              `json:"tests,omitempty"`
	Env              []string          `json:"env,omitempty"`
	LoadMode         packages.LoadMode `json:"load_mode,omitempty"`
	IncludeInactive  bool              `json:"include_inactive,omitempty"`
	ExcludeGenerated bool              `json:"exclude_generated,omitempty"`
	Dir              string            `json:"dir,omitempty"`
	Patterns         []string          `json:"patterns,omitempty"`
	BuildFlags       []string          `json:"build_flags,omitempty"`
	Packages         []string          `json:"packages,omitempty"`
	Roots            []Root            `json:"roots,omitempty"`
	Selectors        []Selector        `json:"selectors,omitempty"`
	ContextTypes     []ContextType     `json:"context_types,omitempty"`
	ContextFields    []string          `json:"context_fields,omitempty"`
	Overlay          map[string][]byte `json:"overlay,omitempty"`
}

// Validate rejects new, changed, stale and unclassified members. Classes are
// supplied by each layer; discovery cannot approve or silently exclude work.
func Validate(actual, approved Inventory, policy Policy) []string {
	classes := policy.Classes
	allowed := map[string]bool{}
	for _, c := range classes {
		allowed[c] = true
	}
	listed := map[string]Member{}
	kinds, failures := classKinds(policy)
	controls := map[string]bool{}
	for _, control := range policy.Controls {
		if control.ID == "" || controls[control.ID] || control.MemberID == "" || control.Replacement == "" {
			failures = append(failures, "INVALID OR DUPLICATE CONTROL "+control.ID)
		}
		controls[control.ID] = true
	}
	if policy.RequireSources {
		failures = append(failures, sourceFailures(actual, approved)...)
	}
	actualIDs := map[string]Member{}
	approvedIDs := map[string]Member{}
	actualSites := map[string]bool{}
	for _, m := range actual.Members {
		if m.SiteID != "" {
			if actualSites[m.SiteID] {
				failures = append(failures, "DUPLICATE ACTUAL SITE "+m.SiteID)
			}
			actualSites[m.SiteID] = true
		}
		actualIDs[m.ID] = m
	}
	for _, m := range approved.Members {
		approvedIDs[m.ID] = m
	}
	assignments := map[string]bool{}
	for _, a := range policy.Assignments {
		m, exists := actualIDs[a.MemberID]
		approval := approvedIDs[a.MemberID]
		if !exists || m.Digest != a.Digest || approval.Assignment != a.ID || approval.Class != a.Class || strings.TrimSpace(a.Owner) == "" || strings.TrimSpace(a.Reference) == "" {
			failures = append(failures, "STALE OR INVALID ASSIGNMENT "+a.ID)
		}
		if a.ID == "" || assignments[a.ID] {
			failures = append(failures, "DUPLICATE OR EMPTY ASSIGNMENT "+a.ID)
		}
		assignments[a.ID] = true
	}
	definitions := map[string]Definition{}
	ownerDefinitions := map[string]Definition{}
	unsafeBindings := map[string]bool{}
	for _, definition := range actual.Definitions {
		for _, ref := range definition.Writes {
			unsafeBindings[ref] = true
		}
		for _, ref := range definition.Escapes {
			unsafeBindings[ref] = true
		}
		if definition.Symbol != "" {
			definitions[definition.Symbol] = definition
		}
		if definition.ID != "" {
			if _, exists := ownerDefinitions[definition.ID]; exists {
				failures = append(failures, "DUPLICATE ACTUAL OWNER "+definition.ID)
			}
			ownerDefinitions[definition.ID] = definition
		}
	}
	layers := map[string]map[string]string{}
	for _, layer := range policy.Layers {
		if _, duplicate := layers[layer.Name]; duplicate || strings.TrimSpace(layer.Name) == "" {
			failures = append(failures, "DUPLICATE OR EMPTY OWNER LAYER "+layer.Name)
		}
		members := map[string]string{}
		bodyIDs := map[string]bool{}
		for _, member := range layer.Members {
			if member.Symbol == "" && member.OwnerID == "" {
				failures = append(failures, "EMPTY LAYER MEMBER "+layer.Name)
			}
			if _, duplicate := members[member.Symbol]; member.Symbol != "" && duplicate {
				failures = append(failures, "DUPLICATE LAYER MEMBER "+layer.Name+" "+member.Symbol)
			}
			if member.Symbol != "" {
				members[member.Symbol] = member.Digest
			}
			if member.OwnerID != "" {
				if bodyIDs[member.OwnerID] {
					failures = append(failures, "DUPLICATE LAYER OWNER "+layer.Name+" "+member.OwnerID)
				}
				bodyIDs[member.OwnerID] = true
			}
		}
		layers[layer.Name] = members
	}
	for _, population := range []struct {
		name  string
		roots []string
	}{{"actual", actual.Roots}, {"approved", approved.Roots}} {
		seen := map[string]bool{}
		for _, root := range population.roots {
			if seen[root] {
				failures = append(failures, "DUPLICATE ROOT "+population.name+" "+root)
			}
			seen[root] = true
		}
	}
	for _, m := range approved.Members {
		if _, exists := listed[m.ID]; exists {
			failures = append(failures, "DUPLICATE "+m.ID)
		}
		listed[m.ID] = m
	}
	if strings.Join(actual.Roots, "\n") != strings.Join(approved.Roots, "\n") {
		failures = append(failures, "ROOT POPULATION CHANGED")
	}
	seenActual := map[string]bool{}
	for _, m := range actual.Members {
		if seenActual[m.ID] {
			failures = append(failures, "DUPLICATE ACTUAL "+m.ID)
			continue
		}
		seenActual[m.ID] = true
		a, ok := listed[m.ID]
		if !ok {
			failures = append(failures, "UNLISTED "+m.ID)
			continue
		}
		delete(listed, m.ID)
		failures = append(failures, evidenceFailures(m, a, policy)...)
		if a.Digest != m.Digest {
			failures = append(failures, "CHANGED "+m.ID)
		}
		if !allowed[a.Class] {
			failures = append(failures, "UNCLASSIFIED "+m.ID)
		}
		failures = append(failures, dispositionFailures(m, a, kinds[a.Class], definitions, layers)...)
		if kinds[a.Class] == "delegated" {
			failures = append(failures, delegationWitnessFailures(m, a, policy, actualIDs)...)
		}
		if kinds[a.Class] == "observing-owner" {
			failures = append(failures, containingOwnerFailures(m, a, policy, actualIDs, ownerDefinitions, unsafeBindings)...)
		}
		if kinds[a.Class] == "bounded" && (a.Bound < 0 || a.Bound != m.Bound || a.BoundReason != m.BoundReason) {
			failures = append(failures, "BOUND CHANGED "+m.ID)
		}
		if policy.RequireOwnerDigest && (a.OwnerDigest == "" || a.OwnerDigest != m.OwnerDigest) {
			failures = append(failures, "OWNER CHANGED "+m.ID)
		}
		if policy.RequireReviewOwner && (a.ReviewOwner == "" || (len(policy.Owners) > 0 && !contains(policy.Owners, a.ReviewOwner))) {
			failures = append(failures, "REVIEW OWNER "+m.ID)
		}
		if (policy.RequireSubjects || contains(policy.RequireSubjectsFor, a.Class)) && len(a.Subjects) == 0 {
			failures = append(failures, "NO SUBJECT "+m.ID)
		}
		for _, subject := range a.Subjects {
			if len(policy.Subjects) > 0 && !contains(policy.Subjects, subject) {
				failures = append(failures, "UNKNOWN SUBJECT "+m.ID+" "+subject)
			}
		}
		if strings.TrimSpace(a.Reason) == "" {
			failures = append(failures, "NO REASON "+m.ID)
		}
	}
	for id := range listed {
		failures = append(failures, "STALE "+id)
	}
	sort.Strings(failures)
	return failures
}

// ContextInScope is the context a source function can name: a named context
// parameter of fn or of a function enclosing it, or a configured receiver field.
// Extract records the same reading as a member's diagnostic Scope; clients that
// ask whether a request context exists use this owner. Empty means none.
func ContextInScope(fn *ssa.Function, contexts []ContextType, fields []string) string {
	if len(contexts) == 0 {
		contexts = []ContextType{{Type: "context.Context"}}
	}
	access := func(typ types.Type) (string, bool) {
		name := types.TypeString(typ, func(p *types.Package) string { return p.Path() })
		for _, context := range contexts {
			if context.Type == name {
				return context.Accessor, true
			}
		}
		return "", false
	}
	for current := fn; current != nil; current = current.Parent() {
		receiver := current.Signature.Recv()
		offset := 0
		if receiver != nil {
			offset = 1
		}
		for i, p := range current.Params {
			var declaration *types.Var
			if receiver != nil && i == 0 {
				declaration = receiver
			} else if index := i - offset; index >= 0 && index < current.Signature.Params().Len() {
				declaration = current.Signature.Params().At(index)
			}
			// SSA can name an unnamed input internally. Neither that name
			// nor the blank identifier is accessible to the source owner.
			if declaration == nil || declaration.Name() == "" || declaration.Name() == "_" {
				continue
			}
			if suffix, ok := access(p.Type()); ok {
				return declaration.Name() + suffix
			}
		}
		if recv := receiver; recv != nil && recv.Name() != "" && recv.Name() != "_" && len(current.Params) > 0 && current.Pkg != nil {
			for _, field := range fields {
				obj, _, _ := types.LookupFieldOrMethod(recv.Type(), true, current.Pkg.Pkg, field)
				if obj != nil {
					if suffix, ok := access(obj.Type()); ok {
						return recv.Name() + "." + field + suffix
					}
				}
			}
		}
	}
	return ""
}

func smallBound(n ast.Node, info *types.Info, lengths map[types.Object]int64) (int64, string) {
	value := func(e ast.Expr) (int64, bool) {
		v := info.Types[e].Value
		if v == nil || v.Kind() != constant.Int {
			return 0, false
		}
		i, ok := constant.Int64Val(v)
		return i, ok
	}
	switch n := n.(type) {
	case *ast.RangeStmt:
		if id, ok := n.X.(*ast.Ident); ok {
			if bound, known := lengths[info.ObjectOf(id)]; known && bound >= 0 {
				return bound, "maximum of all fixed-size initializers/assignments (no unknown length writer)"
			}
		}
		if typ := info.TypeOf(n.X); typ != nil {
			if a, ok := typ.Underlying().(*types.Array); ok {
				return a.Len(), "fixed array length"
			}
		}
		if lit, ok := n.X.(*ast.CompositeLit); ok {
			if bound, ok := literalLength(lit, info); ok {
				return bound, "fixed literal members"
			}
		}
		if v := info.Types[n.X].Value; v != nil && v.Kind() == constant.String {
			return int64(len(constant.StringVal(v))), "constant string bytes"
		}
		if bound, ok := value(n.X); ok {
			return bound, "constant range"
		}
	case *ast.ForStmt:
		if condition, ok := n.Cond.(*ast.BinaryExpr); ok && (condition.Op == token.LSS || condition.Op == token.LEQ) {
			if bound, ok := value(condition.Y); ok && bound >= 0 {
				// A constant limit is a bound only for zero-based, increasing
				// induction. Arbitrary decrementing loops are never exempted.
				init, ok := n.Init.(*ast.AssignStmt)
				if !ok || init.Tok != token.DEFINE || len(init.Lhs) != 1 || len(init.Rhs) != 1 {
					return 0, ""
				}
				start, ok := value(init.Rhs[0])
				if !ok || start != 0 {
					return 0, ""
				}
				post, ok := n.Post.(*ast.IncDecStmt)
				if !ok || post.Tok != token.INC {
					return 0, ""
				}
				index, ok := init.Lhs[0].(*ast.Ident)
				if !ok {
					return 0, ""
				}
				next, ok := post.X.(*ast.Ident)
				if !ok || info.ObjectOf(index) != info.ObjectOf(next) {
					return 0, ""
				}
				test, ok := condition.X.(*ast.Ident)
				if !ok || info.ObjectOf(test) != info.ObjectOf(index) {
					return 0, ""
				}
				mutated := false
				ast.Inspect(n.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.AssignStmt:
						for _, lhs := range n.Lhs {
							if id, ok := lhs.(*ast.Ident); ok && info.ObjectOf(id) == info.ObjectOf(index) {
								mutated = true
							}
						}
					case *ast.IncDecStmt:
						if id, ok := n.X.(*ast.Ident); ok && info.ObjectOf(id) == info.ObjectOf(index) {
							mutated = true
						}
					case *ast.UnaryExpr:
						if id, ok := n.X.(*ast.Ident); ok && n.Op == token.AND && info.ObjectOf(id) == info.ObjectOf(index) {
							mutated = true
						}
					}
					return !mutated
				})
				if mutated {
					return 0, ""
				}
				if condition.Op == token.LEQ {
					// The next increment must represent the exit value.
					// Otherwise a narrow counter wraps and the loop never ends.
					basic, ok := info.TypeOf(index).Underlying().(*types.Basic)
					if !ok {
						return 0, ""
					}
					maximum := int64(1<<63 - 1)
					switch basic.Kind() {
					case types.Int8:
						maximum = 1<<7 - 1
					case types.Uint8:
						maximum = 1<<8 - 1
					case types.Int16:
						maximum = 1<<15 - 1
					case types.Uint16:
						maximum = 1<<16 - 1
					case types.Int32, types.Int:
						maximum = 1<<31 - 1
					case types.Uint32, types.Uint, types.Uintptr:
						maximum = 1<<32 - 1
					}
					if bound >= maximum {
						return 0, ""
					}
					bound++
				}
				return bound, "constant loop limit (requires monotonic induction review)"
			}
		}
	}
	return 0, ""
}

func literalLength(lit *ast.CompositeLit, info *types.Info) (int64, bool) {
	if typ := info.TypeOf(lit); typ != nil {
		if _, ok := typ.Underlying().(*types.Map); ok {
			return int64(len(lit.Elts)), true
		}
	}
	next, maximum := int64(0), int64(0)
	for _, element := range lit.Elts {
		if keyed, ok := element.(*ast.KeyValueExpr); ok {
			value := info.Types[keyed.Key].Value
			if value == nil || value.Kind() != constant.Int {
				return 0, false
			}
			index, ok := constant.Int64Val(value)
			if !ok || index < 0 {
				return 0, false
			}
			next = index
		}
		next++
		maximum = max(maximum, next)
	}
	return maximum, true
}

// A fixed configuration is a bound only when every assignment is a fixed
// initializer. Unknown producers, append recurrence, parameters and aliases
// with unknown length disqualify it. This is a conservative size analysis.
func fixedLengths(pkgs []*packages.Package) map[types.Object]int64 {
	type source struct {
		expr ast.Expr
		info *types.Info
	}
	writes := map[types.Object][]source{}
	unknown := map[types.Object]bool{}
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.FuncType:
					if n.Params != nil {
						for _, field := range n.Params.List {
							for _, name := range field.Names {
								unknown[p.TypesInfo.ObjectOf(name)] = true
							}
						}
					}
				case *ast.ValueSpec:
					for i, name := range n.Names {
						obj := p.TypesInfo.ObjectOf(name)
						if len(n.Values) == 0 {
							writes[obj] = append(writes[obj], source{nil, p.TypesInfo})
						} else if len(n.Names) == len(n.Values) {
							writes[obj] = append(writes[obj], source{n.Values[i], p.TypesInfo})
						} else {
							unknown[obj] = true
						}
					}
				case *ast.UnaryExpr:
					if n.Op == token.AND {
						if id, ok := n.X.(*ast.Ident); ok {
							unknown[p.TypesInfo.ObjectOf(id)] = true
						}
					}
				case *ast.AssignStmt:
					for i, lhs := range n.Lhs {
						if id, ok := lhs.(*ast.Ident); ok {
							obj := p.TypesInfo.ObjectOf(id)
							if len(n.Lhs) == len(n.Rhs) {
								writes[obj] = append(writes[obj], source{n.Rhs[i], p.TypesInfo})
							} else {
								unknown[obj] = true
							}
						}
					}
				}
				return true
			})
		}
	}
	result := map[types.Object]int64{}
	var length func(ast.Expr, *types.Info, map[types.Object]bool) (int64, bool)
	var object func(types.Object, map[types.Object]bool) (int64, bool)
	object = func(obj types.Object, seen map[types.Object]bool) (int64, bool) {
		if obj == nil || unknown[obj] || seen[obj] {
			return 0, false
		}
		if n, known := result[obj]; known {
			return n, n >= 0
		}
		seen[obj] = true
		defer delete(seen, obj)
		values, known := writes[obj]
		if !known {
			return 0, false
		}
		maximum := int64(0)
		for _, value := range values {
			n, ok := length(value.expr, value.info, seen)
			if !ok {
				result[obj] = -1
				return 0, false
			}
			maximum = max(maximum, n)
		}
		result[obj] = maximum
		return maximum, true
	}
	length = func(e ast.Expr, info *types.Info, seen map[types.Object]bool) (int64, bool) {
		if e == nil {
			return 0, true
		}
		if v := info.Types[e].Value; v != nil && v.Kind() == constant.String {
			return int64(len(constant.StringVal(v))), true
		}
		if typ := info.TypeOf(e); typ != nil {
			if a, ok := typ.Underlying().(*types.Array); ok {
				return a.Len(), true
			}
		}
		switch e := e.(type) {
		case *ast.Ident:
			if e.Name == "nil" {
				return 0, true
			}
			return object(info.ObjectOf(e), seen)
		case *ast.CompositeLit:
			typ := info.TypeOf(e)
			if typ == nil {
				return 0, false
			}
			switch typ.Underlying().(type) {
			case *types.Slice, *types.Array:
				return literalLength(e, info)
			}
			return 0, false
		case *ast.SliceExpr:
			// Reslicing can exceed len(base). Only an explicit constant high
			// gives a fixed extent without capacity analysis. A successful
			// dynamic low can only shorten that extent.
			if e.High == nil {
				return 0, false
			}
			high := info.Types[e.High].Value
			if high == nil || high.Kind() != constant.Int {
				return 0, false
			}
			n, ok := constant.Int64Val(high)
			if !ok || n < 0 {
				return 0, false
			}
			if e.Low != nil {
				if low := info.Types[e.Low].Value; low != nil && low.Kind() == constant.Int {
					l, ok := constant.Int64Val(low)
					if !ok || l < 0 || l > n {
						return 0, false
					}
					n -= l
				}
			}
			return n, true
		case *ast.CallExpr:
			if builtin, ok := callObject(e.Fun, info).(*types.Builtin); ok && builtin.Name() == "make" && len(e.Args) > 1 {
				if typ := info.TypeOf(e); typ == nil {
					return 0, false
				} else {
					if _, ok := typ.Underlying().(*types.Slice); !ok {
						return 0, false
					}
				}
				v := info.Types[e.Args[1]].Value
				if v != nil && v.Kind() == constant.Int {
					return constant.Int64Val(v)
				}
			}
		}
		return 0, false
	}
	for obj := range writes {
		object(obj, map[types.Object]bool{})
	}
	return result
}
