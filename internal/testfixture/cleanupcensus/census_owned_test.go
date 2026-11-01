// Census v11: every release of a lifetime the owner adopted, outside the owner.
//
// Contract (method-gap note 3): a lifetime adopted by Own or OwnWithClose is
// released only by the owner's registered close or by the closer OwnWithClose
// returns. Any Close call on that lifetime outside internal/testfixture is a
// member, whatever its signature and however its error is handled: direct,
// through a variable, a closure capture, a defer or go statement, a wrapper
// whose Close reaches the adopted type's Close, a holder (a value built from
// an adopted argument) whose field can hold it, or a struct field it was
// stored in. The only exceptions are entries of the reviewed release ledger
// (release_ledger.tsv): intentional API or closed-state assertions, and
// releases of a different, never-adopted instance held by the same variable.
//
// Identity is the SSA value, followed through conversions, variable loads and
// stores, closure bindings, phi edges, field locations (root of the base,
// field index), results across every call-graph edge (static, closures and
// method values resolved by VTA), adopted parameters, wrappers and holders.
//
// Not modelled (the syntax enumerator in census_enum_test.go must account for
// any release these could hide): maps, slices and channels as containers;
// reflection; releases in non-test product code; files behind build tags the
// SSA load excludes.
// Author of the overlay/source, captured-cell and helper-flow correction: L4 lane, Codex,
// 1 November 2026. The r52b receipt remains the failing evidence.

package cleanupcensus_test

import (
	"bufio"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

func baseName(fn *ssa.Function) string {
	name := fn.Name()
	if index := strings.IndexByte(name, '['); index >= 0 {
		name = name[:index]
	}
	return name
}

type fieldLoc struct {
	base  ssa.Value
	field int
}

// ledgerEntry is one reviewed exception: file, enclosing function, statement text, reason.
type ledgerEntry struct {
	file, function, text, reason string
	matched, enumMatched         bool
}

func ledgerKey(file, function, text string) string { return file + "\t" + function + "\t" + text }

// loadLedger reads release_ledger.tsv next to this file (tab-separated: file,
// function, statement text, reason; # comments).
func loadLedger(path string, overlay map[string][]byte) (map[string]*ledgerEntry, []string) {
	entries := map[string]*ledgerEntry{}
	var problems []string
	data, ok := overlay[path]
	if !ok {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			return entries, []string{"ledger unreadable: " + err.Error()}
		}
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimRight(scanner.Text(), "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 4 || strings.TrimSpace(parts[3]) == "" {
			problems = append(problems, fmt.Sprintf("ledger line %d malformed or without reason", n))
			continue
		}
		key := ledgerKey(parts[0], parts[1], parts[2])
		if _, dup := entries[key]; dup {
			problems = append(problems, fmt.Sprintf("ledger line %d duplicates an earlier entry", n))
			continue
		}
		entries[key] = &ledgerEntry{file: parts[0], function: parts[1], text: parts[2], reason: parts[3]}
	}
	return entries, problems
}

// sourceLine returns the trimmed text of a file line (cached).
func (a *analysis) sourceLine(pos token.Pos) (string, string, int) {
	p := a.fset.Position(pos)
	rel := relative(a.base, p.Filename)
	if a.lines == nil {
		a.lines = map[string][]string{}
	}
	lines, ok := a.lines[p.Filename]
	if !ok {
		data, overlaid := a.overlay[p.Filename]
		if !overlaid {
			data, _ = os.ReadFile(p.Filename)
		}
		lines = strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
		a.lines[p.Filename] = lines
	}
	text := ""
	if p.Line-1 < len(lines) && p.Line > 0 {
		text = strings.TrimSpace(lines[p.Line-1])
	}
	return rel, text, p.Line
}

// ownedReleases fills c.ownedDropped (members), c.ownedChecked (ledger-justified),
// c.ownedAmbiguous (members whose variable also holds an un-owned value) and
// c.ledgerStale, and c.ownedPositions (every release found, for the enumerator).
func (a *analysis) ownedReleases(c *census, prog *ssa.Program, functions []*ssa.Function, siteCallees map[ssa.CallInstruction][]*ssa.Function) {
	free := map[*ssa.FreeVar][]ssa.Value{}
	for _, fn := range functions {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				if mc, ok := instr.(*ssa.MakeClosure); ok {
					if target, ok := mc.Fn.(*ssa.Function); ok {
						for i, fv := range target.FreeVars {
							if i < len(mc.Bindings) {
								free[fv] = append(free[fv], mc.Bindings[i])
							}
						}
					}
				}
			}
		}
	}
	fields := map[fieldLoc]ssa.Value{}
	// Return identity is a call-site substitution, never a global adoption
	// fact about a forwarding helper such as track. Otherwise adopting one
	// track result would falsely adopt every unrelated TrackResult caller.
	returnInputs := map[*ssa.Function]map[int]map[int]bool{}

	var roots func(v ssa.Value, depth int, seen map[ssa.Value]bool) []ssa.Value
	callRoots := func(call *ssa.Call, index, depth int, seen map[ssa.Value]bool) []ssa.Value {
		var out []ssa.Value
		args := ownershipArguments(call)
		for _, target := range resolvedCallees(call, siteCallees) {
			for input := range returnInputs[target][index] {
				if input < len(args) {
					out = append(out, roots(args[input], depth+1, seen)...)
				}
			}
		}
		return out
	}
	fieldRoot := func(x ssa.Value, field int, self ssa.Value, depth int, seen map[ssa.Value]bool) []ssa.Value {
		var out []ssa.Value
		for _, base := range roots(x, depth+1, seen) {
			loc := fieldLoc{base, field}
			canon, ok := fields[loc]
			if !ok {
				fields[loc] = self
				canon = self
			}
			out = append(out, canon)
		}
		if len(out) == 0 {
			out = []ssa.Value{self}
		}
		return out
	}
	roots = func(v ssa.Value, depth int, seen map[ssa.Value]bool) []ssa.Value {
		if v == nil || seen[v] {
			return nil
		}
		seen[v] = true
		switch x := v.(type) {
		case *ssa.Const:
			return nil
		case *ssa.Alloc:
			// A captured variable is a cell containing the resource pointer.
			// Follow its stores as well as the cell, so a field initialized on
			// the resource and read through the capture has one location.
			if pointer, ok := x.Type().Underlying().(*types.Pointer); ok {
				_, pointerCell := pointer.Elem().Underlying().(*types.Pointer)
				_, interfaceCell := pointer.Elem().Underlying().(*types.Interface)
				if pointerCell || interfaceCell {
					out := []ssa.Value{v}
					if refs := x.Referrers(); refs != nil {
						for _, ref := range *refs {
							if store, ok := ref.(*ssa.Store); ok && store.Addr == x {
								out = append(out, roots(store.Val, depth+1, seen)...)
							}
						}
					}
					return out
				}
			}
		case *ssa.MakeInterface:
			return roots(x.X, depth+1, seen)
		case *ssa.ChangeType:
			return roots(x.X, depth+1, seen)
		case *ssa.ChangeInterface:
			return roots(x.X, depth+1, seen)
		case *ssa.Convert:
			return roots(x.X, depth+1, seen)
		case *ssa.TypeAssert:
			return roots(x.X, depth+1, seen)
		case *ssa.Extract:
			if call, ok := x.Tuple.(*ssa.Call); ok {
				if out := callRoots(call, x.Index, depth, seen); len(out) > 0 {
					return out
				}
			}
			if asserted, ok := x.Tuple.(*ssa.TypeAssert); ok && x.Index == 0 {
				return roots(asserted.X, depth+1, seen)
			}
		case *ssa.UnOp:
			if x.Op == token.MUL {
				return roots(x.X, depth+1, seen)
			}
		case *ssa.FieldAddr:
			return fieldRoot(x.X, x.Field, x, depth, seen)
		case *ssa.Field:
			return fieldRoot(x.X, x.Field, x, depth, seen)
		case *ssa.FreeVar:
			var out []ssa.Value
			for _, bound := range free[x] {
				out = append(out, roots(bound, depth+1, seen)...)
			}
			if len(out) > 0 {
				return out
			}
		case *ssa.Phi:
			var out []ssa.Value
			for _, edge := range x.Edges {
				out = append(out, roots(edge, depth+1, seen)...)
			}
			return out
		case *ssa.Call:
			if out := callRoots(x, 0, depth, seen); len(out) > 0 {
				return out
			}
			// The owner's adapters (Redacted) hold the resource they adapt.
			if callee := x.Common().StaticCallee(); callee != nil && functionPackage(callee) == fixturePath && baseName(callee) == "Redacted" {
				var out []ssa.Value
				for _, arg := range x.Common().Args {
					out = append(out, roots(arg, depth+1, seen)...)
				}
				return out
			}
		}
		return []ssa.Value{v}
	}
	rootsOf := func(v ssa.Value) []ssa.Value { return roots(v, 0, map[ssa.Value]bool{}) }
	isFuncValue := func(v ssa.Value) bool {
		_, ok := v.Type().Underlying().(*types.Signature)
		return ok
	}
	// Summarize only identity returns (parameters returned unchanged through
	// aliases/helpers). The summary substitutes actual inputs at each call;
	// it neither adopts those inputs nor treats holder fields as identity.
	for {
		changed := false
		for _, fn := range functions {
			if !strings.HasPrefix(functionPackage(fn), modulePath) {
				continue
			}
			for _, block := range fn.Blocks {
				for _, instr := range block.Instrs {
					ret, ok := instr.(*ssa.Return)
					if !ok {
						continue
					}
					for result, value := range ret.Results {
						for _, root := range rootsOf(value) {
							for input, param := range fn.Params {
								if root != param {
									continue
								}
								if returnInputs[fn] == nil {
									returnInputs[fn] = map[int]map[int]bool{}
								}
								if returnInputs[fn][result] == nil {
									returnInputs[fn][result] = map[int]bool{}
								}
								if !returnInputs[fn][result][input] {
									returnInputs[fn][result][input] = true
									changed = true
								}
							}
						}
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	owned := map[string]bool{}        // functions the owner runs
	adopted := map[ssa.Value]string{} // adopted lifetimes -> adoption position
	intrinsic := map[ssa.Value]bool{} // adoption inside this helper, separate from forward taint
	derived := map[ssa.Value]string{} // wrappers whose Close reaches an adopted type's Close
	holders := map[ssa.Value]string{} // values built from an adopted argument
	mark := markOrigin
	add := func(v ssa.Value, at string) bool {
		changed := false
		for _, r := range rootsOf(v) {
			if !isFuncValue(r) && mark(adopted, r, at) {
				changed = true
			}
		}
		return changed
	}
	variadic := func(v ssa.Value) []ssa.Value {
		slice, ok := v.(*ssa.Slice)
		if !ok {
			return nil
		}
		var out []ssa.Value
		if refs := slice.X.Referrers(); refs != nil {
			for _, ref := range *refs {
				if index, ok := ref.(*ssa.IndexAddr); ok {
					if inner := index.Referrers(); inner != nil {
						for _, use := range *inner {
							if store, ok := use.(*ssa.Store); ok && store.Addr == index {
								out = append(out, store.Val)
							}
						}
					}
				}
			}
		}
		return out
	}
	// The one adoption intrinsic is ownedClose's resource and dependency
	// input. Public entry points, generic factories and curried closures are
	// discovered through their actual calls, not a maintained API-name list.
	for _, fn := range functions {
		if functionPackage(fn) == fixturePath && baseName(fn) == "ownedClose" && len(fn.Params) == 3 {
			intrinsic[fn.Params[1]], intrinsic[fn.Params[2]] = true, true
		}
	}
	adoptionValues := func(value ssa.Value) []ssa.Value {
		if elements := variadic(value); len(elements) > 0 {
			return elements
		}
		return []ssa.Value{value}
	}
	for {
		changed := false
		for _, fn := range functions {
			if !strings.HasPrefix(functionPackage(fn), modulePath) {
				continue
			}
			for _, block := range fn.Blocks {
				for _, instr := range block.Instrs {
					call, ok := instr.(ssa.CallInstruction)
					if !ok {
						continue
					}
					args := ownershipArguments(call)
					for _, target := range resolvedCallees(call, siteCallees) {
						for i, param := range target.Params {
							if i >= len(args) || !intrinsic[param] {
								continue
							}
							for _, value := range adoptionValues(args[i]) {
								for _, root := range rootsOf(value) {
									if !intrinsic[root] {
										intrinsic[root], changed = true, true
									}
								}
							}
						}
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	for _, fn := range functions {
		if functionPackage(fn) == fixturePath || !strings.HasPrefix(functionPackage(fn), modulePath) {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				args := ownershipArguments(call)
				adopts := false
				for _, target := range resolvedCallees(call, siteCallees) {
					for i, param := range target.Params {
						if i >= len(args) || !intrinsic[param] {
							continue
						}
						adopts = true
						at := a.position(call.Common().Pos())
						for _, value := range adoptionValues(args[i]) {
							// Explicitly adopted CloseFunc values are resources too,
							// including when the owner return is ignored.
							for _, root := range rootsOf(value) {
								mark(adopted, root, at)
							}
							for _, release := range a.adoptedReleases(prog, value, 0) {
								if release.Blocks != nil && functionPackage(release) != fixturePath {
									for reached := range a.reach([]*ssa.Function{release}, siteCallees, false).reached {
										owned[functionID(reached)] = true
									}
								}
							}
						}
					}
				}
				if adopts {
					c.ownerAdoptionSites++
				}
			}
		}
	}
	// Registered callbacks share the canonical outcome-use checker.
	c.ownerReleaseRanges = map[string][][2]int{}
	for _, fn := range functions {
		if owned[functionID(fn)] {
			if syntax := fn.Syntax(); syntax != nil {
				start, end := a.fset.Position(syntax.Pos()), a.fset.Position(syntax.End())
				name := filepath.Clean(start.Filename)
				c.ownerReleaseRanges[name] = append(c.ownerReleaseRanges[name], [2]int{start.Offset, end.Offset})
			}
		}
	}
	a.adoptionEffects(c, functions, siteCallees)
	closeOf := func(t types.Type) *ssa.Function {
		// roots can be captured cells (**Resource), rather than the loaded
		// *Resource. Method lookup belongs to the resource, not its cell.
		for {
			pointer, ok := t.Underlying().(*types.Pointer)
			if !ok {
				break
			}
			if _, cell := pointer.Elem().Underlying().(*types.Pointer); !cell {
				break
			}
			t = pointer.Elem()
		}
		if types.IsInterface(t) {
			return nil
		}
		selection := prog.MethodSets.MethodSet(t).Lookup(nil, "Close")
		if selection == nil {
			return nil
		}
		return prog.MethodValue(selection)
	}
	reachMemo := map[*ssa.Function]map[*ssa.Function]bool{}
	reachesAdoptedClose := func(wrapper ssa.Value, inner ssa.Value) bool {
		outer := closeOf(wrapper.Type())
		target := closeOf(inner.Type())
		if outer == nil || target == nil || outer == target {
			return false
		}
		reached, ok := reachMemo[outer]
		if !ok {
			reached = a.reach([]*ssa.Function{outer}, siteCallees, true).reached
			reachMemo[outer] = reached
		}
		return reached[target]
	}
	inAny := func(r ssa.Value) (string, bool) {
		if at, ok := adopted[r]; ok {
			return at, true
		}
		if at, ok := derived[r]; ok {
			return at, true
		}
		return "", false
	}
	// A field loaded from a holder whose type can hold a closable lifetime is
	// treated as the adopted lifetime (over-reported by design; a surplus is a
	// ledger entry with its reason).
	heldField := func(r ssa.Value) (string, bool) {
		var x ssa.Value
		switch f := r.(type) {
		case *ssa.FieldAddr:
			x = f.X
		case *ssa.Field:
			x = f.X
		default:
			return "", false
		}
		origin := ""
		for _, base := range rootsOf(x) {
			for _, at := range []string{holders[base], adopted[base], derived[base]} {
				if at != "" && (origin == "" || at < origin) {
					origin = at
				}
			}
		}
		return origin, origin != ""
	}
	// Compute the monotone closure of value flow. A fixed round count can
	// silently omit a long helper chain. Only explicit adoption of a helper's
	// parameter flows back to its callers; passing an adopted argument to a
	// release helper does not adopt unrelated arguments at its other callers.
	for {
		changed := false
		for _, fn := range functions {
			for _, block := range fn.Blocks {
				for _, instr := range block.Instrs {
					if site, ok := instr.(ssa.CallInstruction); ok {
						args := site.Common().Args
						if site.Common().IsInvoke() {
							args = append([]ssa.Value{site.Common().Value}, args...)
						}
						for _, target := range siteCallees[site] {
							if functionPackage(target) == fixturePath || !strings.HasPrefix(functionPackage(target), modulePath) || !a.isTestCode(target) {
								continue
							}
							for i, param := range target.Params {
								if i >= len(args) {
									break
								}
								for _, root := range rootsOf(args[i]) {
									for _, values := range []map[ssa.Value]string{adopted, derived, holders} {
										if at, ok := values[root]; ok && mark(values, param, at) {
											changed = true
										}
									}
								}
							}
						}
					}
					switch x := instr.(type) {
					case *ssa.FieldAddr, *ssa.Field:
						value := instr.(ssa.Value)
						if at, ok := heldField(value); ok && add(value, at) {
							changed = true
						}
					case *ssa.Store:
						for _, r := range rootsOf(x.Val) {
							// A constructor can create/adopt a member internally and
							// return the containing aggregate, without any owned
							// argument. Keep that holder on the existing Store and
							// Return flow; a Close wrapper additionally needs the
							// same concrete delegation proof used for call results.
							if field, ok := x.Addr.(*ssa.FieldAddr); ok {
								at, owns := inAny(r)
								if !owns {
									at = holders[r]
								}
								if at != "" {
									for _, parent := range rootsOf(field.X) {
										if mark(holders, parent, at) {
											changed = true
										}
										if owns && reachesAdoptedClose(parent, r) && mark(derived, parent, at) {
											changed = true
										}
									}
								}
							}
							for _, addr := range rootsOf(x.Addr) {
								if at, ok := adopted[r]; ok && mark(adopted, addr, at) {
									changed = true
								}
								if at, ok := derived[r]; ok && mark(derived, addr, at) {
									changed = true
								}
								if at, ok := holders[r]; ok && mark(holders, addr, at) {
									changed = true
								}
							}
						}
					case *ssa.Return:
						for index, result := range x.Results {
							for _, r := range rootsOf(result) {
								for _, m := range []map[ssa.Value]string{adopted, derived, holders} {
									if at, ok := m[r]; ok && a.markResults(fn, index, at, m) {
										changed = true
									}
								}
							}
						}
					case *ssa.Call:
						callee := x.Common().StaticCallee()
						if callee != nil && functionPackage(callee) == fixturePath {
							continue
						}
						for _, arg := range x.Common().Args {
							for _, r := range rootsOf(arg) {
								at, ok := inAny(r)
								if !ok {
									if hat, isHolder := holders[r]; isHolder {
										at, ok = hat, true
									}
								}
								if !ok {
									continue
								}
								for _, result := range callResults(x) {
									if reachesAdoptedClose(result, r) && mark(derived, result, at) {
										changed = true
									}
									if mark(holders, result, at) {
										changed = true
									}
								}
							}
						}

					}
				}
			}
		}
		if !changed {
			break
		}
	}
	mixed := map[ssa.Value]bool{}
	for _, fn := range functions {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				store, ok := instr.(*ssa.Store)
				if !ok {
					continue
				}
				for _, addr := range rootsOf(store.Addr) {
					if _, ok := inAny(addr); !ok {
						continue
					}
					for _, r := range rootsOf(store.Val) {
						if _, ok := inAny(r); !ok {
							if _, held := holders[r]; !held {
								mixed[addr] = true
							}
						}
					}
				}
			}
		}
	}
	ledger, problems := loadLedger(filepath.Join(a.base, "internal", "testfixture", "cleanupcensus", "release_ledger.tsv"), a.overlay)
	c.ledgerProblems = append(c.ledgerProblems, problems...)
	c.ownedPositions = map[string]bool{}
	var members, justified, ambiguous []string
	for _, fn := range functions {
		if functionPackage(fn) == fixturePath || !strings.HasPrefix(functionPackage(fn), modulePath) {
			continue
		}
		outsideConsumerScope := !a.isTestCode(fn) || owned[functionID(fn)] || fn.Synthetic != ""
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				var common *ssa.CallCommon
				switch x := instr.(type) {
				case *ssa.Call:
					common = x.Common()
				case *ssa.Defer:
					common = x.Common()
				case *ssa.Go:
					common = x.Common()
				default:
					continue
				}
				var recv ssa.Value
				if common.IsInvoke() {
					if common.Method.Name() != "Close" {
						continue
					}
					recv = common.Value
				} else if callee := common.StaticCallee(); callee != nil && callee.Signature.Recv() != nil && callee.Name() == "Close" && len(common.Args) > 0 {
					recv = common.Args[0]
				} else {
					continue
				}
				// Author: L4 lane (Codex), 1 Nov 2026. Describe every module
				// Close call for the per-row source review, including syntax
				// surplus and registered release callbacks. These facts do not
				// accept, reject or suppress a member, and do not call rootsOf.
				var targets []string
				for _, target := range siteCallees[instr.(ssa.CallInstruction)] {
					targets = append(targets, target.String()+"@"+a.position(target.Pos()))
				}
				if target := common.StaticCallee(); target != nil {
					targets = append(targets, target.String()+"@"+a.position(target.Pos()))
				}
				c.closeFacts = append(c.closeFacts, fmt.Sprintf("%s in %s | receiver %s | value %s | targets %s | owner-delegate function %t | test-support %t",
					a.position(common.Pos()), fn.String(), recv.Type(), strings.ReplaceAll(recv.String(), "\n", " "), strings.Join(unique(targets), ", "), owned[functionID(fn)], a.isTestCode(fn)))
				if outsideConsumerScope {
					continue
				}
				for _, r := range rootsOf(recv) {
					at, kind := adopted[r], "adopted"
					if at == "" {
						at, kind = derived[r], "wrapper"
					}
					if at == "" {
						if hat, ok := heldField(r); ok {
							at, kind = hat, "held field"
						}
					}
					if at == "" {
						continue
					}
					pos := a.position(common.Pos())
					c.ownedPositions[pos] = true
					file, text, _ := a.sourceLine(common.Pos())
					entry := fmt.Sprintf("%s in %s: %s owned at %s | %s", pos, fn.String(), kind, at, text)
					if e, ok := ledger[ledgerKey(file, fn.String(), text)]; ok {
						e.matched = true
						justified = append(justified, entry+" [ledger: "+e.reason+"]")
						break
					}
					if mixed[r] {
						ambiguous = append(ambiguous, entry+" (variable also holds an un-owned value; needs a ledger entry or a fix)")
						break
					}
					members = append(members, entry)
					break
				}
			}
		}
	}
	c.ledger = ledger
	sort.Strings(members)
	sort.Strings(justified)
	sort.Strings(ambiguous)
	c.ownedDropped = unique(members)
	c.ownedChecked = unique(justified)
	c.ownedAmbiguous = unique(ambiguous)
	c.closeFacts = unique(c.closeFacts)
}

// Author: L4 lane (Codex), 1 Nov 2026. Shared helper parameters can
// receive several adopted instances. Retain a stable representative source;
// the monotone fixed point propagates improvements as well as new membership.
// First traversal order must not choose the diagnostic's adoption provenance.
func markOrigin(m map[ssa.Value]string, value ssa.Value, at string) bool {
	if old, exists := m[value]; exists && old <= at {
		return false
	}
	m[value] = at
	return true
}

// callResults returns the values a call produces: the call itself, or its
// tuple's extracts.
func callResults(call *ssa.Call) []ssa.Value {
	if _, ok := call.Type().(*types.Tuple); !ok {
		return []ssa.Value{call}
	}
	var out []ssa.Value
	if refs := call.Referrers(); refs != nil {
		for _, ref := range *refs {
			if extract, ok := ref.(*ssa.Extract); ok {
				out = append(out, extract)
			}
		}
	}
	return out
}

// markResults marks result index of every call of fn found on the call graph
// (static calls, closures and method values alike).
func (a *analysis) markResults(fn *ssa.Function, index int, at string, m map[ssa.Value]string) bool {
	changed := false
	node := a.cg.Nodes[fn]
	if node == nil {
		return false
	}
	for _, edge := range node.In {
		call, ok := edge.Site.(*ssa.Call)
		if !ok {
			continue
		}
		var result ssa.Value
		if _, isTuple := call.Type().(*types.Tuple); !isTuple {
			if index == 0 {
				result = call
			}
		} else {
			for _, r := range callResults(call) {
				if r.(*ssa.Extract).Index == index {
					result = r
				}
			}
		}
		if result == nil {
			continue
		}
		if markOrigin(m, result, at) {
			changed = true
		}
	}
	return changed
}

// Author: L4 lane (Codex), 1 Nov 2026. Renamed owned adapters must not hide
// a discarded direct, context-taking or delegated Close result behind nil.
const plantedOwnerDelegates = `package replication

import (
    "context"
    "errors"
    "testing"
    fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

type zzOwnerResource struct{}
func (*zzOwnerResource) Close() error { return errors.New("planted close error") }
type zzOwnerContextResource struct{}
func (*zzOwnerContextResource) Close(context.Context) error { return errors.New("planted context close error") }
type zzOwnerGroup struct{ resource *zzOwnerResource }
func (g *zzOwnerGroup) retire() { _ = g.resource.Close() }
func zzOwnedDelegateBare(t *testing.T) {
    bare := &zzOwnerResource{}
    fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { bare.Close(); return nil }))
}
func zzOwnedDelegateBlank(t *testing.T) {
    blank := &zzOwnerResource{}
    fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { _ = blank.Close(); return nil }))
}
func zzOwnedDelegateDefer(t *testing.T) {
    delayed := &zzOwnerResource{}
    fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { defer delayed.Close(); return nil }))
}
func zzOwnedDelegateGo(t *testing.T) {
    async := &zzOwnerResource{}
    fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { go async.Close(); return nil }))
}
func zzOwnedDelegateGroup(t *testing.T) {
    group := &zzOwnerGroup{resource: &zzOwnerResource{}}
    fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { group.retire(); return nil }))
}
func zzOwnedDelegateContext(t *testing.T) {
    contextual := &zzOwnerContextResource{}
    fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { _ = contextual.Close(context.Background()); return nil }))
}
func zzOwnedDelegateReturned(t *testing.T) {
    returned := &zzOwnerResource{}
    fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { return returned.Close() }))
}
`

// plantedOwnedRelease holds one member per modelled shape, at fixed lines (see runMatrix).
const plantedOwnedRelease = `package replication

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

type zzOwnedRes struct{}

func (*zzOwnedRes) Close() error { return nil }

type zzOwnedWrap struct{ inner *zzOwnedRes }

func (w *zzOwnedWrap) Close() error { return w.inner.Close() }

func zzOwnedDirect(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	_ = r.Close()
}

func zzOwnedCaptured(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	t.Cleanup(func() { _ = r.Close() })
}

func zzOwnedDeferred(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	defer r.Close()
}

func zzOwnedWrapper(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	wrapped := zzWrap(r)
	t.Cleanup(func() { _ = wrapped.Close() })
}

func zzWrap(r *zzOwnedRes) *zzOwnedWrap { return &zzOwnedWrap{inner: r} }

func zzOwnedHelper(t *testing.T) *zzOwnedRes { return fixtureowner.Own(t, &zzOwnedRes{}) }

func TestZZOwnedReturned(t *testing.T) {
	r := zzOwnedHelper(t)
	defer r.Close()
}

func TestZZOwnedClosureWrapper(t *testing.T) {
	open := func() *zzOwnedWrap { return zzWrap(fixtureowner.Own(t, &zzOwnedRes{})) }
	w := open()
	_ = w.Close()
}

type zzOwnedHolder struct{ res *zzOwnedRes }

func TestZZOwnedField(t *testing.T) {
	h := &zzOwnedHolder{}
	h.res = fixtureowner.Own(t, &zzOwnedRes{})
	_ = h.res.Close()
}

func zzNewHolder(r *zzOwnedRes) *zzOwnedHolder { return &zzOwnedHolder{res: r} }

func TestZZOwnedHeldField(t *testing.T) {
	h := zzNewHolder(fixtureowner.Own(t, &zzOwnedRes{}))
	_ = h.res.Close()
}

func zzOwnedChecked(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func zzOwnedReassigned(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	_ = r
	r = &zzOwnedRes{}
	t.Cleanup(func() { _ = r.Close() })
}

func TestZZOwnedAPIAssertion(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	if r.Close() != nil {
		t.Fatal("close must succeed")
	}
}

func TestZZOwnedCapturedWrapper(t *testing.T) {
	open := func() (*zzOwnedRes, *zzOwnedWrap) {
		r := &zzOwnedRes{}
		fixtureowner.Own(t, r)
		return r, zzWrap(r)
	}
	r, w := open()
	_ = r
	t.Cleanup(func() { _ = w.Close() })
	r, w = open()
	_ = r
}

func TestZZOwnedInitializedCapturedField(t *testing.T) {
	h := &zzOwnedHolder{res: fixtureowner.Own(t, &zzOwnedRes{})}
	t.Cleanup(func() { _ = h.res.Close() })
}

func zzReleaseParameter(r *zzOwnedRes) { _ = r.Close() }

func TestZZOwnedPassedToReleaseHelper(t *testing.T) {
	r := fixtureowner.Own(t, &zzOwnedRes{})
	zzReleaseParameter(r)
}

func TestZZOwnedProductWrapper(t *testing.T) {
	r := fixtureowner.Own(t, &zzProductRes{})
	w := zzProductWrap(r)
	_ = w.Close()
}
`

// plantedOwnedDropLines are the planted release lines that must be members.
var plantedOwnedDropLines = []string{"19:", "24:", "29:", "35:", "44:", "50:", "58:", "65:", "70:", "97:", "104:", "107:", "117:"}

// The wrapper body is product code, not _test.go: teardown-policy traversal
// may stop at it, but resource identity must follow it to the inner Close.
const plantedProductWrapper = `package replication

type zzProductRes struct{}

func (*zzProductRes) Close() error { return nil }

type zzProductWrapper struct{ inner *zzProductRes }

func (w *zzProductWrapper) Close() error { return w.closeInner() }
func (w *zzProductWrapper) closeInner() error { return w.inner.Close() }
func zzProductWrap(r *zzProductRes) *zzProductWrapper { return &zzProductWrapper{inner: r} }
`

// plantedOwnedAmbiguousLine releases a variable that also holds an un-owned value.
const plantedOwnedAmbiguousLine = "79:"

// plantedOwnedLedgerLine is justified by the planted ledger entry.
const plantedOwnedLedgerLine = "84:"

// plantedLedger justifies the planted API assertion and the reassigned
// variable, and carries one stale entry the census must report.
const plantedLedger = "pkg/replication/zz_owned_release_test.go\tgithub.com/orneryd/nornicdb/pkg/replication.TestZZOwnedAPIAssertion\tif r.Close() != nil {\tplanted API assertion\n" +
	"pkg/replication/zz_owned_release_test.go\tgithub.com/orneryd/nornicdb/pkg/replication.zzOwnedReassigned$1\tt.Cleanup(func() { _ = r.Close() })\tplanted: releases the reassigned, never-adopted instance\n" +
	"pkg/replication/zz_owned_release_test.go\tgithub.com/orneryd/nornicdb/pkg/replication.zzGone\t_ = gone.Close()\tplanted stale entry\n"

// plantedFile reports whether a census entry is in one of the files runMatrix
// plants (matched by exact name, not by prefix: the tree has zz_ files too).
func plantedFile(entry string) bool {
	for _, name := range []string{"pkg/replication/zz_intrinsic_adoption_test.go:", "pkg/replication/zz_cleanup_matrix_test.go:", "pkg/replication/zz_owned_release_test.go:", "pkg/replication/zz_owned_product_wrapper.go:", "pkg/replication/zz_owner_delegate_release_test.go:", "internal/testfixture/zz_planted_close.go:", "internal/testfixture/zz_planted_discard.go:", "pkg/cypher/testutil/zz_cleanup_variant.go:", "pkg/cypher/testutil/zz_cleanup_variant_test.go:", "cmd/nornicdb/zz_cleanup_variant_test.go:"} {
		if strings.HasPrefix(entry, name) {
			return true
		}
	}
	return false
}
