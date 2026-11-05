package capturecensus

// Author: storage-format-owners lane (Claude), #1693 takeover, 30 Oct 2026.

import (
	"fmt"
	"go/types"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// ContextSite is one call in the policy package that passes context.Background
// or context.TODO into a function that reaches a ContextOwner. Reason records
// why no request context exists there; a site where one does is a violation.
type ContextSite struct {
	File, Function, Callee, Reason string
	Line                           int
	// Reaches is the first boundary call the fresh context reaches (the
	// call, its target and position) that brought the site into the census.
	Reaches string
}

// NilDisposition is a reviewed exception for in-family nil flows from one
// witness root that the model cannot prove non-nil: why the value is non-nil,
// who approved it, and the exact uses approved (nilWitnessUse keys). A flow
// from the root at any other use fails, and an approved use, or the root,
// that matches no current flow fails as stale.
type NilDisposition struct {
	Reason, Reviewer string
	Uses             []string
}

var witnessPositions = regexp.MustCompile(` \(call at [^)]*\)| at [^ ]*:[0-9]+`)

// nilWitnessRoot keys a nil flow by its origin and first hop, without source
// positions, so an entry survives unrelated line moves.
func nilWitnessRoot(witness []string) string {
	root := witness[0]
	if len(witness) > 1 {
		root += " " + witness[1]
	}
	return strings.TrimSpace(witnessPositions.ReplaceAllString(root, ""))
}

// nilWitnessUse keys one nil flow at one physical call site: the call's
// position (path relative to the module root, line and column), the calling
// function, the target and the whole witness path (its own positions stripped).
// A moved or added call is a new use; it never inherits an approval.
func nilWitnessUse(site, caller, target string, witness []string) string {
	return site + " " + strings.TrimSpace(witnessPositions.ReplaceAllString(caller+" -> "+target+": "+strings.Join(witness, " "), ""))
}

// CrossUnitOwner records a substitution still present in this tree that a
// named merge unit fixes on its own lifetime owner: the unit, the fixing
// change, why, and who approved the record. It is listed and counted as
// KindCrossUnitOwner, never silent, and fails as stale when it matches no
// substitution.
type CrossUnitOwner struct{ Unit, FixingPR, Reason, Approver string }

// ScopeEntry is a ruled census outcome outside Contract A, with its reason.
// Occurrences, when set, limits a tracked defect to those exact fresh contexts
// (heldDescription); any other in the function is judged as usual, and an
// entry none of them still matches fails.
type ScopeEntry struct {
	Reason, Approver string
	Occurrences      []string
}

// covers reports that the entry applies to the fresh context origin.
func (e ScopeEntry) covers(origin ssa.Value) bool {
	if len(e.Occurrences) == 0 {
		return true
	}
	return slices.Contains(e.Occurrences, heldDescription(origin))
}

// ContextSource is the one named context of work whose lifetime belongs to an
// owner rather than to any request, such as the ordered notification drainer.
// Source may be used only on the work Root performs itself (the engine's
// DirectReachableFrom), and that work may use no fresh context. Disposition and Reason label the sites that use it.
type ContextSource struct {
	Source              string
	Root                workcensus.Root
	Disposition, Reason string
}

// contextSites enforces Contract A over every call of the loaded module that
// hands a context to code reaching the boundary (the owners plus every context
// reader of the native engine, nilBoundary):
//   - no nil: a flow the nil analysis reports fails, unless a reviewed nil
//     disposition names its exact use (a forbidden kind, so the census then
//     fails on the count); a non-nil verdict on a value the naive taint can
//     bring nil must carry the analysis's reasons, which are counted;
//   - no substitution: every context.Background or context.TODO that can reach
//     such a call (by the naive taint, through any variable, field or call) is
//     classified where it is created. It is admitted only where no request
//     exists: an exported function without a context, no unexported caller
//     chain reaching a request, the nil branch of the request itself, or a
//     named owned source on its owner's path; a cross-unit owner is listed and
//     counted. Any other creation fails.
//
// Every admission goes through the one acceptance recorder (log).
func contextSites(p *workcensus.Program, policy Policy, log *acceptanceLog, sourceFiles []workcensus.SourceBoundary) (sites []ContextSite, violations, boundary []string, crossings []BoundaryCrossing) {
	if p == nil || len(policy.ContextOwners) == 0 {
		return nil, nil, nil, nil
	}
	pb := newPublicBoundary(p, policy)
	usedDispositions := map[string]bool{}
	usedUses := map[[2]string]bool{}
	useSeen := map[string]bool{}
	syntheticOwners := map[*ssa.Function]string{}
	crossSeen := map[string]bool{}
	scopeSeen := map[string]bool{}
	reviewedSeen := map[string]bool{}
	namedSeen := map[[3]string]bool{}
	staleSites := map[string]bool{}
	staleHolders := map[string]string{}
	digests := newSourceDigests(p)
	digests.sourceFiles = sourceFiles
	violations = append(violations, applyNonNilResultContracts(p, policy, digests)...)
	owners := map[string]bool{}
	for _, owner := range policy.ContextOwners {
		owners[owner] = true
	}
	readers := nilBoundary(p, policy, owners)
	reachesReader := map[*ssa.Function]bool{}
	var readerTargets []*ssa.Function
	for fn := range readers {
		reachesReader[fn] = true
		readerTargets = append(readerTargets, fn)
		boundary = append(boundary, symbolOf(fn))
	}
	sort.Strings(boundary)
	sort.Slice(readerTargets, func(a, b int) bool { return symbolOf(readerTargets[a]) < symbolOf(readerTargets[b]) })
	p.WalkCallers(readerTargets, func(caller, _ *ssa.Function) (bool, bool) {
		reachesReader[caller] = true
		return true, false
	})
	sources := map[string]ContextSource{}
	paths := map[string]map[*ssa.Function]bool{}
	for _, source := range policy.ContextSources {
		sources[source.Source] = source
		population, err := p.DirectReachableFrom([]workcensus.Root{source.Root})
		if err != nil {
			violations = append(violations, source.Source+": "+err.Error())
		}
		paths[source.Source] = population.Functions
	}
	isSourceFn := func(fn *ssa.Function) bool { _, ok := sources[symbolOf(fn)]; return ok }
	graph := newNaiveTaint(p, isSourceFn)
	naiveNil := graph.reach(naiveSources(p, naiveNilSource), false)
	freshOrigins := graph.reach(naiveSources(p, freshContext), true)
	classified := map[ssa.Value]bool{}
	reached := map[ssa.Value]string{}
	sourceUses := map[string]bool{}

	// classify admits or fails one context.Background or context.TODO creation
	// that can reach the boundary.
	classify := func(origin ssa.Value) {
		if classified[origin] {
			return
		}
		classified[origin] = true
		fn := origin.Parent()
		if fn == nil {
			return
		}
		top := workcensus.OutermostFunction(fn)
		pos := p.FileSet.PositionFor(origin.Pos(), false)
		site := ContextSite{File: pos.Filename, Line: pos.Line, Function: symbolOf(top), Callee: originName(origin), Reaches: reached[origin]}
		where := fmt.Sprintf("%s:%d %s creates %s", site.File, site.Line, site.Function, site.Callee)
		key := site.Function
		// Ruled outcomes outside Contract A, each listed and counted.
		if entry, ok := policy.OutOfContract[key]; ok && entry.covers(origin) {
			scopeSeen[key] = true
			log.accept(KindOutsideBoundary, where, "out of contract: "+entry.Reason, entry.Approver, &site)
			sites = append(sites, site)
			return
		}
		if top.Pkg != nil {
			if entry, ok := policy.ExcludedPackages[top.Pkg.Pkg.Path()]; ok {
				scopeSeen[top.Pkg.Pkg.Path()] = true
				log.accept(KindOutsideBoundary, where, "excluded package: "+entry.Reason, entry.Approver, &site)
				sites = append(sites, site)
				return
			}
		}
		if entry, ok := policy.TrackedDefects[key]; ok && entry.covers(origin) {
			scopeSeen[key] = true
			log.accept(KindTrackedDefect, where, entry.Reason, entry.Approver, &site)
			sites = append(sites, site)
			return
		}
		cross, crossRecorded := policy.CrossUnitOwners[key]
		onPath := ""
		for name, path := range paths {
			if path[fn] {
				onPath = name
			}
		}
		instr, _ := origin.(ssa.Instruction)
		// Everything the creating function holds there, except what the
		// exclusions E1-E6 prove holds no request (held.go). A nil branch
		// excludes only the function's own parameters it proves nil.
		roots := creationRoots(origin, fn, log.discharge)
		held := heldValues(fn, nil, roots)
		// A reviewed record clears exactly the held occurrences it is pinned
		// to, and what E4-E6 build only from them; anything else still held
		// fails it. The site is then judged like one holding nothing, its
		// caller walk included. It is validated even when nothing is held now:
		// a pinned value that is gone fails it before it can end any route.
		record, reviewed := policy.ReviewedHeldValues[key]
		stale := ""
		if reviewed {
			reviewedSeen[key] = true
			stale = reviewedHeldStale(p, fn, record, held, digests)
			if rest := heldValues(fn, nil, reviewedRoots(record, roots)); stale == "" && len(rest) > 0 {
				stale = fmt.Sprintf("held beyond the record: %q", rest)
			}
			if stale == "" && len(held) > 0 {
				log.accept(KindReviewedHeld, where, fmt.Sprintf("held %q: %s", held, record.Reason), record.Approver, nil)
				held = nil
			}
		}
		switch {
		case stale != "":
			staleSites[key] = true
			violations = append(violations, where+": reviewed held-value record is stale: "+stale)
		case instr != nil && ownContextNilAt(fn, instr.Block()) && len(held) == 0:
			log.accept(KindNoRequestChain, where, "created on the nil branch of its own context parameter, holding nothing else: no request on this path", "census", &site)
		case onPath != "":
			violations = append(violations, where+": a fresh context on the path of "+sources[onPath].Root.Name+"; use "+onPath)
		case len(held) > 0 && crossRecorded:
			crossSeen[key] = true
			log.accept(KindCrossUnitOwner, where, fmt.Sprintf("a request context is in scope; fixed by %s in merge unit %s: %s", cross.FixingPR, cross.Unit, cross.Reason), cross.Approver, &site)
		case len(held) > 0:
			violations = append(violations, fmt.Sprintf("%s: a request context is in scope; pass it (held: %s) [occurrence %q] [held values: %q; source %s %s]", where, held[0], heldDescription(origin), held, key, digests.of(key)))
		default:
			chain, also, allocated, cut := requestChain(p, top, policy.ReviewedHeldValues[key], policy, pb, crossSeen, reviewedSeen, namedSeen, staleHolders, digests, log, where)
			public := pb.unfulfilled(cut)
			switch {
			case chain == nil && public != nil && crossRecorded:
				// A named merge unit fixes it on its own lifetime owner.
				crossSeen[key] = true
				log.accept(KindCrossUnitOwner, where, fmt.Sprintf("request held at %s across the public API at %s; fixed by %s in merge unit %s: %s", public.Holder, public.Entry, cross.FixingPR, cross.Unit, cross.Reason), cross.Approver, &site)
			case chain == nil && public != nil:
				log.accept(KindUnfulfilled, where, unfulfilledReason(public, policy), policy.DeferredPublicAPI.Approver, &site)
			case chain == nil && allocated:
				// Listed and counted at each allocated holder; never cleared.
				log.accept(KindCrossUnitOwner, where, "a request is held up its caller chains only at holders another merge unit owns", "census (recorded caller allocations)", &site)
			case chain == nil && exported(top):
				log.accept(KindAPIBoundary, where, "exported API without a request context parameter, which no caller holding one in the module reaches", "census", &site)
			case chain == nil:
				log.accept(KindNoRequestChain, where, "no request context in scope or up its caller chains in the module", "census", &site)
			case crossRecorded:
				crossSeen[key] = true
				log.accept(KindCrossUnitOwner, where, fmt.Sprintf("context at %s; fixed by %s in merge unit %s: %s", strings.Join(chain, " <- "), cross.FixingPR, cross.Unit, cross.Reason), cross.Approver, &site)
			default:
				held := ""
				if len(also) > 0 {
					held = " (also held at: " + strings.Join(also, " | ") + ")"
				}
				routes := holderRoutes(p, top, policy, pb, digests, where)
				violations = append(violations, where+": a request context exists up the caller chain "+strings.Join(chain, " <- ")+held+"; thread it [routes: "+strings.Join(routes, " || ")+"]")
			}
		}
		sites = append(sites, site)
	}

	for _, fn := range p.Functions {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				callee := call.Common().StaticCallee()
				target, inFamily := "a dynamic call", false
				if callee != nil {
					target = symbolOf(callee)
				}
				for _, candidate := range p.CallTargets(call) {
					if reachesReader[candidate] {
						inFamily = true
						if callee == nil {
							target = "a dynamic call to " + symbolOf(candidate)
						}
					}
				}
				// A call site whose work another unit's owner takes over, where
				// no context is passed, is an owned source of that unit: listed
				// and counted, never disposed.
				if callee != nil {
					siteKey := symbolOf(workcensus.OutermostFunction(fn)) + " -> " + target
					if cross, ok := policy.CrossUnitOwners[siteKey]; ok {
						pos := p.FileSet.PositionFor(call.Pos(), false)
						where := fmt.Sprintf("%s:%d %s", pos.Filename, pos.Line, siteKey)
						crossSeen[siteKey] = true
						log.accept(KindOwnedSource, where, "work owned by merge unit "+cross.Unit, cross.Approver, nil)
						log.accept(KindCrossUnitOwner, where, fmt.Sprintf("lifetime to become %s's owner context when merge unit %s lands: %s", cross.FixingPR, cross.Unit, cross.Reason), cross.Approver, nil)
					}
				}
				for argument, arg := range call.Common().Args {
					for _, fieldPath := range workcensus.InlineFieldPaths(arg.Type(), isContext) {
						inputValues := []ssa.Value{arg}
						var unknownField *types.Var
						var witness, why []string
						suffix := ""
						if len(fieldPath) == 0 {
							witness, why = p.MayBeNilWhy(arg, call)
						} else {
							projected := p.ProjectFieldAt(arg, fieldPath, call)
							inputValues = projected.Values
							if projected.Unknown {
								unknownField = projected.Field
							}
							witness, why = projected.Witness, projected.Why
							if witness != nil && p.GuardedInlineFieldUses(arg, fieldPath, projected) {
								witness = nil
								why = []string{"proof: private inline field has only sibling-guarded, non-nil semantic uses"}
							}
							suffix = fmt.Sprintf("/argument-%d/field-%v(%s)", argument, fieldPath, workcensus.InlineFieldName(arg.Type(), fieldPath))
						}
						pos := p.FileSet.PositionFor(call.Pos(), false)
						caller := symbolOf(workcensus.OutermostFunction(fn))
						// Every fresh context the naive taint brings to a call in the
						// family is classified, whatever the nil verdict on the same
						// argument: a nil finding, its disposition or its cross-unit
						// record never stands in for a substitution.
						if inFamily {
							if unknownField != nil {
								for _, origin := range freshOrigins[unknownField] {
									if _, seen := reached[origin]; !seen {
										reached[origin] = fmt.Sprintf("%s -> %s at %s:%d%s (unknown field provenance)", caller, target, pos.Filename, pos.Line, suffix)
									}
									classify(origin)
								}
							}
							for _, input := range inputValues {
								for _, origin := range freshOrigins[input] {
									if _, seen := reached[origin]; !seen {
										reached[origin] = fmt.Sprintf("%s -> %s at %s:%d", caller, target, pos.Filename, pos.Line)
									}
									classify(origin)
								}
							}
						}
						if witness != nil {
							finding := fmt.Sprintf("%s:%d %s -> %s: a nil context can reach it: %s", pos.Filename, pos.Line, caller, target, strings.Join(witness, " ")) + suffix
							if !inFamily {
								log.accept(KindOutsideBoundary, finding, "reaches no context reader", "census", nil)
								continue
							}
							root := nilWitnessRoot(witness)
							site := nilContextSite(p, call, argument, syntheticOwners) + suffix
							use := nilWitnessUse(site, caller, target, witness)
							if useSeen[use] {
								violations = append(violations, "nil-context use key is not unique to one call site: "+use)
							}
							useSeen[use] = true
							// A nil whose lifetime another unit's owner takes over is an
							// owned source of that unit: listed and counted, never disposed.
							if cross, ok := policy.CrossUnitOwners[root]; ok {
								crossSeen[root] = true
								log.accept(KindOwnedSource, finding, "nil lifetime owned by merge unit "+cross.Unit, cross.Approver, nil)
								log.accept(KindCrossUnitOwner, finding, fmt.Sprintf("lifetime to become %s's owner context when merge unit %s lands: %s", cross.FixingPR, cross.Unit, cross.Reason), cross.Approver, nil)
								continue
							}
							if detached, ok := policy.NilDetached[root]; ok && slices.Contains(detached.Uses, use) {
								usedUses[[2]string{root, use}] = true
								log.accept(KindNilDetached, finding+" [root: "+root+"]", detached.Reason, detached.Reviewer, nil)
								continue
							}
							d, ok := policy.NilContextDispositions[root]
							switch {
							case !ok:
								violations = append(violations, finding+" [witness root: "+root+"; use: "+use+"]")
							case !slices.Contains(d.Uses, use):
								usedDispositions[root] = true
								violations = append(violations, finding+" [witness root: "+root+"; use not approved by its disposition; use: "+use+"]")
							default:
								usedDispositions[root] = true
								usedUses[[2]string{root, use}] = true
								log.accept(KindNilDisposition, finding+" [disposition: "+d.Reason+"; reviewer "+d.Reviewer+"]", d.Reason, d.Reviewer, nil)
							}
							continue
						}
						if !inFamily {
							continue
						}
						// Reconciliation: the naive taint can bring nil here; the
						// analysis's non-nil verdict must say why.
						if _, naive := naiveNil[arg]; naive {
							if len(why) == 0 {
								violations = append(violations, fmt.Sprintf("%s:%d %s -> %s: the naive taint can bring nil here and the nil analysis judged it non-nil without a recorded reason", pos.Filename, pos.Line, caller, target))
							}
							for _, rule := range why {
								log.discharge(rule)
							}
						}
						for _, input := range inputValues {
							named, isSource := sourceOf(input, sources)
							sourceKey := fmt.Sprintf("%d%s/%s", call.Pos(), suffix, named.Source)
							if isSource && !sourceUses[sourceKey] {
								sourceUses[sourceKey] = true
								where := fmt.Sprintf("%s:%d %s -> %s", pos.Filename, pos.Line, caller, target) + suffix
								fromSource := func(v ssa.Value) bool { _, ok := sourceOf(v, sources); return ok }
								if !paths[named.Source][fn] {
									violations = append(violations, where+": "+named.Source+" used outside the path of its owner "+named.Root.Name)
								} else if held := heldRequest(fn, nil, fromSource); held != "" {
									// The owner's context stands in only where no request is
									// held: one in scope is the context to pass.
									violations = append(violations, where+": "+named.Source+" used where a request context is in scope; pass it (held: "+held+")")
								} else {
									site := ContextSite{File: pos.Filename, Line: pos.Line, Function: caller, Callee: target}
									log.accept(KindOwnedSource, where, named.Disposition+": "+named.Reason, "census", &site)
									sites = append(sites, site)
									// A source whose lifetime moves to another unit's owner is
									// also listed and counted as cross-unit.
									if cross, ok := policy.CrossUnitOwners[named.Source]; ok {
										crossSeen[named.Source] = true
										log.accept(KindCrossUnitOwner, where, fmt.Sprintf("lifetime to become %s's owner context when merge unit %s lands: %s", cross.FixingPR, cross.Unit, cross.Reason), cross.Approver, nil)
									}
								}
							}
						}
					}
				}
			}
		}
	}
	for root, d := range policy.NilContextDispositions {
		if !usedDispositions[root] {
			violations = append(violations, "nil-context disposition "+root+" matches no current flow")
		}
		if len(d.Uses) == 0 {
			violations = append(violations, "nil-context disposition "+root+" names no approved use")
		}
		for _, use := range d.Uses {
			if !usedUses[[2]string{root, use}] {
				violations = append(violations, "nil-context disposition "+root+" approved use matches no current flow: "+use)
			}
		}
	}
	// An excluded package holds no product request only while no product
	// package imports it: the census checks that premise on every load.
	importers := map[string]bool{}
	for _, fn := range p.Functions {
		if fn.Pkg == nil || importers[fn.Pkg.Pkg.Path()] {
			continue
		}
		importers[fn.Pkg.Pkg.Path()] = true
		if _, excluded := policy.ExcludedPackages[fn.Pkg.Pkg.Path()]; excluded {
			continue
		}
		for _, imported := range fn.Pkg.Pkg.Imports() {
			if _, excluded := policy.ExcludedPackages[imported.Path()]; excluded {
				violations = append(violations, "excluded package "+imported.Path()+" is imported by "+fn.Pkg.Pkg.Path()+": it can hold that package's request")
			}
		}
	}
	for _, scope := range []map[string]ScopeEntry{policy.OutOfContract, policy.ExcludedPackages, policy.TrackedDefects} {
		for key := range scope {
			if !scopeSeen[key] {
				violations = append(violations, key+": a ruled contract-scope entry matches no context site")
			}
		}
	}
	for holder, why := range staleHolders {
		violations = append(violations, holder+": reviewed record of a holder is stale: "+why)
	}
	for site, record := range policy.ReviewedHeldValues {
		if stale := staleSites[site]; stale {
			continue
		}
		for _, edge := range append(append([]RouteEdge(nil), record.EndsEdges...), record.TrackedHolders...) {
			if !namedSeen[[3]string{site, edge.Caller, edge.Callee}] {
				violations = append(violations, site+": named edge matches no walked edge: "+edge.Caller+" -> "+edge.Callee)
			}
		}
	}
	for root, detached := range policy.NilDetached {
		for _, use := range detached.Uses {
			if !usedUses[[2]string{root, use}] {
				violations = append(violations, "detached nil use matches no flow: "+root+": "+use)
			}
		}
	}
	for key := range policy.ReviewedHeldValues {
		if !reviewedSeen[key] {
			violations = append(violations, key+": reviewed held-value record matches no site holding a context")
		}
	}
	for key, cross := range policy.CrossUnitOwners {
		if !crossSeen[key] {
			violations = append(violations, fmt.Sprintf("%s: cross-unit owner %s (%s) matches no substitution", key, cross.FixingPR, cross.Unit))
		}
	}
	sort.Slice(sites, func(a, b int) bool {
		if sites[a].File != sites[b].File {
			return sites[a].File < sites[b].File
		}
		return sites[a].Line < sites[b].Line
	})
	crossings = pb.all()
	unfulfilled := len(log.keys(KindUnfulfilled))
	switch {
	case unfulfilled > 0 && policy.DeferredPublicAPI.Reason == "":
		violations = append(violations, fmt.Sprintf("%d substitutions reached only across the public API have no tracked deferral", unfulfilled))
	case unfulfilled == 0 && policy.DeferredPublicAPI.Reason != "":
		violations = append(violations, "the tracked public API deferral matches no substitution")
	}
	return sites, violations, boundary, crossings
}

// originName names a fresh-context creation: context.Background or context.TODO.
func originName(origin ssa.Value) string {
	if conv, ok := origin.(*ssa.MakeInterface); ok {
		origin = conv.X
	}
	if call, ok := origin.(*ssa.Call); ok {
		if callee := call.Common().StaticCallee(); callee != nil {
			return "context." + callee.Name()
		}
	}
	return origin.String()
}

// nilBoundary returns the owners plus every function of the policy package,
// reachable from a method of the native type (the engine's walk, callbacks
// included), that reads a context parameter itself.
func nilBoundary(p *workcensus.Program, policy Policy, owners map[string]bool) map[*ssa.Function]bool {
	boundary := map[*ssa.Function]bool{}
	var roots []workcensus.Root
	for _, fn := range p.Functions {
		if owners[symbolOf(fn)] {
			boundary[fn] = true
		}
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == policy.Package && receiverNamed(fn, policy.Native) {
			roots = append(roots, workcensus.Root{Package: policy.Package, Name: fn.Name(), Receiver: policy.Native})
		}
	}
	native, err := p.ReachableFrom(roots)
	if err != nil {
		return boundary
	}
	for fn := range native.Functions {
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == policy.Package && readsContext(fn, policy.Package) {
			boundary[fn] = true
		}
	}
	return boundary
}

// receiverNamed reports a method whose receiver (or its pointer) is the named
// type.
func receiverNamed(fn *ssa.Function, name string) bool {
	recv := fn.Signature.Recv()
	if recv == nil || name == "" {
		return false
	}
	t := recv.Type()
	if ptr, ok := t.Underlying().(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := types.Unalias(t).(*types.Named)
	return ok && named.Obj().Name() == name
}

// readsContext reports a function that uses one of its context parameters
// itself. Every use counts except a nil comparison, a debug reference and
// passing it on to a static callee of the same package (whose own use is
// judged there): a method call on it, handing it to other code, storing or
// capturing it.
func readsContext(fn *ssa.Function, pkg string) bool {
	for _, param := range fn.Params {
		if !isContext(param.Type()) || param.Referrers() == nil {
			continue
		}
		for _, ref := range *param.Referrers() {
			switch ref := ref.(type) {
			case *ssa.BinOp, *ssa.DebugRef:
				continue
			case ssa.CallInstruction:
				common := ref.Common()
				if !common.IsInvoke() || common.Value != param {
					if callee := common.StaticCallee(); callee != nil && callee.Pkg != nil && callee.Pkg.Pkg.Path() == pkg {
						continue
					}
				}
			}
			return true
		}
	}
	return false
}

// unfulfilledReason states a substitution reached only across the public API:
// the request holder beyond it, the crossing, and the tracked deferral.
func unfulfilledReason(c *BoundaryCrossing, policy Policy) string {
	return fmt.Sprintf("Contract A unfulfilled: request held at %s reaches it across the public API at %s (%s); deferred: %s", c.Holder, c.Entry, c.Via, policy.DeferredPublicAPI.Reason)
}

func symbolOf(fn *ssa.Function) string {
	if origin := fn.Origin(); origin != nil {
		fn = origin
	}
	if obj, ok := fn.Object().(*types.Func); ok {
		return obj.FullName()
	}
	return fn.String()
}

func exported(fn *ssa.Function) bool {
	obj, ok := fn.Object().(*types.Func)
	return ok && obj.Exported()
}

// requestChain walks caller chains upward from fn across the module (the
// engine's WalkCallers) and returns the first chain, fn first, that reaches a
// function holding a request context, or nil, and every other holder the
// complete walk finds, so one report lists them all. A holder whose lifetime another
// merge unit takes over is listed and counted for that unit, reported as
// allocated, and walked past like any other caller: it hides no holder above
// it. The walk stops only at the policy package's exported API entered from
// outside it (publicCall): that API takes no request context, the
// tracked deferral for 1.4.4, not shown to carry no request. Each such cut is
// recorded in pb with any request holder beyond it, and returned, so a
// substitution reached only across that API is listed as unfulfilled
// Contract A, never accepted. Every other edge is followed, in any package:
// a request holder there substitutes the fresh context. A holder whose held
// contexts are exactly those of its own current reviewed record is listed,
// and walked past too.
func requestChain(p *workcensus.Program, fn *ssa.Function, site ReviewedHeldValue, policy Policy, pb *publicBoundary, crossSeen, reviewedSeen map[string]bool, namedSeen map[[3]string]bool, staleHolders map[string]string, digests *sourceDigests, log *acceptanceLog, where string) ([]string, []string, bool, []*BoundaryCrossing) {
	next := map[*ssa.Function]*ssa.Function{}
	// chainVia is the chain through this edge; next keeps one route per
	// function, the first found, so it cannot cycle.
	chainVia := func(caller, callee *ssa.Function) []string {
		chain := []string{symbolOf(caller)}
		for step := callee; step != nil; step = next[step] {
			chain = append([]string{symbolOf(step)}, chain...)
		}
		return chain
	}
	var first []string
	var also []string
	seenAlso := map[string]bool{}
	var cut []*BoundaryCrossing
	allocated := false
	// The one decision about an edge, from its own calls. An edge with a private
	// call is walked, and its public calls are still recorded.
	p.WalkCallerEdges([]*ssa.Function{fn}, func(caller, callee *ssa.Function, paths []workcensus.CallPath) (bool, bool) {
		if excludedPackage(caller, policy) {
			return false, false
		}
		public := 0
		for _, path := range paths {
			if publicCall(path, caller, callee, policy) {
				public++
			}
		}
		if public > 0 {
			cut = append(cut, pb.cross(caller, callee))
			if public == len(paths) {
				return false, false
			}
		}
		if _, seen := next[caller]; !seen {
			next[caller] = callee
		}
		key, calleeKey := symbolOf(caller), symbolOf(callee)
		// A named edge, from the site's record, which classify validated before
		// walking. It ends the route only on a body boundary, or where the census
		// finds the edge's calls supply a context.
		if edge, ok := routeEdge(site.EndsEdges, key, calleeKey); ok {
			namedSeen[[3]string{symbolOf(fn), key, calleeKey}] = true
			if edge.Kind == BodyBoundary || suppliesContext(p, paths) {
				log.accept(KindReviewedHolder, where, fmt.Sprintf("route ends at %s -> %s: %s", key, calleeKey, edge.Reason), site.Approver, nil)
				return false, false
			}
		}
		if pb.toward(caller, callee) == "" {
			return true, false
		}
		if edge, ok := routeEdge(site.TrackedHolders, key, calleeKey); ok {
			namedSeen[[3]string{symbolOf(fn), key, calleeKey}] = true
			log.accept(KindTrackedDefect, where+" held at "+key+" -> "+calleeKey, edge.Reason, site.Approver, nil)
			return true, false
		}
		record, recorded := policy.ReviewedHeldValues[key]
		why := ""
		if recorded {
			reviewedSeen[key] = true
			why = reviewedHolder(p, caller, callee, record, digests)
			if why != "" {
				staleHolders[key] = why
			}
		}
		if recorded && why == "" {
			log.accept(KindReviewedHolder, where, fmt.Sprintf("held at %s (%s): %s", key, strings.Join(chainVia(caller, callee), " <- "), record.Reason), record.Approver, nil)
			return true, false
		}
		cross, ok := policy.CrossUnitOwners[key]
		if !ok {
			chain := chainVia(caller, callee)
			if first == nil {
				first = chain
			} else if joined := strings.Join(chain, " <- "); !seenAlso[joined] {
				seenAlso[joined] = true
				also = append(also, joined)
			}
			return true, false
		}
		crossSeen[key] = true
		allocated = true
		log.accept(KindCrossUnitOwner, where, fmt.Sprintf("request held at %s (%s); lifetime to become %s's owner context when merge unit %s lands: %s", key, strings.Join(chainVia(caller, callee), " <- "), cross.FixingPR, cross.Unit, cross.Reason), cross.Approver, nil)
		return true, false
	})
	return first, also, allocated, cut
}

// holderRoutes lists, for each direct caller of a reported site, every holder
// on or above that caller, each with one chain, so a reviewer can check every
// route of the report. The walks record nothing: they only read.
func holderRoutes(p *workcensus.Program, fn *ssa.Function, policy Policy, pb *publicBoundary, digests *sourceDigests, where string) []string {
	record := policy.ReviewedHeldValues[symbolOf(fn)]
	seen := map[*ssa.Function]bool{}
	var routes []string
	for _, site := range p.SourceCallSites(fn) {
		caller := workcensus.OutermostFunction(site.Parent())
		_, ended := routeEdge(record.EndsEdges, symbolOf(caller), symbolOf(fn))
		if seen[caller] || excludedPackage(caller, policy) || ended {
			continue
		}
		seen[caller] = true
		var holders []string
		if pb.toward(caller, fn) != "" {
			holders = append(holders, "held here")
		}
		chain, also, _, _ := requestChain(p, caller, record, policy, pb, map[string]bool{}, map[string]bool{}, map[[3]string]bool{}, map[string]string{}, digests, &acceptanceLog{}, where)
		if chain != nil {
			holders = append(append(holders, strings.Join(chain, " <- ")), also...)
		}
		routes = append(routes, symbolOf(caller)+": "+strings.Join(holders, " | "))
	}
	sort.Strings(routes)
	return routes
}

// sourceOf reports a context produced by a call to a named ContextSource.
func sourceOf(v ssa.Value, sources map[string]ContextSource) (ContextSource, bool) {
	call, ok := v.(*ssa.Call)
	if !ok {
		return ContextSource{}, false
	}
	callee := call.Common().StaticCallee()
	if callee == nil {
		return ContextSource{}, false
	}
	source, ok := sources[symbolOf(callee)]
	return source, ok
}

func isContext(t types.Type) bool { return named(t, "context", "Context") }

// freshContext reports a context created by context.Background or context.TODO.
func freshContext(v ssa.Value) bool {
	if conv, ok := v.(*ssa.MakeInterface); ok {
		v = conv.X
	}
	call, ok := v.(*ssa.Call)
	if !ok {
		return false
	}
	callee := call.Common().StaticCallee()
	return callee != nil && callee.Pkg != nil && callee.Pkg.Pkg.Path() == "context" && (callee.Name() == "Background" || callee.Name() == "TODO")
}

// publicCall reports that this call path of an edge is a call of the deferred
// public API, from the call itself: a call into a public package's exported
// function from elsewhere, or by any but a direct call; or, for an implementation
// elsewhere, an interface call, or a method value (a $bound wrapper on the path),
// on an interface a public package declares, whose method has the callee's name
// and signature.
func publicCall(path workcensus.CallPath, caller, callee *ssa.Function, policy Policy) bool {
	if !exported(callee) {
		return false
	}
	common := path.Site.Common()
	if callee.Pkg != nil && (publicPackage(callee.Pkg.Pkg.Path(), policy) || publicEntry(callee, policy)) {
		direct := common.StaticCallee() != nil && len(path.Via) == 0
		return caller.Pkg == nil || caller.Pkg.Pkg.Path() != callee.Pkg.Pkg.Path() || !direct
	}
	if common.IsInvoke() {
		return common.Method.Name() == callee.Name() && types.Identical(common.Method.Type(), callee.Signature) && policyInterface(common.Value.Type(), policy)
	}
	receiver, bound := path.BoundReceiver(callee.Name())
	return bound && policyInterface(receiver, policy) && methodSignature(receiver, callee)
}

// policyInterface reports an interface type a public package declares.
func policyInterface(t types.Type, policy Policy) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil || !publicPackage(named.Obj().Pkg().Path(), policy) {
		return false
	}
	_, isInterface := named.Underlying().(*types.Interface)
	return isInterface
}

// methodSignature reports that receiver's method named as callee has its
// signature.
func methodSignature(receiver types.Type, callee *ssa.Function) bool {
	object, _, _ := types.LookupFieldOrMethod(receiver, true, nil, callee.Name())
	if object == nil && callee.Object() != nil {
		object, _, _ = types.LookupFieldOrMethod(receiver, true, callee.Object().Pkg(), callee.Name())
	}
	method, ok := object.(*types.Func)
	return ok && types.Identical(method.Type(), callee.Signature)
}

// suppliesContext reports that every call of the edge passes a context the nil
// analysis cannot prove may be nil, and at least one.
func suppliesContext(p *workcensus.Program, paths []workcensus.CallPath) bool {
	if len(paths) == 0 {
		return false
	}
	for _, path := range paths {
		supplied := workcensus.SuppliedContexts(path.Site, isContext)
		if len(supplied) == 0 {
			return false
		}
		for _, value := range supplied {
			if witness, _ := p.MayBeNilWhy(value, path.Site); witness != nil {
				return false
			}
		}
	}
	return true
}

// publicPackage reports the policy package or a listed public package.
func publicPackage(path string, policy Policy) bool {
	if path == policy.Package {
		return true
	}
	for _, public := range policy.PublicPackages {
		if path == public {
			return true
		}
	}
	return false
}

// excludedPackage reports a caller in a package outside the product contract.
func excludedPackage(fn *ssa.Function, policy Policy) bool {
	if fn.Pkg == nil {
		return false
	}
	_, ok := policy.ExcludedPackages[fn.Pkg.Pkg.Path()]
	return ok
}

// publicEntry reports an exported function listed by symbol as a public
// entry (PublicEntries): its package is not a public package, but this one
// function is the deferred boundary.
func publicEntry(fn *ssa.Function, policy Policy) bool {
	symbol := symbolOf(fn)
	for _, entry := range policy.PublicEntries {
		if symbol == entry {
			return true
		}
	}
	return false
}
