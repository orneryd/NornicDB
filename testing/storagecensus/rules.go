package storagecensus

import (
	"fmt"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// dispositionOwners are where a hidden-ID disposition is decided: whether a
// write over a stored (possibly hidden) entity inserts, updates, is a
// no-op or is rejected. Every other decision on hidden state that gates a
// write is a second interpretation of the same transition.
var dispositionOwners = map[string]string{
	"pkg/storage.BadgerTransaction": "the transaction commit: the owner of the write transition (staging, validation against the stored image, commit)",
}

// decisionOwners are functions outside the transaction that decide on hidden
// state by design, each with the reason; every row names a function that
// exists and still decides.
var decisionOwners = map[string]string{
	"pkg/storage.BadgerEngine.evaluateEdgeSuppressionInTxn": "the decay-suppression owner: decides from the stored relationship whether decay hides it, and rewrites its body and adjacency when that changes",
	"pkg/storage.BadgerEngine.forgetManualSuppression":      "the manual-suppression owner: a deleted entity's on-demand suppression ends only when it has one",
	"pkg/storage.BadgerEngine.markNewTemporalConstraint":    "decides, under its maintenance write, whether the label has stored nodes; hidden ones count, as they keep their history",
}

// decisionOwnerOnPath is the decision owner the path from a decision to its
// write passes through, or "": the branch is that owner's, reached from the
// function the census reports.
func decisionOwnerOnPath(d Decision) string {
	for _, owner := range sortedKeys(decisionOwners) {
		if strings.Contains(d.Gates, owner+" ") {
			return owner
		}
	}
	return ""
}

// ownsDisposition reports whether fn is a method (or a closure of one) of a
// disposition-owner type, or a decision owner.
func ownsDisposition(fn string) bool {
	if decisionOwners[fn] != "" || decisionOwners[outer(fn)] != "" {
		return true
	}
	for owner := range dispositionOwners {
		if strings.HasPrefix(fn, owner+".") {
			return true
		}
	}
	return false
}

// pendingConvergence are second interpretations of the write transition
// that the stacked change converging every write into one owner removes.
// Each row must name code that still exists: a row whose code is gone is
// stale and must be deleted, so the list only shrinks.
var pendingConvergence = map[string]string{
	"pkg/multidb.databaseLimitChecker.CheckStorageLimits": "sizes an update's prior through a visible read; the accounting batch over stored images replaces it (#1406)",
	"pkg/multidb.sizeTrackingEngine.CreateNode":           "re-derives the hidden-ID create policy for quota; managed writes stage through the transaction commit (#1406)",
	"pkg/multidb.sizeTrackingEngine.createEdge":           "re-derives the hidden-ID create policy for quota; managed writes stage through the transaction commit (#1406)",
	"pkg/multidb.sizeTrackingEngine.UpdateNode":           "sizes the prior and the result itself; managed writes stage through the transaction commit (#1406)",
	"pkg/multidb.sizeTrackingEngine.UpdateEdge":           "sizes the prior and the result itself; managed writes stage through the transaction commit (#1406)",
	"pkg/multidb.sizeTrackingEngine.DeleteNode":           "sizes the deleted entity itself; managed writes stage through the transaction commit (#1406)",
	"pkg/multidb.sizeTrackingEngine.DeleteEdge":           "sizes the deleted entity itself; managed writes stage through the transaction commit (#1406)",
}

// rawEngineFields hold an engine that sees every database: a count read
// through one is a total across databases, never one database's usage.
var rawEngineFields = map[string]bool{
	"pkg/multidb.DatabaseManager.inner": true,
}

// countMethods are the storage.Engine methods that read a count.
var countMethods = map[string]bool{"NodeCount": true, "EdgeCount": true, "NodeCountByLabel": true, "EdgeCountByType": true}

// visibleReads are the storage.Engine methods that hide what visibility
// hides: a prior read through one misses hidden entities. In pkg/multidb,
// the quota layer, they are allowed only where nothing is accounted.
var visibleReads = map[string]bool{"GetNode": true, "GetEdge": true}

var visibleReaders = map[string]string{
	"pkg/multidb.DatabaseManager.loadMetadata":        "reads the system database's metadata node",
	"pkg/multidb.DatabaseManager.persistMetadata":     "reads the system database's metadata node",
	"pkg/multidb.DatabaseManager.isMigrationComplete": "reads the system database's migration status node",
	"pkg/multidb.sizeTrackingEngine.GetQueryNode":     "forwards a query's read; accounts nothing",
}

func (a *analysis) pending(fn string) bool {
	return pendingConvergence[fn] != "" || pendingConvergence[outer(fn)] != ""
}

// checkRules applies the rules on decisions, quota priors, unscoped totals
// and unresolved write callbacks.
func (a *analysis) checkRules(r *Report, names map[string]*ssa.Function) {
	used := map[string]bool{}
	note := func(fn string) bool {
		if pendingConvergence[fn] != "" {
			used[fn] = true
			return true
		}
		if pendingConvergence[outer(fn)] != "" {
			used[outer(fn)] = true
			return true
		}
		return false
	}
	for _, d := range r.Decisions {
		if ownsDisposition(d.Func) || note(d.Func) || decisionOwnerOnPath(d) != "" {
			continue
		}
		r.Violations = append(r.Violations, fmt.Sprintf("hidden decision outside the disposition owner: %s at %s (decides %s)", d.Func, d.Pos, d.Gates))
	}
	for _, fn := range a.p.funcs {
		name := a.p.funcName(fn)
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				c := call.Common()
				// A count read through an engine that sees every database.
				if c.IsInvoke() && countMethods[c.Method.Name()] && a.fromRawEngine(c.Value) && !note(name) {
					r.Violations = append(r.Violations, fmt.Sprintf("unscoped total: %s at %s reads %s through an engine that sees every database", name, a.p.pos(ins.Pos()), c.Method.Name()))
				}
				// A visible read in the quota layer. A promoted method's
				// wrapper only forwards: its callers are checked instead.
				if fn.Synthetic == "" && a.visibleRead(c) && strings.HasPrefix(name, "pkg/multidb.") &&
					visibleReaders[name] == "" && visibleReaders[outer(name)] == "" && !note(name) {
					r.Violations = append(r.Violations, fmt.Sprintf("visible read in quota accounting: %s at %s reads through %s, which misses hidden entities", name, a.p.pos(ins.Pos()), c.Method.Name()))
				}
				// A write callback the call graph cannot resolve.
				_, builtin := c.Value.(*ssa.Builtin)
				if !builtin && c.StaticCallee() == nil && passesWriter(c) && len(a.dynamicCallees(fn, call)) == 0 && !a.paramValue(c.Value) && !note(name) {
					r.Violations = append(r.Violations, fmt.Sprintf("unresolved write callback: %s at %s calls a function value that receives a write transaction and that nothing in the module provides", name, a.p.pos(ins.Pos())))
				}
			}
		}
	}
	for _, fn := range sortedKeys(pendingConvergence) {
		if names[fn] == nil {
			r.Violations = append(r.Violations, "stale: pending-convergence row "+fn+" names no function: delete it")
		} else if !used[fn] {
			r.Violations = append(r.Violations, "stale: pending-convergence row "+fn+" no longer interprets the transition: delete it")
		}
		r.Pending = append(r.Pending, NamedReason{fn, pendingConvergence[fn]})
	}
	for _, fn := range sortedKeys(visibleReaders) {
		if names[fn] == nil {
			r.Violations = append(r.Violations, "stale: visible reader "+fn+" does not exist")
		}
	}
	decided := map[string]bool{}
	for _, d := range r.Decisions {
		decided[d.Func] = true
		decided[outer(d.Func)] = true
		decided[decisionOwnerOnPath(d)] = true
	}
	for _, fn := range sortedKeys(decisionOwners) {
		if names[fn] == nil {
			r.Violations = append(r.Violations, "stale: decision owner "+fn+" does not exist")
		} else if !decided[fn] {
			r.Violations = append(r.Violations, "stale: decision owner "+fn+" decides nothing on hidden state: delete it")
		}
		r.Disposition = append(r.Disposition, NamedReason{fn, decisionOwners[fn]})
	}
	for _, owner := range sortedKeys(dispositionOwners) {
		exists := false
		for name := range names {
			exists = exists || strings.HasPrefix(name, owner+".")
		}
		if !exists {
			r.Violations = append(r.Violations, "stale: disposition owner "+owner+" has no methods")
		}
		r.Disposition = append(r.Disposition, NamedReason{owner, dispositionOwners[owner]})
	}
}

// fromRawEngine reports whether v is loaded from a field that holds an
// engine seeing every database.
func (a *analysis) fromRawEngine(v ssa.Value) bool {
	for i := 0; i < 8; i++ {
		switch x := v.(type) {
		case *ssa.UnOp:
			if fa, ok := x.X.(*ssa.FieldAddr); ok {
				name, _ := a.p.field(fa.X.Type().Underlying().(*types.Pointer).Elem(), fa.Field)
				return rawEngineFields[name]
			}
			return false
		case *ssa.ChangeType:
			v = x.X
		case *ssa.MakeInterface:
			v = x.X
		default:
			return false
		}
	}
	return false
}

// visibleRead reports a call of an engine's visible read: through the
// storage.Engine interface, or a method of a storage type.
func (a *analysis) visibleRead(c *ssa.CallCommon) bool {
	if c.IsInvoke() {
		return visibleReads[c.Method.Name()]
	}
	callee := c.StaticCallee()
	if callee == nil || !visibleReads[callee.Name()] || callee.Signature.Recv() == nil {
		return false
	}
	// A wrapper of a promoted method reads through the embedded engine.
	return strings.HasPrefix(a.p.funcName(callee), "pkg/storage.") || strings.HasPrefix(callee.Synthetic, "wrapper for")
}

// passesWriter reports whether a call passes a Badger write transaction or
// batch.
func passesWriter(c *ssa.CallCommon) bool {
	for _, arg := range c.Args {
		t := types.TypeString(arg.Type(), nil)
		if strings.HasSuffix(t, "dgraph-io/badger/v4.Txn") || strings.HasSuffix(t, "dgraph-io/badger/v4.WriteBatch") {
			return true
		}
	}
	return false
}

// dynamicCallees are the call graph's callees of a dynamic call.
func (a *analysis) dynamicCallees(fn *ssa.Function, site ssa.CallInstruction) []*ssa.Function {
	node := a.p.cg.Nodes[fn]
	if node == nil {
		return nil
	}
	var out []*ssa.Function
	for _, e := range node.Out {
		if e.Site == site && e.Callee.Func != nil {
			out = append(out, e.Callee.Func)
		}
	}
	sort.Slice(out, func(i, j int) bool { return a.p.funcName(out[i]) < a.p.funcName(out[j]) })
	return out
}

// paramValue reports whether a called function value is a parameter or a
// captured variable: it is accounted where the function is made.
func (a *analysis) paramValue(v ssa.Value) bool {
	switch v.(type) {
	case *ssa.Parameter, *ssa.FreeVar:
		return true
	}
	return false
}
