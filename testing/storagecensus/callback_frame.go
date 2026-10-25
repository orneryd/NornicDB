package storagecensus

import (
	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// sealedCallbackFrame consumes the engine's generic lifetime facts. A retained
// value is not itself an exemption: every storage effect in its body must lie
// inside the proved guard. Unknown asynchronous work keeps its own entry.
func (a *analysis) sealedCallbackFrame(mc *ssa.MakeClosure) bool {
	if a.callbackFrames == nil {
		a.callbackFrames = map[*ssa.MakeClosure]*workcensus.CallbackFrameProof{}
	}
	proof, seen := a.callbackFrames[mc]
	if !seen {
		proof, _ = workcensus.ProveCallbackFrame(mc)
		a.callbackFrames[mc] = proof
	}
	if proof == nil {
		return false
	}
	fn := mc.Fn.(*ssa.Function)
	// Effect summaries grow during the fixed point. Recheck them every time;
	// caching an early empty summary would hide a later-discovered unsafe use.
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			if a.bodyMarks[ins] && !proof.Covers(ins) {
				return false
			}
			switch ins := ins.(type) {
			case *ssa.Go, *ssa.MakeClosure:
				return false
			case *ssa.Defer:
				callee := ins.Common().StaticCallee()
				if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg.Path() != "sync" || callee.Name() != "Unlock" {
					return false
				}
			case *ssa.Call:
				if _, _, write := badgerWrite(ins.Common()); write && !proof.Covers(ins) {
					return false
				}
				_, builtin := ins.Common().Value.(*ssa.Builtin)
				if ins.Common().StaticCallee() == nil && !builtin && !proof.Covers(ins) {
					return false
				}
				for _, callee := range a.callees(fn, ins) {
					s := a.sums[callee]
					if s != nil && (len(s.reqs) > 0 || len(s.pends) > 0) && !proof.Covers(ins) {
						return false
					}
				}
			}
		}
	}
	return true
}
