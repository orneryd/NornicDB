package workcensus

import (
	"fmt"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// NonNilResultContract is a client's reviewed assertion about a normal return,
// not an inferred theorem. The client must validate its source and writer
// evidence before registration. It affects only nil-flow result traversal;
// provenance, field loads and request-substitution analysis remain unchanged.
type NonNilResultContract struct {
	Function *ssa.Function
	Index    int
	Evidence string
}

// SetNonNilResultContracts atomically replaces the reviewed result boundaries
// for this exact Program. Invalid identities or result indices leave the
// existing set untouched. Registration resets nil-flow memoized verdicts.
func (p *Program) SetNonNilResultContracts(contracts []NonNilResultContract) error {
	known := map[*ssa.Function]bool{}
	for _, function := range p.Functions {
		known[function] = true
	}
	next := map[*ssa.Function]map[int]string{}
	for _, contract := range contracts {
		fn := contract.Function
		if fn == nil || !known[fn] {
			return fmt.Errorf("non-nil result contract function does not belong to this program")
		}
		if contract.Index < 0 || contract.Index >= fn.Signature.Results().Len() {
			return fmt.Errorf("non-nil result contract for %s has invalid result index %d", fn, contract.Index)
		}
		if !nilable(fn.Signature.Results().At(contract.Index).Type()) {
			return fmt.Errorf("non-nil result contract for %s result %d has a non-nilable type", fn, contract.Index)
		}
		if strings.TrimSpace(contract.Evidence) == "" {
			return fmt.Errorf("non-nil result contract for %s has no review evidence", fn)
		}
		if next[fn] == nil {
			next[fn] = map[int]string{}
		}
		if _, duplicate := next[fn][contract.Index]; duplicate {
			return fmt.Errorf("duplicate non-nil result contract for %s result %d", fn, contract.Index)
		}
		next[fn][contract.Index] = contract.Evidence
	}
	p.nonNilResults = next
	p.nilFlow = nil
	return nil
}
