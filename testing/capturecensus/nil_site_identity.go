package capturecensus

import (
	"fmt"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// nilContextSite preserves reviewed physical-site keys, qualifying generic
// instances that share a position by their compiled owner. A source-less compiler
// call instead uses its exact SSA owner, source carriers and instruction slot.
// Function.String alone is insufficient: separate bound-method wrappers can
// have identical names, signatures and bodies but different source bindings.
func nilContextSite(program *workcensus.Program, call ssa.CallInstruction, argument int, syntheticOwners map[*ssa.Function]string) string {
	position := program.FileSet.PositionFor(call.Pos(), false)
	if call.Pos().IsValid() {
		file := position.Filename
		if relative, err := filepath.Rel(program.Root, file); err == nil {
			file = filepath.ToSlash(relative)
		}
		site := fmt.Sprintf("%s:%d:%d", file, position.Line, position.Column)
		if function := call.Parent(); function.Origin() != nil {
			site += fmt.Sprintf("/instantiation=%q", function.String())
		}
		return site
	}
	function := call.Parent()
	owner, found := syntheticOwners[function]
	if !found {
		var bindings []string
		// Use the extraction engine's exact function-value source carriers. These
		// identify origins, not proof that any particular source invokes the call.
		for _, reference := range program.SourceReferences(function) {
			pos := program.FileSet.PositionFor(reference.Pos(), false)
			file := pos.Filename
			if relative, err := filepath.Rel(program.Root, file); err == nil {
				file = filepath.ToSlash(relative)
			}
			bindings = append(bindings, fmt.Sprintf("%s:%d:%d %s/%s", file, pos.Line, pos.Column, reference.Parent().String(), nilInstructionSlot(reference)))
		}
		sort.Strings(bindings)
		owner = fmt.Sprintf("ssa{function=%q; signature=%q; synthetic=%q; bindings=%q}", function.String(), types.TypeString(function.Signature, nil), function.Synthetic, strings.Join(bindings, " | "))
		syntheticOwners[function] = owner
	}
	return fmt.Sprintf("%s/%s/context-argument-%d", owner, nilInstructionSlot(call), argument)
}

func nilInstructionSlot(instruction ssa.Instruction) string {
	block := instruction.Block()
	for index, candidate := range block.Instrs {
		if candidate == instruction {
			return fmt.Sprintf("block-%d/instruction-%d", block.Index, index)
		}
	}
	// Every analyzed instruction belongs to its recorded SSA block. A broken
	// model must stop instead of fabricating a reusable source identity.
	panic("nil-context identity: instruction is absent from its SSA block")
}
