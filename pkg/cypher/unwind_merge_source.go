package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

// Author: #1663 lane. The text-keyed cache holds a grammar recipe, never a
// second statement's usable facts. Source rebinds that recipe to the actual
// input; property-map applicability consumes the current original records.
func rebaseUnwindMergeChainPlan(plan unwindMergeChainPlan, input braceView) unwindMergeChainPlan {
	if !plan.supported {
		return plan
	}
	if plan.input.source == nil {
		panic("cypher: cached UNWIND grammar lost its source ancestor")
	}
	bound := make(map[*cyphertext.Source]braceView)
	applicable := true
	rebind := func(view braceView) braceView {
		if view.source == nil {
			return view
		} // absent optional syntax
		if result, found := bound[view.source]; found {
			return result
		}
		source := view.source.Rebase(plan.input.source, input.source)
		result := braceView{syntax: input.syntax, source: source}
		bound[view.source] = result
		return result
	}
	mapChecked := make(map[*cyphertext.Source]bool)
	assignments := func(items []unwindSimpleSetAssignment) []unwindSimpleSetAssignment {
		copy := append([]unwindSimpleSetAssignment(nil), items...)
		for index := range copy {
			original := copy[index].mapInput.source
			if original == nil {
				continue
			} // SET's derived name/parameter metadata
			copy[index].mapInput = rebind(copy[index].mapInput)
			if !mapChecked[original] {
				_, valid := copy[index].mapInput.keyedEntries()
				applicable = applicable && valid
				mapChecked[original] = true
			}
		}
		return copy
	}
	result := plan
	result.input = input
	result.steps = append([]unwindMergeChainStep(nil), plan.steps...)
	for index := range result.steps {
		step := &result.steps[index]
		if step.node != nil {
			copy := *step.node
			copy.matchAssignments = assignments(copy.matchAssignments)
			copy.setAssignments = assignments(copy.setAssignments)
			copy.onCreateAssignments = assignments(copy.onCreateAssignments)
			copy.onMatchAssignments = assignments(copy.onMatchAssignments)
			step.node = &copy
		}
		if step.lookup != nil {
			copy := *step.lookup
			copy.matchAssignments = assignments(copy.matchAssignments)
			copy.setAssignments = assignments(copy.setAssignments)
			step.lookup = &copy
		}
		if step.relationship != nil {
			copy := *step.relationship
			copy.matchAssignments = assignments(copy.matchAssignments)
			copy.setAssignments = assignments(copy.setAssignments)
			step.relationship = &copy
		}
		if step.with != nil {
			copy := *step.with
			copy.assignments = append([]unwindMergeChainWithAssignment(nil), copy.assignments...)
			copy.projection.view = rebind(copy.projection.view)
			copy.projection.whereView = rebind(copy.projection.whereView)
			copy.projection.projections = append([]pipelineRowProjection(nil), copy.projection.projections...)
			for index := range copy.projection.projections {
				copy.projection.projections[index].view = rebind(copy.projection.projections[index].view)
			}
			step.with = &copy
		}
	}
	if !applicable {
		return unwindMergeChainPlan{}
	}
	return result
}
