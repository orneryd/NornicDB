package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// Author: #1663 lane. Cache inputs are distinct Source windows, even when a
// caller deliberately passes one view twice. Recipes bind by those identities.
// The key includes each accepted original statement's context, since equal
// fragments alone cannot establish equal contextual brace roles.
func selectedPathInputs(pattern braceView, variable string, predicate braceView, terms []braceView) ([]braceView, string) {
	inputs := append([]braceView{pattern, predicate}, terms...)
	var key strings.Builder
	field := func(text string) { key.WriteString(strconv.Itoa(len(text))); key.WriteByte(':'); key.WriteString(text) }
	field(variable)
	roots := map[*cyphertext.Source]int{}
	for index, input := range inputs {
		if input.source != nil {
			root := input.source.OriginalView()
			id, found := roots[root]
			if !found {
				id = len(roots) + 1
				roots[root] = id
				key.WriteByte(1)
				field(root.Text)
			} else {
				key.WriteByte(2)
			}
			field(strconv.Itoa(id))
			inputs[index] = input.window(0, len(input.text()))
		} else {
			key.WriteByte(0)
		}
		field(input.text())
	}
	return inputs, key.String()
}

// Rebinding only composes Source's actual ancestor relations. A missing input
// ancestor is a producer error; text equality is never used to locate one.
func rebaseSelectedPathEntry(entry selectedPathPlanEntry, inputs []braceView) selectedPathPlanEntry {
	if len(entry.inputs) != len(inputs) {
		panic("cypher: selected-path cache input arity changed")
	}
	bound := map[*cyphertext.Source]braceView{}
	rebase := func(view braceView) braceView {
		if view.source == nil {
			return view
		}
		if result, found := bound[view.source]; found {
			return result
		}
		for index, previous := range entry.inputs {
			if previous.source == nil {
				continue
			}
			if _, err := view.source.CopyRelationsFrom(previous.source); err != nil {
				continue
			}
			if inputs[index].source == nil {
				panic("cypher: selected-path cache lost a retained input")
			}
			result := braceView{syntax: inputs[index].syntax, source: view.source.Rebase(previous.source, inputs[index].source)}
			bound[view.source] = result
			return result
		}
		panic("cypher: selected-path cache view has no recorded input ancestor")
	}
	rebaseViews := func(views []braceView) []braceView {
		if views == nil {
			return nil
		}
		result := make([]braceView, len(views))
		for index, view := range views {
			result[index] = rebase(view)
		}
		return result
	}
	var rebaseItem func(pathPatternItem) pathPatternItem
	rebaseItem = func(item pathPatternItem) pathPatternItem {
		item.text, item.where, item.predicates = rebase(item.text), rebase(item.where), rebaseViews(item.predicates)
		if item.items != nil {
			items := make([]pathPatternItem, len(item.items))
			for index, member := range item.items {
				items[index] = rebaseItem(member)
			}
			item.items = items
		}
		return item
	}
	rebaseAutomaton := func(original *pathSearchAutomaton) *pathSearchAutomaton {
		if original == nil {
			return nil
		}
		copy := *original
		copy.slots = append([]pathSearchSlot(nil), original.slots...)
		for index := range copy.slots {
			slot := &copy.slots[index]
			slot.predicates = rebaseViews(slot.predicates)
			slot.transitions = append([]pathSearchTransition(nil), slot.transitions...)
			for next := range slot.transitions {
				slot.transitions[next].predicates = rebaseViews(slot.transitions[next].predicates)
			}
		}
		copy.uniform, copy.uniformStart = uniformTransition(&copy)
		return &copy
	}
	result := entry
	result.inputs, result.filters = inputs, rebaseViews(entry.filters)
	if entry.plan != nil {
		copy := *entry.plan
		copy.start, copy.end = rebaseItem(copy.start), rebaseItem(copy.end)
		copy.constants, copy.pathChecks = rebaseViews(copy.constants), rebaseViews(copy.pathChecks)
		copy.iterationChecks = make(map[int][]braceView, len(entry.plan.iterationChecks))
		for group, checks := range entry.plan.iterationChecks {
			copy.iterationChecks[group] = rebaseViews(checks)
		}
		copy.lengthBounds = append([]pathLengthBound(nil), copy.lengthBounds...)
		for index := range copy.lengthBounds {
			copy.lengthBounds[index].bound = rebase(copy.lengthBounds[index].bound)
		}
		copy.forward, copy.backward = rebaseAutomaton(copy.forward), rebaseAutomaton(copy.backward)
		result.plan = &copy
	}
	return result
}
