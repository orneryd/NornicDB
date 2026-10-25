package cypher

import (
	"context"
	"strings"
)

// mergeNodeWrites is what a statement writes to the nodes its MERGE clauses
// match: the variables named by its SET and REMOVE items, by its MERGE ON
// MATCH SET items, by an ON CREATE SET item of a variable the MERGE didn't
// bind itself, and by its DELETE items. A clause the reader can't see into
// (FOREACH, a CALL subquery, a writing procedure) writes every variable
// (all), as does a write after a WITH or LET that may rename one.
//
// A MERGE of a node the statement writes takes the node's key lock even when
// the node exists (prepareMergeKeys), so concurrent MERGE writers of one node
// wait for each other as in Neo4j instead of failing at commit because both
// wrote it from the same snapshot (NOR-117). A MERGE of a node the statement
// only reads keeps matching without the lock.
type mergeNodeWrites struct {
	variables map[string]struct{}
	all       bool
}

type mergeNodeWritesKey struct{}

// writes reports whether the statement writes the node variable binds.
func (w *mergeNodeWrites) writes(variable string) bool {
	if w == nil || variable == "" {
		return false
	}
	if w.all {
		return true
	}
	_, found := w.variables[variable]
	return found
}

func (w *mergeNodeWrites) add(variable string, renamed bool) {
	if variable == "" || renamed {
		w.all = true
		return
	}
	if w.variables == nil {
		w.variables = make(map[string]struct{})
	}
	w.variables[variable] = struct{}{}
}

func (w *mergeNodeWrites) addSetItems(items []string, renamed bool, except map[string]struct{}) {
	for _, item := range items {
		if strings.TrimSpace(item) == "" {
			continue
		}
		target, _, operator, _ := splitSetAssignment(item)
		if _, own := except[target]; own && target != "" {
			continue
		}
		if operator == "" {
			target = ""
		}
		w.add(target, renamed)
	}
}

// withMergeNodeWrites returns ctx carrying what clauses write
// merged with what the enclosing statement writes, so a
// MERGE in a subquery or FOREACH body sees the writes around it too.
func withMergeNodeWrites(ctx context.Context, clauses []pipelineClause) context.Context {
	writes := &mergeNodeWrites{}
	renamed := false
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseSet:
			writes.addSetItems(splitSetAssignments(pipelineClauseBody(clause.text, "SET")), renamed, nil)
		case pipelineClauseRemove:
			items, err := parseRemoveItems(pipelineClauseBody(clause.text, "REMOVE"))
			if err != nil {
				writes.all = true
				continue
			}
			for _, item := range items {
				writes.add(item.variable, renamed)
			}
		case pipelineClauseMerge:
			actions := splitMergeClauseActions(pipelineClauseBody(clause.text, "MERGE"))
			bound := make(map[string]struct{})
			eachPatternElement(actions.pattern, func(opener byte, element string) {
				if opener == '(' {
					bound[nodePatternStructure(element[1:len(element)-1]).variable] = struct{}{}
				}
			})
			writes.addSetItems(mergeActionAssignments(actions.onMatch), renamed, nil)
			// The MERGE that creates a node holds its key already.
			writes.addSetItems(mergeActionAssignments(actions.onCreate), renamed, bound)
		case pipelineClauseDelete:
			body := pipelineClauseBody(pipelineClauseBody(clause.text, "DETACH"), "DELETE")
			for _, item := range splitTopLevelComma(body) {
				if item = strings.TrimSpace(item); !isValidIdentifier(item) {
					item = ""
				}
				writes.add(item, renamed)
			}
		case pipelineClauseWith:
			renamed = renamed || containsKeywordOutsideStrings(clause.text, "AS")
		case pipelineClauseLet:
			renamed = true
		case pipelineClauseForeach, pipelineClauseCallSubquery:
			writes.all = true
		case pipelineClauseCall:
			if procedure, found := globalProcedureRegistry.Get(extractProcedureName(ctx, clause.text)); !found || procedure.Spec.writes() {
				writes.all = true
			}
		}
	}
	if outer := mergeNodeWritesFromContext(ctx); outer != nil {
		writes.all = writes.all || outer.all
		for variable := range outer.variables {
			writes.add(variable, false)
		}
	}
	return context.WithValue(ctx, mergeNodeWritesKey{}, writes)
}

func mergeNodeWritesFromContext(ctx context.Context) *mergeNodeWrites {
	writes, _ := ctx.Value(mergeNodeWritesKey{}).(*mergeNodeWrites)
	return writes
}
