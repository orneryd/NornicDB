package cypher

import (
	"github.com/orneryd/nornicdb/pkg/fabric"
	"strings"
)

// compiledReadFootprint reads an operand's retained compiler facts. An operand
// is not a statement: physical consumers must not rebuild a statement plan to
// discover its reads. Incomplete facts still go to the native role owner.
func compiledReadFootprint(text string) ([]string, bool) {
	if strings.TrimSpace(text) == "" {
		return nil, true
	}
	if fabric.QuerySyntaxDemand(text).Has(fabric.NativeAdmissionSyntax) {
		return nil, false
	}
	info := rowValueProgramForFacts(text).info
	return info.readFootprint(), info.pure
}

// A clause arrives with the kind and native input supplied by its statement
// owner. Its existing projection plan owns the complete projection read set.
func compiledClauseReadFootprint(clause pipelineClause) ([]string, bool) {
	if clause.kind != pipelineClauseReturn {
		return nil, false
	}
	plan := returnProjectionPlanFor(clause.text, clause)
	return plan.readReferences, plan.readComplete
}

func physicalReadReferences(text string, inputs ...fabric.ExpressionInput) []string {
	if references, complete := compiledReadFootprint(text); complete {
		return references
	}
	return nativePhysicalReadReferences(text, inputs...)
}

func physicalClauseReadReferences(clause pipelineClause) []string {
	if references, complete := compiledClauseReadFootprint(clause); complete {
		return references
	}
	return nativePhysicalReadReferences(clause.text, clause.input)
}

func nativePhysicalReadReferences(text string, inputs ...fabric.ExpressionInput) []string {
	var occurrences []fabric.ExpressionReference
	if len(inputs) > 0 && inputs[0].Valid() {
		occurrences = inputs[0].ReadReferences()
	} else {
		occurrences = fabric.ExpressionReadReferences(text)
	}
	var references []string
	for _, occurrence := range occurrences {
		name := occurrence.Name
		if occurrence.Property != "" {
			name += "." + occurrence.Property
		}
		references = appendUniqueString(references, name)
	}
	return references
}

// Called only by the existing projection-plan producer, before publication.
// The immutable plan and the evaluator borrow one canonical operand program.
func (plan *returnProjectionPlan) compileReadFootprint(clause string) {
	if !plan.valid || plan.star || fabric.QuerySyntaxDemand(clause).Has(fabric.NativeAdmissionSyntax) || topLevelKeywordIndex(plan.modifiers, "ORDER BY") >= 0 {
		return
	}
	var references []string
	for _, projection := range plan.projections {
		info := rowValueProgramForFacts(projection.expr).info
		if !info.pure {
			return
		}
		for _, reference := range info.readFootprint() {
			references = appendUniqueString(references, reference)
		}
	}
	plan.readReferences, plan.readComplete = references, true
}
