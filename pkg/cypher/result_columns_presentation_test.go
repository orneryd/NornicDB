package cypher

import (
	"context"
	"sort"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Presentation uses the same retained clause/UNION structure as execution.
// The first branch owns UNION's ordered columns; a last-RETURN text scan would
// reorder fields when a later branch writes the same identities differently.
// This is a source-spelling adapter, with no second binding or scope walk.
func restoreProjectionPresentation(source string, result *ExecuteResult) {
	syntax, err := cypherantlr.ParseSyntax(context.Background(), source, cypherantlr.FragmentEntry, false, cypherantlr.AcceptedSource)
	if err != nil {
		return
	}
	body := queryBodyFromSyntax(statementSyntaxQuery(syntax))
	if len(body.branches) == 0 {
		return
	}
	clauses := body.branches[0].clauses
	if len(clauses) == 0 || clauses[len(clauses)-1].kind != pipelineClauseReturn {
		return
	}
	returnProjectionPlanFor(context.Background(), clauses[len(clauses)-1].text).restorePresentation(result)
}

// restorePresentation applies this owner's names after source-position mapping.
// Wildcard expansion sorts only incoming names; written items retain their
// position after the wildcard. The same expansion already drives execution.
func (plan *returnProjectionPlan) restorePresentation(result *ExecuteResult) {
	if !plan.valid {
		return
	}
	if !plan.star {
		if len(plan.columns) == len(result.Columns) {
			result.Columns = append([]string(nil), plan.columns...)
		}
		return
	}
	explicit := make(map[string]bool, len(plan.starProjections))
	for _, item := range plan.starProjections {
		explicit[item.alias] = true
	}
	var incoming []string
	for _, column := range result.Columns {
		if !explicit[column] {
			incoming = append(incoming, column)
		}
	}
	sort.Strings(incoming)
	columns := plan.withStarExpanded(incoming).columns
	if len(columns) != len(result.Columns) {
		return
	}
	order := make([]int, len(columns))
	for index, column := range columns {
		found := false
		for previous, actual := range result.Columns {
			if actual == column {
				order[index], found = previous, true
				break
			}
		}
		if !found {
			return
		}
	}
	rows := make([][]interface{}, len(result.Rows))
	for index, row := range result.Rows {
		if len(row) != len(order) {
			return
		}
		rows[index] = make([]interface{}, len(order))
		for column, previous := range order {
			rows[index][column] = row[previous]
		}
	}
	result.Columns, result.Rows = columns, rows
}
