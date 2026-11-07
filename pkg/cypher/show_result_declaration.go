package cypher

import (
	"context"
	"slices"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// showResult is one SHOW command's declared result schema: columns in the
// listing's order (YIELD * reaches all of them) and defaults, the columns a
// statement without YIELD returns (nil: every column is a default one).
//
// A listing returns the rows; its EXPLAIN and PROFILE header, and the projection
// a retained YIELD / RETURN tail composes, read this one declaration. None of
// them runs the listing to learn its schema, and none keeps a second copy of the
// column order, so a plan's header cannot disagree with the listing it plans.
type showResult struct {
	columns  []string
	defaults []string
}

// declareShowResult builds a declaration whose full order is defaults followed
// by the listing-only columns extensions. The declaration is built once, at
// package initialization, and then only read.
func declareShowResult(defaults []string, extensions ...string) showResult {
	columns := make([]string, 0, len(defaults)+len(extensions))
	columns = append(columns, defaults...)
	columns = append(columns, extensions...)
	return showResult{columns: columns, defaults: defaults}
}

// listing returns the listing's result over rows. The header is the declaration's
// own order in a copy, because a result header is handed to callers that may
// rewrite it (applyShowTail projects and extends it). A listing whose default
// columns are its full set records none, as it recorded none before.
func (declared showResult) listing(rows [][]interface{}) *ExecuteResult {
	result := &ExecuteResult{Columns: slices.Clone(declared.columns), Rows: rows}
	if declared.defaults == nil {
		return result
	}
	return withShowDefaultColumns(result, declared.defaults)
}

// defaultColumns are the columns a statement without YIELD returns: the
// declared default set, or every column when the command declares no smaller
// one.
func (declared showResult) defaultColumns() []string {
	if declared.defaults == nil {
		return declared.columns
	}
	return declared.defaults
}

// tailProjection composes the declaration with the tail the statement retains.
// Without YIELD the command's default columns are the result and a WHERE filters
// them; with YIELD the items name the result columns in their order, and the
// tail's own expressions read the declared columns and the YIELD aliases, as
// execution reads them there. An item that isn't a declared column is rejected
// by the same owner execution reports it from; position is where the tail is.
func (declared showResult) tailProjection(tail cypherantlr.SyntaxNode) (outputs, expressions []string, position int, err error) {
	defaults := declared.defaultColumns()
	yield := yieldFromSyntax(tail)
	if yield == nil {
		return defaults, defaults, 0, nil
	}
	if yield.yieldAll {
		return declared.columns, declared.columns, 0, nil
	}
	if err := validateYieldColumnsExist(declared.columns, yield); err != nil {
		start, _ := tail.Span()
		return nil, nil, start, err
	}
	outputs = make([]string, 0, len(yield.items))
	expressions = make([]string, 0, len(declared.columns)+len(yield.items))
	expressions = append(expressions, declared.columns...)
	for _, item := range yield.items {
		outputs = append(outputs, item.column())
		expressions = append(expressions, item.column())
	}
	return outputs, expressions, 0, nil
}

// showConstraintResult5 and showConstraintResult25 are SHOW [kind] CONSTRAINTS'
// two schemas: Neo4j's full set, then NornicDB's cardinality and policy
// constraint columns, which only YIELD * or YIELD <column> show (with Cypher
// 25's enforcedLabel and classification), and the default columns a statement
// without YIELD returns.
var showConstraintResult5, showConstraintResult25 = declareShowConstraintResults()

func declareShowConstraintResults() (cypher5, cypher25 showResult) {
	extensions := []string{"ownedIndex", "propertyType", "options", "createStatement", "direction", "maxCount", "sourceLabel", "targetLabel", "policyMode"}
	lead := showConstraintsDefaultColumns[:6]
	cypher5 = showResult{
		columns:  slices.Concat(lead, extensions),
		defaults: showConstraintsDefaultColumns,
	}
	cypher25 = showResult{
		columns:  slices.Concat(lead, []string{"enforcedLabel", "classification"}, extensions),
		defaults: showConstraintsCypher25DefaultColumns,
	}
	return cypher5, cypher25
}

// showConstraintResult is SHOW [kind] CONSTRAINTS' schema in statement version.
func showConstraintResult(cypher25 bool) showResult {
	if cypher25 {
		return showConstraintResult25
	}
	return showConstraintResult5
}

// showIndexesResult is SHOW [type] INDEX[ES]' schema: Neo4j's tracked-read
// columns after the default set, which NornicDB reports as untracked.
var showIndexesResult = declareShowResult(showIndexesDefaultColumns, "trackedSince", "options", "failureMessage", "createStatement")

// showConstraintContractsResult is SHOW CONSTRAINT CONTRACTS' schema. Every
// column is a default one: Neo4j declares no smaller default set for it.
var showConstraintContractsResult = showResult{columns: []string{"name", "targetEntityType", "targetLabelOrType", "entryCount", "compiledEntryCount", "runtimeEntryCount", "definition"}}

// showUsersResult is SHOW USERS' and SHOW CURRENT USER's schema in Neo4j 5.26,
// which is also its default set.
var showUsersResult = showResult{columns: showUsersColumns}

// showCommandResult declares the result schema of the SHOW command show names,
// through the declaration the command's own listing returns its rows with. The
// second result reports whether this owner declares that command's schema: a
// command whose listing is declared elsewhere keeps the existing contract.
func showCommandResult(ctx context.Context, show cypherantlr.SyntaxNode, cypher25 bool) (showResult, bool, error) {
	code := show.Code()
	// SHOW CONSTRAINT CONTRACTS lists contracts, which have a schema of their
	// own; the constraint listing's declaration does not answer for it.
	if isShowConstraintContractsCommand(ctx, code) {
		return showConstraintContractsResult, true, nil
	}
	if _, isConstraints, err := showConstraintKindOf(code, cypher25); isConstraints {
		if err != nil {
			return showResult{}, true, err
		}
		return showConstraintResult(cypher25), true, nil
	}
	if showIndexesCommand(ctx, code) {
		return showIndexesResult, true, nil
	}
	if startsWithKeywords(ctx, code, "SHOW", "USERS") || startsWithKeywords(ctx, code, "SHOW", "CURRENT USER") {
		return showUsersResult, true, nil
	}
	return showResult{}, false, nil
}

// showIndexesCommand reports whether code is one of the SHOW [type] INDEX[ES]
// commands executeShowIndexes lists: the index types it filters, then the
// unfiltered forms.
func showIndexesCommand(ctx context.Context, code string) bool {
	for _, indexType := range []string{"FULLTEXT", "RANGE", "VECTOR", "LOOKUP"} {
		if startsWithKeywords(ctx, code, "SHOW", indexType+" INDEXES") || startsWithKeywords(ctx, code, "SHOW", indexType+" INDEX") {
			return true
		}
	}
	return startsWithKeywords(ctx, code, "SHOW", "INDEXES") || startsWithKeywords(ctx, code, "SHOW", "INDEX")
}
