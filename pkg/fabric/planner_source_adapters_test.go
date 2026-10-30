package fabric

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Author: #1663 implementation lane. New-original-input adapters for legacy
// unit tests. Production decomposition carries planningSource throughout.
func testPlanningSource(query string) planningSource {
	source := cyphertext.NewSource(context.Background(), query, cyphertext.AcceptedSource)
	return planningSource{view: source, anchor: source, ctx: context.Background(), analysis: cypherantlr.OriginalBraceAnalysis(source, false, nil)}
}
func splitTopLevelUnion(query string, isName ClauseKeywordNameReader) ([]string, []bool, bool, error) {
	input := testPlanningSourceWithNameReader(query, isName)
	spans, ops, ok, err := splitTopLevelUnionSpans(input)
	var parts []string
	for _, span := range spans {
		parts = append(parts, query[span.Start:span.End])
	}
	return parts, ops, ok, err
}
func parseLeadingWithUse(ctx context.Context, query string, isName ClauseKeywordNameReader) (UseClause, string, bool, error) {
	input := testPlanningSourceWithNameReader(query, isName)
	input.ctx = ctx
	use, source, ok, err := parseLeadingWithUseSource(input)
	return use.UseClause, source.text(), ok, err
}
func extractTopLevelCallBlocks(query string, isName ClauseKeywordNameReader) ([]callSubqueryBlock, error) {
	return extractTopLevelCallSourceBlocks(testPlanningSourceWithNameReader(query, isName))
}
func callBlockContainsFabricUse(ctx context.Context, query string, isName ClauseKeywordNameReader) (bool, error) {
	input := testPlanningSourceWithNameReader(query, isName)
	input.ctx = ctx
	return callSourceContainsFabricUse(input)
}
func findMatchingBrace(query string, open int) (int, error) {
	record, ok := testPlanningSource(query).view.BraceFacts().At(open)
	if !ok || record.Close < 0 {
		return -1, fmt.Errorf("unmatched brace")
	}
	return record.Close, nil
}

func testPlanningSourceWithNameReader(query string, isName ClauseKeywordNameReader) planningSource {
	input := testPlanningSource(query)
	planner := NewFabricPlanner(nil)
	planner.SetClauseKeywordNameReader(isName)
	input.keywordIsName = planner.keywordIsName
	return input
}
func findLeadingWithClauseEnd(query string, isName ClauseKeywordNameReader) (int, bool) {
	return findLeadingWithSourceClauseEnd(testPlanningSourceWithNameReader(query, isName))
}
func ensureRowProducingPrefix(query string, isName ClauseKeywordNameReader) string {
	return ensureRowProducingSourcePrefix(testPlanningSourceWithNameReader(query, isName))
}
func hasTopLevelReturnClause(query string, isName ClauseKeywordNameReader) bool {
	return hasTopLevelSourceReturnClause(testPlanningSourceWithNameReader(query, isName))
}
func trailingWithAliases(query string, isName ClauseKeywordNameReader) []string {
	return trailingWithSourceAliases(testPlanningSourceWithNameReader(query, isName))
}
func splitAtTopLevelUse(query string, isName ClauseKeywordNameReader) (string, string, bool) {
	return splitSourceAtTopLevelUse(testPlanningSourceWithNameReader(query, isName))
}
func rewriteLeadingWithImports(query string, imports []string) string {
	return rewriteLeadingWithSourceImports(testPlanningSource(query), imports)
}
