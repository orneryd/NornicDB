package cypher

import cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

// These immutable package-local views select grammar roles. Keeping the views
// here avoids both copied spellings in scanners and per-query allocations.
var clauseBoundaryKeywords = cypherantlr.ClauseWords()
var queryPartStartKeywords = cypherantlr.QueryPartStartWords()
var paginationKeywords = cypherantlr.ClauseKeywordsForRules(
	cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt)
var projectionTailKeywords = cypherantlr.ClauseKeywordsForRules(
	cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt)
var projectionModifierKeywords = cypherantlr.ClauseKeywordsForRules(
	cypherantlr.CypherParserRULE_where, cypherantlr.CypherParserRULE_orderSt,
	cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt)
var projectionPaginationWhereKeywords = cypherantlr.ClauseKeywordsForRules(
	cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt, cypherantlr.CypherParserRULE_where)
var projectionReturnTailKeywords = cypherantlr.ClauseKeywordsForRules(
	cypherantlr.CypherParserRULE_withSt, cypherantlr.CypherParserRULE_returnSt,
	cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt)
var showSegmentKeywords = cypherantlr.ClauseKeywordGroupsForRules(
	cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt,
	cypherantlr.CypherParserRULE_limitSt, cypherantlr.CypherParserRULE_where, cypherantlr.CypherParserRULE_returnSt)
var returnItemBoundaryKeywords = cypherantlr.ClauseKeywordsForRules(
	cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt,
	cypherantlr.CypherParserRULE_limitSt, cypherantlr.CypherParserRULE_unionSt)
var updatingClauseKeywords = cypherantlr.ClauseKeywordsForRules(cypherantlr.CypherParserRULE_updatingStatement)
var patternPartTailKeywords = cypherantlr.ClauseKeywordsForRules(
	cypherantlr.CypherParserRULE_mergeAction, cypherantlr.CypherParserRULE_where)

// WHEN is the native conditional-composition entry, lowered before grammar
// admission. All ordinary clause starts are grammar-owned.
func startsQueryPartWord(word string) bool {
	if equalFoldASCII(word, "WHEN") {
		return true
	}
	for _, keyword := range queryPartStartKeywords {
		if equalFoldASCII(word, keyword) {
			return true
		}
	}
	return false
}
