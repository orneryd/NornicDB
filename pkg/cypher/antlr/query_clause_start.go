package antlr

import (
	"slices"
	"sort"
	"strings"
	"sync"
)

type useFollowerScope uint8

const (
	useQueryScope useFollowerScope = 1 << iota
	useAdministrationScope
)

// A word has one grammar-derived entry, with its permitted routing scopes.
// Shared first words such as CREATE are represented once, not in two lists.
var useFollowerWords = sync.OnceValue(func() map[string]useFollowerScope {
	words := make(map[string]useFollowerScope)
	for word := range ruleStartWords(CypherParserRULE_useQueryFollower) {
		words[word] = useQueryScope
	}
	for word := range ruleStartWords(CypherParserRULE_useAdministrationFollower) {
		words[word] |= useAdministrationScope
	}
	return words
})

var clauseWords = sync.OnceValue(func() []string {
	words := ruleStartWords(CypherParserRULE_clauseWord)
	result := make([]string, 0, len(words))
	for word := range words {
		result = append(result, word)
	}
	sort.Strings(result)
	return result
})

// ClauseWords returns an independent ordered view of the grammar's clause
// vocabulary. Callers may apply their own projection policy, not redefine it.
func ClauseWords() []string { return slices.Clone(clauseWords()) }

// completionClauseWords are clauses whose bodies determine whether a query
// can finish. Projection modifiers do not replace their owning WITH/RETURN,
// and a USE routing prefix does not replace the body it selects. This role
// is distinct from the full clause spelling vocabulary.
// SHOW is a completion owner: its command returns default columns without a
// RETURN. A YIELD/WITH/RETURN continuation then owns its own completion. This
// does not make bare SHOW valid; showCommand still validates the command body.
var completionClauseWords = sync.OnceValue(func() []string {
	words := ruleStartWords(CypherParserRULE_readingStatement, CypherParserRULE_updatingStatement,
		CypherParserRULE_withSt, CypherParserRULE_returnSt, CypherParserRULE_yieldItemsStart,
		CypherParserRULE_unionSt, CypherParserRULE_showCommand)
	for word := range ruleStartWords(CypherParserRULE_useClause) {
		delete(words, word)
	}
	result := make([]string, 0, len(words))
	for word := range words {
		result = append(result, word)
	}
	sort.Strings(result)
	return result
})

// CompletionClauseWords preserves clause ownership through modifiers. Its
// spellings are grammar-derived; callers consume this semantic role rather
// than substituting every recognized clause word for a completion owner.
func CompletionClauseWords() []string { return slices.Clone(completionClauseWords()) }

// ClauseKeywordsForRules projects clause prefixes in the caller's semantic
// order. Spellings come from the grammar. ORDER's BY belongs to its prefix,
// rather than its expression, so scanners consume both tokens together.
// A caller chooses roles (for example pagination), never a second word list.
func ClauseKeywordsForRules(rules ...int) []string {
	var result []string
	for _, group := range ClauseKeywordGroupsForRules(rules...) {
		for _, word := range group {
			if !slices.Contains(result, word) {
				result = append(result, word)
			}
		}
	}
	return result
}

// ClauseKeywordGroupsForRules retains one group per requested grammar role,
// including roles with multiple spellings. Flattened FIRST-set cardinality
// cannot be used as an ordinal for semantic stages such as SHOW's tail.
func ClauseKeywordGroupsForRules(rules ...int) [][]string {
	result := make([][]string, 0, len(rules))
	for _, rule := range rules {
		var group []string
		for _, word := range sortedGrammarWords(rule) {
			if rule == CypherParserRULE_orderSt {
				word += " " + strings.Trim(CypherParserParserStaticData.LiteralNames[CypherParserBY], "'")
			}
			group = append(group, word)
		}
		result = append(result, group)
	}
	return result
}

// QueryPartStartWords projects the query starts used after composition's NEXT.
// UNION joins query parts; it does not open one. The native composition
// rewriter owns its additional WHEN entry separately from syntax grammar.
func QueryPartStartWords() []string {
	words := ruleStartWords(CypherParserRULE_useQueryFollower)
	for word := range ruleStartWords(CypherParserRULE_useClause) {
		words[word] = struct{}{}
	}
	for word := range ruleStartWords(CypherParserRULE_unionSt) {
		delete(words, word)
	}
	result := make([]string, 0, len(words))
	for word := range words {
		result = append(result, word)
	}
	sort.Strings(result)
	return result
}

// IsClauseWord reports whether word belongs to the grammar's clause vocabulary.
func IsClauseWord(word string) bool {
	for _, keyword := range clauseWords() {
		if equalGrammarWord(word, keyword) {
			return true
		}
	}
	return false
}

// IsShowIndexModifier reads the optional modifier of the grammar's SHOW INDEX
// branch. It does not derive a command family from an unrelated FIRST set.
func IsShowIndexModifier(word string) bool {
	for _, modifier := range showIndexModifiers {
		if equalGrammarWord(word, modifier) {
			return true
		}
	}
	return false
}

var statementBodyStarts = sync.OnceValue(func() map[string]struct{} {
	return ruleStartWords(CypherParserRULE_statementBodyOpening)
})

// IsStatementBodyStart projects the grammar's standalone statement opening
// policy. SET/REMOVE and terminal-only FINISH/UNION do not open a body alone.
func IsStatementBodyStart(word string) bool {
	return grammarContains(statementBodyStarts(), word)
}

// graphQueryStarts are the first words of the grammar's single query: every
// clause and projection a graph query can open with (MATCH, LET, FOR, CREATE,
// RETURN, FINISH, …).
var graphQueryStarts = sync.OnceValue(func() map[string]struct{} {
	return ruleStartWords(CypherParserRULE_singleQuery)
})

// statementStarts are the first words of the grammar's script: every word a
// statement (graph query, command, transaction control or execution prefix)
// can start with.
var statementStarts = sync.OnceValue(func() map[string]struct{} {
	return ruleStartWords(CypherParserRULE_script)
})

// IsStatementStart reports whether word starts a statement in the shared
// grammar (statementStarts).
func IsStatementStart(word string) bool {
	return grammarContains(statementStarts(), word)
}

// ruleStartWords reads FIRST at production entry from generated grammar metadata.
func ruleStartWords(rules ...int) map[string]struct{} {
	CypherParserInit()
	starts := make(map[string]struct{})
	for _, rule := range rules {
		for _, word := range grammarFirstWords[rule] {
			starts[word] = struct{}{}
		}
	}
	return starts
}

// IsGraphQueryStart reports whether word opens a graph query in the shared
// grammar (graphQueryStarts).
func IsGraphQueryStart(word string) bool {
	return grammarContains(graphQueryStarts(), word)
}

func grammarContains(words map[string]struct{}, word string) bool {
	for keyword := range words {
		if equalGrammarWord(word, keyword) {
			return true
		}
	}
	return false
}

// Grammar keywords are ASCII and lookup must not allocate or turn a Unicode
// identifier into a keyword (for example, a long-s into S).
func equalGrammarWord(word, keyword string) bool {
	if len(word) != len(keyword) {
		return false
	}
	for i := range word {
		c := word[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != keyword[i] {
			return false
		}
	}
	return true
}

func grammarWord(word string) bool {
	if word == "" {
		return false
	}
	for _, c := range word {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
}

var projectionModifierWords = sync.OnceValue(func() []string {
	return ClauseKeywordsForRules(CypherParserRULE_where, CypherParserRULE_orderSt,
		CypherParserRULE_skipSt, CypherParserRULE_limitSt)
})
var showTailWords = sync.OnceValue(func() []string {
	return sortedGrammarWords(CypherParserRULE_showTailStart)
})
var callTailBoundaryWords = sync.OnceValue(func() []string {
	handled := ruleStartWords(CypherParserRULE_callTailProjectionStart)
	words := ClauseWords()
	return slices.DeleteFunc(words, func(word string) bool { _, found := handled[word]; return found })
})

func sortedGrammarWords(rule int) []string {
	words := ruleStartWords(rule)
	result := make([]string, 0, len(words))
	for word := range words {
		result = append(result, word)
	}
	sort.Strings(result)
	return result
}

// ProjectionModifierWords projects complete modifier prefixes, including BY.
// FIRST words alone cannot distinguish the ORDER name from an ORDER BY clause.
func ProjectionModifierWords() []string { return slices.Clone(projectionModifierWords()) }

// ShowTailWords projects the grammar's SHOW-tail boundaries.
func ShowTailWords() []string { return slices.Clone(showTailWords()) }

// CallTailBoundaryWords excludes clauses handled by the specialized projection plan.
func CallTailBoundaryWords() []string { return slices.Clone(callTailBoundaryWords()) }

// IsCallTailBoundaryWord reads the grammar's continuation projection.
func IsCallTailBoundaryWord(word string) bool {
	for _, keyword := range callTailBoundaryWords() {
		if equalGrammarWord(word, keyword) {
			return true
		}
	}
	return false
}

var labelClauseStarts = sync.OnceValue(func() map[string]struct{} {
	return ruleStartWords(CypherParserRULE_labelClauseStart)
})

// IsLabelClauseWord includes the grammar's MERGE subclause boundary.
func IsLabelClauseWord(word string) bool { return grammarContains(labelClauseStarts(), word) }

// IsShowConstraintKindWord reads syntax vocabulary, not category/version policy.
func IsShowConstraintKindWord(word string) bool {
	for _, keyword := range showConstraintKindVocabulary {
		if equalGrammarWord(word, keyword) {
			return true
		}
	}
	return false
}

// QueryBodyContainer reports the grammar-owned identity of a body-bearing rule.
func QueryBodyContainer(rule int) string { return queryBodyContainerByRule[rule] }

var queryBodyContainerWords = sync.OnceValue(func() []string {
	words := make(map[string]struct{})
	for _, word := range queryBodyContainerByRule {
		words[word] = struct{}{}
	}
	result := make([]string, 0, len(words))
	for word := range words {
		result = append(result, word)
	}
	sort.Strings(result)
	return result
})

// QueryBodyContainerWords returns each grammar-owned body identity once.
func QueryBodyContainerWords() []string { return slices.Clone(queryBodyContainerWords()) }

var expressionQueryBodyWords = sync.OnceValue(func() []string {
	set := make(map[string]struct{})
	for rule := range expressionQueryBodyRules {
		set[queryBodyContainerByRule[rule]] = struct{}{}
	}
	words := make([]string, 0, len(set))
	for word := range set {
		words = append(words, word)
	}
	sort.Strings(words)
	return words
})

// ExpressionQueryBodyWords projects only the grammar's expression containers.
func ExpressionQueryBodyWords() []string { return slices.Clone(expressionQueryBodyWords()) }

// IsUpdatingClause reports the nearest clause's grammar role. A read inside
// an updating FOREACH remains a read; outer scopes cannot change its role.
func (node SyntaxNode) IsUpdatingClause() bool {
	for node.Valid() {
		if QueryBodyContainer(node.Rule()) != "" {
			return false
		}
		switch node.Rule() {
		case CypherParserRULE_updatingStatement:
			return true
		case CypherParserRULE_readingStatement, CypherParserRULE_singleQuery, CypherParserRULE_subqueryBody:
			return false
		}
		parent, found := node.Parent()
		if !found {
			break
		}
		node = parent
	}
	return false
}
