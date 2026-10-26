package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// splitWhereByVariable divides the AND-conjuncts of where among variables:
// own[v] joins, with AND, the conjuncts whose only free variable is v (id(a) =
// $x, a.name STARTS WITH 'x', toLower(a.k) = 'y', …), and rest joins every
// other conjunct: those over several variables (a.k = b.k), over a variable
// not in variables, or over none ($flag). A conjunct with a subquery or a
// pattern ({ … }, -->, <-, -[) stays in rest, since its pattern may bind or
// read other variables. Because WHERE keeps a row only when every conjunct is
// true, filtering each variable's candidates by own[v] and the combinations
// by rest gives the rows the whole clause gives.
func splitWhereByVariable(where braceView, variables []string) (own map[string]braceView, rest braceView) {
	known := make(map[string]bool, len(variables))
	for _, variable := range variables {
		known[variable] = true
	}
	parts := make(map[string][]braceView, len(variables))
	var others []braceView
	for _, conjunct := range splitTopLevelAndConjuncts(where) {
		conjunct = conjunct.trim()
		if conjunct.empty() {
			continue
		}
		if variable, ok := singleFreeVariable(conjunct); ok && known[variable] {
			parts[variable] = append(parts[variable], conjunct)
			continue
		}
		others = append(others, conjunct)
	}
	own = make(map[string]braceView, len(parts))
	for variable, conjuncts := range parts {
		own[variable] = joinWhereConjuncts(where, conjuncts)
	}
	return own, joinWhereConjuncts(where, others)
}

// singleFreeVariable returns the one variable conjunct reads
// (expressionFreeVariables), or false when it reads none or several, or holds
// a pattern or subquery (hasPatternOrSubquery).
func singleFreeVariable(conjunct braceView) (string, bool) {
	if hasPatternOrSubquery(conjunct) {
		return "", false
	}
	variable := ""
	for _, name := range expressionFreeVariables(conjunct) {
		if variable != "" && name != variable {
			return "", false
		}
		variable = name
	}
	return variable, variable != ""
}

// joinWhereConjuncts joins conjuncts into one WHERE expression, each in
// parentheses so an OR inside one stays inside it.
func joinWhereConjuncts(input braceView, conjuncts []braceView) braceView {
	switch len(conjuncts) {
	case 0:
		return input.window(0, 0)
	case 1:
		return conjuncts[0]
	}
	parts := make([]braceView, len(conjuncts))
	for index, conjunct := range conjuncts {
		parts[index] = conjunct.wrap("(", ")")
	}
	return input.assembleCode(parts, " AND ")
}

// hasPatternOrSubquery reports whether expression holds, outside string
// literals and quoted names, a relationship pattern (->, <-, -[, ]-, )-, --)
// or a braced subquery (EXISTS { … }, COUNT { … }, COLLECT { … }). It errs
// towards true: a - -1 or id(a)-1 count as a pattern.
func hasPatternOrSubquery(view braceView) bool {
	expression := view.text()
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; {
		case (cypherLexicalByte(character)) == cypherQuoteClass:
			index = skipQuotedSemanticText(expression, index) - 1
		case (cypherLexicalByte(character)) == '-':
			if index+1 < len(expression) && strings.IndexByte("->[", expression[index+1]) >= 0 {
				return true
			}
			if index > 0 && strings.IndexByte(")]<", expression[index-1]) >= 0 {
				return true
			}
		case cyphertext.IsOpenBrace(cypherLexicalByte(character)):
			if view.isBody(index) {
				return true
			}
		}
	}
	return false
}
