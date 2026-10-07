package cypher

import (
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
func splitWhereByVariable(where string, variables []string) (own map[string]string, rest string) {
	known := make(map[string]bool, len(variables))
	for _, variable := range variables {
		known[variable] = true
	}
	parts := make(map[string][]string, len(variables))
	var others []string
	for _, conjunct := range splitTopLevelAndConjuncts(where) {
		conjunct = strings.TrimSpace(conjunct)
		if conjunct == "" {
			continue
		}
		if variable, ok := singleFreeVariable(conjunct); ok && known[variable] {
			parts[variable] = append(parts[variable], conjunct)
			continue
		}
		others = append(others, conjunct)
	}
	own = make(map[string]string, len(parts))
	for variable, conjuncts := range parts {
		own[variable] = joinWhereConjuncts(conjuncts)
	}
	return own, joinWhereConjuncts(others)
}

// singleFreeVariable returns the one variable conjunct reads
// (expressionFreeVariables), or false when it reads none or several, or holds
// a pattern or subquery (hasPatternOrSubquery).
func singleFreeVariable(conjunct string) (string, bool) {
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
func joinWhereConjuncts(conjuncts []string) string {
	switch len(conjuncts) {
	case 0:
		return ""
	case 1:
		return conjuncts[0]
	}
	return "(" + strings.Join(conjuncts, ") AND (") + ")"
}

// hasPatternOrSubquery reports whether expression holds, outside string
// literals and quoted names, a relationship pattern (->, <-, -[, ]-, )-, --)
// or a braced subquery (EXISTS { … }, COUNT { … }, COLLECT { … }). It errs
// towards true: a - -1 or id(a)-1 count as a pattern.
func hasPatternOrSubquery(expression string) bool {
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; character {
		case '\'', '"', '`':
			index = skipQuotedSemanticText(expression, index) - 1
		case '-':
			if index+1 < len(expression) && strings.IndexByte("->[", expression[index+1]) >= 0 {
				return true
			}
			if index > 0 && strings.IndexByte(")]<", expression[index-1]) >= 0 {
				return true
			}
		case '{':
			word := strings.TrimSpace(expression[:index])
			for _, keyword := range [...]string{"EXISTS", "COUNT", "COLLECT"} {
				if len(word) >= len(keyword) && strings.EqualFold(word[len(word)-len(keyword):], keyword) {
					return true
				}
			}
		}
	}
	return false
}
