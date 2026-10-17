package cypher

import (
	"context"
	"strconv"
	"strings"
)

// Parsing of a RETURN/WITH projection's modifier tail: the ORDER BY terms and
// the SKIP / LIMIT expressions that applyResultModifiers applies.

type orderByTerm struct {
	column     string
	descending bool
}

// parseOrderByTerms parses the ORDER BY of a projection's modifiers. Keywords
// inside braces (COLLECT { … ORDER BY … }, map literals) belong to nested
// expressions, never to the projection (#652, #547).
func parseOrderByTerms(modifiers string) []orderByTerm {
	orderByIndex := topLevelKeywordIndex(modifiers, "ORDER BY")
	if orderByIndex < 0 {
		return nil
	}
	return parseOrderByClause(modifiers[orderByIndex+len("ORDER BY"):])
}

// parseOrderByClause parses the sort items that follow ORDER BY. The list ends
// at SKIP / LIMIT, and at the WHERE that may follow ORDER BY in a WITH clause
// (WITH n ORDER BY n.x WHERE n.y > 0): that WHERE filters the sorted rows and
// is not part of the last sort item.
func parseOrderByClause(clause string) []orderByTerm {
	clause = strings.TrimSpace(clause)
	end := len(clause)
	for _, keyword := range []string{"LIMIT", "SKIP"} {
		if index := topLevelKeywordIndex(clause, keyword); index >= 0 && index < end {
			end = index
		}
	}
	if index := topLevelKeywordIndex(clause, "WHERE"); index >= 0 && index < end {
		end = index
	}
	clause = strings.TrimSpace(clause[:end])
	parts := splitTopLevelComma(clause)
	terms := make([]orderByTerm, 0, len(parts))
	for _, part := range parts {
		expression := strings.TrimSpace(part)
		fields := strings.Fields(expression)
		if len(fields) == 0 {
			continue
		}
		term := orderByTerm{column: expression}
		if len(fields) > 1 {
			direction := upperASCII(fields[len(fields)-1])
			switch direction {
			case "DESC", "DESCENDING":
				term.descending = true
				term.column = strings.TrimSpace(expression[:strings.LastIndex(expression, fields[len(fields)-1])])
			case "ASC", "ASCENDING":
				term.column = strings.TrimSpace(expression[:strings.LastIndex(expression, fields[len(fields)-1])])
			}
		}
		terms = append(terms, term)
	}
	return terms
}

func parseOrderByModifier(modifiers string) (column string, descending bool, ok bool) {
	terms := parseOrderByTerms(modifiers)
	if len(terms) == 0 {
		return "", false, false
	}
	return terms[0].column, terms[0].descending, terms[0].column != ""
}

// parseIntModifier returns the SKIP or LIMIT value in a RETURN/WITH modifier
// tail. A plain integer is read directly; anything else (1 + 1, $s + 1 after
// parameter substitution, toInteger('2')) is evaluated as a constant
// expression through evaluatePipelinePagination, the same evaluation WITH uses,
// so every route applies SKIP/LIMIT expressions instead of ignoring them.
// ok is false when the keyword is absent or the value is not a non-negative
// integer.
func (e *StorageExecutor) parseIntModifier(ctx context.Context, modifiers, keyword string) (value int, ok bool) {
	modifier := intModifierTextFor(ctx, modifiers, keyword)
	if !modifier.found {
		return 0, false
	}
	vs := modifier.text
	if v, err := strconv.Atoi(vs); err == nil {
		return v, true
	}
	if vs == "" {
		return 0, false
	}
	return e.evaluatePipelinePagination(ctx, vs, nil)
}

// intModifier is the text of a LIMIT or SKIP modifier's expression: what
// follows keyword up to the next modifier (found: the keyword is there).
type intModifier struct {
	text  string
	found bool
}

func parseIntModifierText(modifiers, keyword string) intModifier {
	idx := findKeywordIndex(modifiers, keyword)
	if idx == -1 {
		return intModifier{}
	}
	kwPart := strings.TrimSpace(modifiers[idx+len(keyword):])
	nextKw := len(kwPart)
	for _, otherKeyword := range []string{"LIMIT", "SKIP", "ORDER BY"} {
		if otherKeyword == keyword {
			continue
		}
		if kidx := findKeywordIndex(kwPart, otherKeyword); kidx != -1 && kidx < nextKw {
			nextKw = kidx
		}
	}
	return intModifier{text: strings.TrimSpace(kwPart[:nextKw]), found: true}
}
