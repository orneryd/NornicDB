package cyphertext

import "strings"

// FilterImportConjuncts removes the imported column's IS NOT NULL guards from
// AND terms supplied by splitConjuncts. Other references to that column reject
// the batched rewrite, leaving ordinary correlated execution to the caller.
// Reference checks
// use policy exactly as FindIdentifier; guard names and IS NOT NULL are always
// case-insensitive, with optional backticks on the guard name. The caller owns
// conjunct splitting and selects its reference policy. Empty input avoids invoking
// the splitter, preserving the allocation-free no-predicate path.
//
// For example, terms {"seed IS NOT NULL", "n.active = true"} and import "seed"
// produce "n.active = true"; {"seed > 0"} rejects the rewrite.
func FilterImportConjuncts(where, importColumn string, policy IdentifierReferencePolicy, splitConjuncts func(string) []string) (string, bool) {
	if strings.TrimSpace(where) == "" {
		return "", true
	}
	terms := splitConjuncts(where)
	kept := make([]string, 0, len(terms))
	guardColumn := strings.TrimSpace(importColumn)
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		parts := strings.Fields(term)
		if len(parts) == 4 && strings.EqualFold(strings.Trim(parts[0], "`"), guardColumn) &&
			strings.EqualFold(parts[1], "IS") && strings.EqualFold(parts[2], "NOT") && strings.EqualFold(parts[3], "NULL") {
			continue
		}
		if FindIdentifier(term, importColumn, 0, policy) >= 0 {
			return "", false
		}
		kept = append(kept, term)
	}
	return strings.Join(kept, " AND "), true
}
