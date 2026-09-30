package cypher

import "strings"

func parsePipelineForeach(clause string) (string, string, []pipelineClause, error) {
	invalid := func() error {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidForeach", "invalid or unsupported FOREACH update")
	}
	open := strings.Index(clause, "(")
	if open < 0 || !strings.EqualFold(strings.TrimSpace(clause[:open]), "FOREACH") {
		return "", "", nil, invalid()
	}
	close := findMatchingParen(clause, open)
	if close < 0 || strings.TrimSpace(clause[close+1:]) != "" {
		return "", "", nil, invalid()
	}
	inner := clause[open+1 : close]
	inIndex := topLevelKeywordIndex(inner, "IN")
	if inIndex < 0 {
		return "", "", nil, invalid()
	}
	variable := strings.TrimSpace(inner[:inIndex])
	if !isValidIdentifier(variable) {
		return "", "", nil, invalid()
	}
	remainder := inner[inIndex+len("IN"):]
	pipeIndex := findTopLevelByte(remainder, '|')
	if pipeIndex < 0 {
		return "", "", nil, invalid()
	}
	listExpression := strings.TrimSpace(remainder[:pipeIndex])
	updates, supported := splitPipelineClauses(strings.TrimSpace(remainder[pipeIndex+1:]))
	if !supported || len(updates) == 0 {
		return "", "", nil, invalid()
	}
	for _, update := range updates {
		switch update.kind {
		case pipelineClauseCreate, pipelineClauseSet, pipelineClauseMerge, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseForeach:
		default:
			return "", "", nil, invalid()
		}
	}
	return variable, listExpression, updates, nil
}
