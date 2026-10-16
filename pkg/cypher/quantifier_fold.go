package cypher

// parseQuantifierArguments splits the arguments of all / any / none /
// single, "variable IN list WHERE predicate", for every evaluator.
func parseQuantifierArguments(inner string) (string, string, string, bool) {
	return parseQuantifierArgumentsWithCancellation(inner, nil)
}
func parseQuantifierArgumentsWithCancellation(inner string, probe *pipelineCancellation) (variable, listExpression, predicate string, ok bool) {
	variable, _, listStart, declared := bindingDeclarationWithCancellation(inner, 0, probe)
	if !declared {
		return "", "", "", false
	}
	rest := inner[listStart:]
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	whereIndex := keywordIndexFromWithCancellation(rest, "WHERE", 0, opts, probe)
	if whereIndex < 0 {
		return "", "", "", false
	}
	listExpression = trimSpaceWithCancellation(rest[:whereIndex], probe)
	predicate = trimSpaceWithCancellation(rest[whereIndex+len("WHERE"):], probe)
	if listExpression == "" || predicate == "" || probe != nil && probe.err != nil {
		return "", "", "", false
	}
	return variable, listExpression, predicate, true
}

// isQuantifierFunction reports whether name (lower case) is a list
// predicate: all, any, none or single.
func isQuantifierFunction(name string) bool {
	switch name {
	case "all", "any", "none", "single":
		return true
	}
	return false
}

// quantifierFold folds the per-element predicate results of a list predicate
// (all / any / none / single) with Cypher's three-valued rules. An element
// whose predicate is null makes the result null unless the answer is already
// decided: all by a false, any by a true, none by a true, single by a second
// true. A null list is null (#736). The row evaluator and the shared
// evaluator both fold through it.
type quantifierFold struct {
	function  string
	trueCount int
	sawNull   bool
}

// add records one element's predicate result (a bool, or nil for null) and
// returns the predicate's value once it is decided.
func (q *quantifierFold) add(result interface{}) (interface{}, bool) {
	boolean, ok := result.(bool)
	if !ok {
		q.sawNull = true
		return nil, false
	}
	if boolean {
		q.trueCount++
	}
	switch q.function {
	case "all":
		if !boolean {
			return false, true
		}
	case "any", "none":
		if boolean {
			return q.function == "any", true
		}
	case "single":
		if q.trueCount > 1 {
			return false, true
		}
	}
	return nil, false
}

// result is the predicate's value after every element was added undecided.
func (q *quantifierFold) result() interface{} {
	if q.sawNull {
		return nil
	}
	switch q.function {
	case "all", "none":
		return true
	case "any":
		return false
	}
	return q.trueCount == 1
}
