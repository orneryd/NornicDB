package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// parseQuantifierArguments splits the arguments of all / any / none /
// single, "variable IN list WHERE predicate", for every evaluator.
func parseQuantifierArguments(view braceView) (variable, listExpression, predicate string, ok bool) {
	inner := view.text()
	parts, ok := parseQuantifierSpans(view)
	if !ok {
		return "", "", "", false
	}
	return parts.variable, strings.TrimSpace(inner[parts.list.Start:parts.list.End]), strings.TrimSpace(inner[parts.predicate.Start:parts.predicate.End]), true
}

func parseQuantifierSpans(view braceView) (comprehensionSpans, bool) {
	inner := view.text()
	variable, _, listStart, declared := bindingDeclaration(inner, 0)
	if !declared {
		return comprehensionSpans{}, false
	}
	where := topLevelKeywordIndex(view.window(listStart, len(inner)), "WHERE")
	if where < 0 {
		return comprehensionSpans{}, false
	}
	parts := comprehensionSpans{variable: variable,
		list:      cyphertext.Span{Start: listStart, End: listStart + where},
		predicate: cyphertext.Span{Start: listStart + where + len("WHERE"), End: len(inner)}}
	if strings.TrimSpace(inner[parts.list.Start:parts.list.End]) == "" || strings.TrimSpace(inner[parts.predicate.Start:parts.predicate.End]) == "" {
		return comprehensionSpans{}, false
	}
	return parts, true
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
