package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

// caseValuePart identifies an already parsed (and possibly compiled) CASE part.
type caseValuePart uint8

func (parsed *caseExpression) partSpan(part caseValuePart, index int) cyphertext.Span {
	switch part {
	case caseTest:
		return parsed.testSpan
	case caseWhen:
		return parsed.whenClauses[index].conditionSpan
	case caseResult:
		return parsed.whenClauses[index].resultSpan
	default:
		return parsed.elseSpan
	}
}

const (
	caseTest caseValuePart = iota
	caseWhen
	caseResult
	caseElse
)

// runCaseExpression owns CASE selection for context, text and compiled evaluators.
// The adapter evaluates only the requested part; unselected results never run.
func runCaseExpression(parsed *caseExpression, cypher25 bool, evaluate func(caseValuePart, int, string) (interface{}, bool, error)) (interface{}, bool, error) {
	if parsed == nil {
		return nil, false, nil
	}
	var test interface{}
	if parsed.isSimple {
		var ok bool
		var err error
		test, ok, err = evaluate(caseTest, 0, parsed.testExpression)
		if err != nil || !ok {
			return nil, false, err
		}
	}
	for i, clause := range parsed.whenClauses {
		text := clause.condition
		if parsed.isSimple {
			text = clause.value
		}
		value, ok, err := evaluate(caseWhen, i, text)
		if err != nil || !ok {
			return nil, false, err
		}
		if parsed.isSimple {
			left, right := promoteConstantNumbers("=", parsed.testExpression, clause.value, test, value, cypher25)
			value = cypherEquality(left, right)
		}
		truth, err := cypherPredicateTruth(value)
		if err != nil {
			return nil, false, err
		}
		if truth == truthTrue {
			return evaluate(caseResult, i, clause.result)
		}
	}
	if parsed.elseResult == "" {
		return nil, true, nil
	}
	return evaluate(caseElse, 0, parsed.elseResult)
}
