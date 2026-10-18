package cypher

import "strings"

// casePrefixEndRequired asks whether treating the candidate END as a prefix
// operand leaves no complete CASE parse. A state records the CASE stack at one
// bracket level: 'c' awaits THEN, 'v' has a branch value and may close. Other
// prefix-END ambiguities retain both interpretations until a later keyword or
// delimiter resolves them. The existing non-balancing keyword reader prevents
// recursive closure scans and preserves keyword-variable/property handling.
func casePrefixEndRequired(text string, candidate int) bool {
	states := []string{""}
	var parents [][]string
	candidateDepth := -1
	for i := 0; i < len(text); {
		switch cypherLexicalByte(text[i]) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(text, i, text[i])
			continue
		case '/':
			if end := queryCommentEnd(text, i); end > i {
				i = end
				continue
			}
		case '(', '[', '{':
			parents = append(parents, states)
			states = []string{""}
			i++
			continue
		case ')', ']', '}':
			complete := caseClosureStateContains(states, "")
			if candidateDepth == len(parents) {
				return len(states) > 0 && !complete
			}
			if !complete || len(parents) == 0 {
				return false
			}
			states = parents[len(parents)-1]
			parents = parents[:len(parents)-1]
			i++
			continue
		}
		if !isCaseWordStart(text, i) {
			i++
			continue
		}
		word := caseWordAt(text, i)
		if word == "end" && i == candidate {
			// At a complete top-level branch, an operator after END can
			// continue NOT's operand (NOT end + ...). Do not reinterpret it
			// solely to repair a missing closing END in a sibling expression.
			after := queryGapEnd(text, i+len(word))
			if len(states) == 1 && len(states[0]) == 1 && after < len(text) && strings.IndexByte("+-*/%^", text[after]) >= 0 {
				return false
			}
			candidateDepth = len(parents)
		} else {
			states = advanceCaseClosureStates(text, i, word, states)
			if len(states) == 0 {
				return false // Malformed tokens cannot prove an END is required.
			}
		}
		i += len(word)
	}
	return candidateDepth == 0 && !caseClosureStateContains(states, "")
}

func advanceCaseClosureStates(text string, at int, word string, states []string) []string {
	ambiguousEnd := word == "end" && caseEndFollowsBarePrefix(text, at)
	switch word {
	case "case", "when", "then", "else", "end":
		if !ambiguousEnd && !caseKeywordAtMode(text, at, upperASCII(word), false) {
			return states
		}
	default:
		return states
	}
	next := make([]string, 0, len(states)+1)
	for _, state := range states {
		switch word {
		case "case":
			next = appendCaseClosureState(next, state+"c")
		case "when":
			if len(state) > 0 {
				next = appendCaseClosureState(next, state[:len(state)-1]+"c")
			}
		case "then":
			if len(state) > 0 && state[len(state)-1] == 'c' {
				next = appendCaseClosureState(next, state[:len(state)-1]+"v")
			}
		case "else":
			if len(state) > 0 && state[len(state)-1] == 'v' {
				next = appendCaseClosureState(next, state)
			}
		case "end":
			if ambiguousEnd {
				next = appendCaseClosureState(next, state) // END is the operand.
			}
			if len(state) > 0 && state[len(state)-1] == 'v' {
				next = appendCaseClosureState(next, state[:len(state)-1])
			}
		}
	}
	return next
}

func appendCaseClosureState(states []string, state string) []string {
	if !caseClosureStateContains(states, state) {
		return append(states, state)
	}
	return states
}

func caseClosureStateContains(states []string, wanted string) bool {
	for _, state := range states {
		if state == wanted {
			return true
		}
	}
	return false
}

func caseEndFollowsBarePrefix(text string, end int) bool {
	i := end - 1
	for i >= 0 && isASCIISpace(text[i]) {
		i--
	}
	finish := i + 1
	for i >= 0 && isIdentByte(text[i]) {
		i--
	}
	if i >= 0 && (text[i] == '.' || text[i] == ':' || text[i] == '$') {
		return false
	}
	switch lowerASCII(text[i+1 : finish]) {
	case "not", "distinct":
		return true
	}
	return false
}
