package cypher

import "strings"

// Neo4j runs every clause of a statement over all its rows before the next
// clause reads, so a clause never sees what a later row of an earlier clause
// will write, and always sees what every row of an earlier clause wrote.
// runPipelineClauseRows runs a stretch of clauses that writes one row at a
// time through all of them instead (row-ordered writes, #772). That answers
// the same only while no clause of the stretch reads what another clause of
// it writes: pipelineWriteStretchConflicts finds such a pair, and the stretch
// then runs clause by clause, as Neo4j's planner puts an Eager barrier there
// (#907). A MERGE or SET reading what its own earlier rows wrote is Neo4j's
// behaviour too, so a clause never conflicts with itself.

// stretchTokens is what one clause of a write stretch reads or writes: node
// labels, relationship types and property keys. anyNode / anyRelationship /
// anyKey stand for ones the clause doesn't name (an unlabeled node pattern
// reads every node; SET n = map writes any key); everything for a clause the
// analysis can't see into (DELETE, FOREACH, CALL).
type stretchTokens struct {
	labels, types, keys              map[string]struct{}
	anyNode, anyRelationship, anyKey bool
	everything                       bool
}

func (tokens *stretchTokens) add(set *map[string]struct{}, name string) {
	if name == "" {
		return
	}
	if *set == nil {
		*set = make(map[string]struct{})
	}
	(*set)[name] = struct{}{}
}

func (tokens *stretchTokens) empty() bool {
	return !tokens.everything && !tokens.anyNode && !tokens.anyRelationship && !tokens.anyKey &&
		len(tokens.labels) == 0 && len(tokens.types) == 0 && len(tokens.keys) == 0
}

// observes reports whether reads can see something writes changes.
func (reads *stretchTokens) observes(writes *stretchTokens) bool {
	if reads.empty() || writes.empty() {
		return false
	}
	if reads.everything || writes.everything {
		return true
	}
	return stretchSetsMeet(reads.labels, writes.labels, reads.anyNode, writes.anyNode) ||
		stretchSetsMeet(reads.types, writes.types, reads.anyRelationship, writes.anyRelationship) ||
		stretchSetsMeet(reads.keys, writes.keys, reads.anyKey, writes.anyKey)
}

func stretchSetsMeet(read, written map[string]struct{}, readAny, writtenAny bool) bool {
	if (readAny && (writtenAny || len(written) > 0)) || (writtenAny && len(read) > 0) {
		return true
	}
	for name := range read {
		if _, found := written[name]; found {
			return true
		}
	}
	return false
}

// pipelineWriteStretchConflicts reports whether some clause of stretch reads
// a label, relationship type or property key another clause of it writes.
func pipelineWriteStretchConflicts(stretch []pipelineClause) bool {
	reads := make([]stretchTokens, len(stretch))
	writes := make([]stretchTokens, len(stretch))
	for index, clause := range stretch {
		analyzeStretchClause(clause, &reads[index], &writes[index])
	}
	for reader := range stretch {
		for writer := range stretch {
			if reader != writer && reads[reader].observes(&writes[writer]) {
				return true
			}
		}
	}
	return false
}

// analyzeStretchClause records what clause reads and writes.
func analyzeStretchClause(clause pipelineClause, reads, writes *stretchTokens) {
	text := clause.text
	switch clause.kind {
	case pipelineClauseMatch, pipelineClauseOptionalMatch:
		keyword := "MATCH"
		if clause.kind == pipelineClauseOptionalMatch {
			keyword = "OPTIONAL MATCH"
		}
		body := pipelineClauseBody(text, keyword)
		pattern := body
		if where := topLevelKeywordIndex(body, "WHERE"); where >= 0 {
			pattern = body[:where]
		}
		addStretchPattern(pattern, reads, false)
		addStretchPropertyReads(body, reads)
	case pipelineClauseMerge:
		pattern, onCreate, onMatch := splitMergeClauseActions(strings.TrimSpace(pipelineClauseBody(text, "MERGE")))
		addStretchPattern(pattern, reads, false)
		addStretchPattern(pattern, writes, true)
		addStretchPropertyReads(pattern, reads)
		addStretchSetItems(onCreate, reads, writes)
		addStretchSetItems(onMatch, reads, writes)
	case pipelineClauseCreate:
		pattern := pipelineClauseBody(text, "CREATE")
		addStretchPattern(pattern, writes, true)
		addStretchPropertyReads(pattern, reads)
	case pipelineClauseSet:
		addStretchSetItems(pipelineClauseBody(text, "SET"), reads, writes)
	case pipelineClauseRemove:
		for _, item := range splitTopLevelComma(pipelineClauseBody(text, "REMOVE")) {
			item = strings.TrimSpace(item)
			if _, property, isProperty := parseVarPropertyRef(item); isProperty {
				writes.add(&writes.keys, property)
			} else if colon := indexByteOutsideBackticks(item, ':'); colon > 0 {
				for _, label := range strings.Split(item[colon+1:], ":") {
					writes.add(&writes.labels, strings.Trim(strings.TrimSpace(label), "`"))
				}
			} else {
				writes.everything = true
			}
		}
	case pipelineClauseWith, pipelineClauseUnwind:
		addStretchPropertyReads(text, reads)
	default:
		// DELETE, FOREACH, CALL and anything else: assume it reads and
		// writes everything.
		reads.everything = true
		writes.everything = true
	}
}

// addStretchPattern records the labels, relationship types and property keys
// of pattern's nodes and relationships; an element without labels or types
// stands for any. For a pattern a clause writes (written), a node that is only
// a variable ((a) in CREATE (a)-[:R]->(b)) is one bound before, not a node
// the clause creates.
func addStretchPattern(pattern string, tokens *stretchTokens, written bool) {
	eachPatternElement(pattern, func(opener byte, element string) {
		inner := element[1 : len(element)-1]
		head := inner
		brace := indexByteOutsideBackticks(inner, '{')
		if brace >= 0 {
			head = inner[:brace]
			addStretchMapKeys(inner[brace:], tokens)
		}
		if opener == '(' {
			variable, chain, hasLabels := splitNodeHead(head)
			if !hasLabels {
				if written && brace < 0 && variable != "" {
					return
				}
				tokens.anyNode = true
				return
			}
			for _, label := range strings.FieldsFunc(chain, func(r rune) bool { return r == ':' || r == '&' || r == '|' || r == ' ' }) {
				tokens.add(&tokens.labels, strings.Trim(label, "`!"))
			}
			return
		}
		if star := strings.IndexByte(head, '*'); star >= 0 {
			head = head[:star]
		}
		colon := indexByteOutsideBackticks(head, ':')
		if colon < 0 {
			tokens.anyRelationship = true
			return
		}
		for _, relationshipType := range strings.Split(head[colon+1:], "|") {
			tokens.add(&tokens.types, strings.Trim(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(relationshipType), ":")), "`!"))
		}
	})
}

// addStretchMapKeys records the keys of a "{k: v, …}" map.
func addStretchMapKeys(text string, tokens *stretchTokens) {
	end := findMatchingDelimiter(text, 0, '{', '}')
	if end < 0 {
		return
	}
	for _, entry := range splitTopLevelComma(text[1:end]) {
		if colon := topLevelColonIndex(entry); colon > 0 {
			tokens.add(&tokens.keys, strings.Trim(strings.TrimSpace(entry[:colon]), "`"))
		}
	}
}

// addStretchSetItems records what SET items (or an ON CREATE / ON MATCH SET
// list) write, and the properties their values read.
func addStretchSetItems(list string, reads, writes *stretchTokens) {
	for _, item := range splitSetAssignments(list) {
		_, property, operator, right := splitSetAssignment(item)
		switch {
		case operator == ":":
			if strings.HasPrefix(strings.TrimSpace(right), "$") {
				writes.anyNode = true
				continue
			}
			for _, label := range strings.Split(right, ":") {
				writes.add(&writes.labels, strings.Trim(strings.TrimSpace(label), "`"))
			}
		case property != "":
			writes.add(&writes.keys, property)
		default:
			writes.anyKey = true
		}
		addStretchPropertyReads(right, reads)
	}
}

// addStretchPropertyReads records the property keys text reads: v.key
// outside string literals, and any key for v {.*}, properties() or keys().
func addStretchPropertyReads(text string, tokens *stretchTokens) {
	lower := lowerASCII(text)
	if strings.Contains(text, ".*") || strings.Contains(lower, "properties(") || strings.Contains(lower, "keys(") {
		tokens.anyKey = true
	}
	for index := 0; index < len(text); index++ {
		switch character := text[index]; {
		case character == '\'' || character == '"' || character == '`':
			index = skipCypherQuotedText(text, index, character) - 1
		case character == '.' && index > 0 && isCypherIdentByte(text[index-1]) && index+1 < len(text) && isIdentStartByte(text[index+1]):
			end := index + 1
			for end < len(text) && isCypherIdentByte(text[end]) {
				end++
			}
			tokens.add(&tokens.keys, text[index+1:end])
			index = end - 1
		}
	}
}

func isIdentStartByte(character byte) bool {
	return character == '_' || (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
}
