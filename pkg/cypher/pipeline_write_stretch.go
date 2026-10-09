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
// a label, relationship type or property key another clause of it writes,
// or a clause after the stretch (after: its RETURN, its later WITHs) reads
// one the stretch writes. A row carries the entities it matched as they were
// then, so a later clause would read a row's entity before later rows of the
// stretch wrote it; Neo4j reads it after every row did.
func pipelineWriteStretchConflicts(stretch, after []pipelineClause) bool {
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
	for _, clause := range after {
		var laterReads, laterWrites stretchTokens
		analyzeStretchClause(clause, &laterReads, &laterWrites)
		for writer := range stretch {
			if laterReads.observes(&writes[writer]) {
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
		addStretchExpressionReads(body[len(pattern):], reads)
		addStretchPropertyReads(pattern, reads)
	case pipelineClauseMerge:
		actions := splitMergeClauseActions(strings.TrimSpace(pipelineClauseBody(text, "MERGE")))
		addStretchPattern(actions.pattern, reads, false)
		addStretchPattern(actions.pattern, writes, true)
		addStretchPropertyReads(actions.pattern, reads)
		for _, item := range mergeActionAssignments(actions.onCreate) {
			addStretchSetItems(item, reads, writes)
		}
		for _, item := range mergeActionAssignments(actions.onMatch) {
			addStretchSetItems(item, reads, writes)
		}
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
				addStretchLabelChain(item[colon:], writes)
			} else {
				writes.everything = true
			}
		}
	case pipelineClauseWith, pipelineClauseUnwind, pipelineClauseReturn:
		addStretchExpressionReads(text, reads)
	case pipelineClauseCall:
		// A procedure the registry knows to be read-only only reads.
		reads.everything = true
		if procedure, found := globalProcedureRegistry.Get(extractProcedureName(text)); !found || procedure.Spec.Mode == ProcedureModeWrite {
			writes.everything = true
		}
	default:
		// DELETE, FOREACH, CALL subqueries and anything else: assume it
		// reads and writes everything.
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
			addStretchLabelChain(":"+chain, tokens)
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
		typeText := head[colon+1:]
		if strings.ContainsAny(typeText, "`$&!%()") {
			// A quoted, dynamic or expression type: any relationship.
			tokens.anyRelationship = true
			return
		}
		for _, relationshipType := range strings.Split(typeText, "|") {
			tokens.add(&tokens.types, strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(relationshipType), ":")))
		}
	})
}

// addStretchLabelChain records the labels of a chain (":A:`B C`"), read by
// the label-chain owner (labelChainNames). A label expression (A|B, !A, %),
// a dynamic label ($(e)) or a chain the owner can't read stands for any
// label.
func addStretchLabelChain(chain string, tokens *stretchTokens) {
	if strings.ContainsAny(chain, "$&|!%()") {
		tokens.anyNode = true
		return
	}
	names := labelChainNames(chain)
	if len(names) == 0 {
		tokens.anyNode = true
		return
	}
	for _, name := range names {
		tokens.add(&tokens.labels, name)
	}
}

// addStretchExpressionReads records what an expression text (a WHERE, a
// WITH or UNWIND, a SET value) reads: property keys
// (addStretchPropertyReads), labels tested with v:Label or labels(v), and
// for a pattern predicate or a subquery (EXISTS { … }, COUNT { … },
// COLLECT { … }, (a)-[:R]->(b), a pattern comprehension) any node and any
// relationship, since the analysis doesn't model their patterns.
func addStretchExpressionReads(text string, tokens *stretchTokens) {
	addStretchPropertyReads(text, tokens)
	lower := lowerASCII(text)
	if strings.Contains(lower, "labels(") {
		tokens.anyNode = true
	}
	if strings.Contains(text, "-[") || strings.Contains(text, "]-") || strings.Contains(text, ")-") ||
		strings.Contains(text, "<-") || strings.Contains(text, "-(") ||
		strings.Contains(lower, "exists") || strings.Contains(lower, "count {") || strings.Contains(lower, "count{") ||
		strings.Contains(lower, "collect {") || strings.Contains(lower, "collect{") {
		tokens.anyNode = true
		tokens.anyRelationship = true
	}
	depth := 0
	for index := 0; index < len(text); index++ {
		switch character := text[index]; character {
		case '\'', '"', '`':
			index = skipCypherQuotedText(text, index, character) - 1
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case ':':
			// v:Label (a label test) outside a map literal's keys.
			if depth == 0 && index > 0 && (isIdentByte(text[index-1]) || text[index-1] == '`' || text[index-1] == ')') {
				end := index
				for end < len(text) && (isIdentByte(text[end]) || strings.IndexByte(":`|&!%", text[end]) >= 0) {
					if text[end] == '`' {
						end = skipCypherQuotedText(text, end, '`')
						continue
					}
					end++
				}
				addStretchLabelChain(strings.TrimSpace(text[index:end]), tokens)
				index = end - 1
			}
		}
	}
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
			addStretchLabelChain(":"+right, writes)
			continue
		case property != "":
			writes.add(&writes.keys, property)
		default:
			writes.anyKey = true
		}
		addStretchExpressionReads(right, reads)
	}
}

// addStretchPropertyReads records the property keys text reads: v.key
// outside string literals. Any key for what this scan can't name: v {.*} or
// any map projection, properties(), keys(), a quoted key (v.`key`), spaces
// around the dot, and a subscript (v['key'], v[$k]), which may read any key.
func addStretchPropertyReads(text string, tokens *stretchTokens) {
	lower := lowerASCII(text)
	if strings.Contains(text, ".*") || strings.Contains(text, "{.") || strings.Contains(text, "{ .") ||
		strings.Contains(lower, "properties(") || strings.Contains(lower, "keys(") {
		tokens.anyKey = true
	}
	for index := 0; index < len(text); index++ {
		switch character := text[index]; {
		case character == '\'' || character == '"' || character == '`':
			index = skipCypherQuotedText(text, index, character) - 1
		case character == '[' && index > 0 && (isIdentByte(text[index-1]) || text[index-1] == ')' || text[index-1] == ']'):
			tokens.anyKey = true
		case character == '.' && index+1 < len(text) && text[index+1] != '.' && (index == 0 || text[index-1] != '.'):
			before, after := index, index+1
			for before > 0 && isASCIISpace(text[before-1]) {
				before--
			}
			for after < len(text) && isASCIISpace(text[after]) {
				after++
			}
			if before == 0 || !(isIdentByte(text[before-1]) || text[before-1] == ')' || text[before-1] == '`') || after >= len(text) {
				continue
			}
			if before != index || after != index+1 || text[after] == '`' || !isIdentStartByte(text[after]) {
				if text[after] == '`' || isIdentStartByte(text[after]) {
					tokens.anyKey = true
				}
				continue
			}
			end := after
			for end < len(text) && isIdentByte(text[end]) {
				end++
			}
			tokens.add(&tokens.keys, text[after:end])
			index = end - 1
		}
	}
}
