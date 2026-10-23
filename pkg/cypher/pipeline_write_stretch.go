package cypher

import (
	"context"
	"strings"
)

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

func (tokens *stretchTokens) include(other *stretchTokens) {
	for name := range other.labels {
		tokens.add(&tokens.labels, name)
	}
	for name := range other.types {
		tokens.add(&tokens.types, name)
	}
	for name := range other.keys {
		tokens.add(&tokens.keys, name)
	}
	tokens.anyNode = tokens.anyNode || other.anyNode
	tokens.anyRelationship = tokens.anyRelationship || other.anyRelationship
	tokens.anyKey = tokens.anyKey || other.anyKey
	tokens.everything = tokens.everything || other.everything
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
func pipelineWriteStretchConflicts(ctx context.Context, stretch, after []pipelineClause) bool {
	reads := make([]stretchTokens, len(stretch))
	writes := make([]stretchTokens, len(stretch))
	for index, clause := range stretch {
		analyzeStretchClause(ctx, clause, &reads[index], &writes[index])
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
		analyzeStretchClause(ctx, clause, &laterReads, &laterWrites)
		for writer := range stretch {
			if laterReads.observes(&writes[writer]) {
				return true
			}
		}
	}
	return false
}

// analyzeStretchClause is the write-stretch consumer of the shared effect
// owner. It has no clause or expression scanner of its own.
func analyzeStretchClause(ctx context.Context, clause pipelineClause, reads, writes *stretchTokens) {
	effects, err := pipelineClauseEffectsFor(ctx, clause.text)
	if err != nil {
		reads.everything = true
		writes.everything = true
		return
	}
	reads.include(&effects.reads)
	// Procedures consume their input inside a CALL chain, but an ordinary
	// write stretch must preserve their opaque reads across earlier rows.
	if effects.opaqueReads {
		reads.everything = true
	}
	writes.include(&effects.writes)
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

// addStretchSetWrites projects the existing mutation item reader onto writes.
// Expression reads are supplied exclusively by the shared grammar-tree walk.
func addStretchSetWrites(list string, writes *stretchTokens) {
	for _, item := range splitSetAssignments(list) {
		_, property, operator, right := splitSetAssignment(item)
		switch {
		case operator == ":":
			addStretchLabelChain(":"+right, writes)
		case operator == "[]=":
			writes.anyKey = true
		case property != "":
			writes.add(&writes.keys, property)
		default:
			writes.anyKey = true
		}
	}
}

func addStretchRemoveWrites(list string, writes *stretchTokens) {
	items, err := parseRemoveItems(list)
	if err != nil {
		writes.everything = true
		return
	}
	for _, item := range items {
		switch {
		case len(item.labels) > 0:
			for _, label := range item.labels {
				if label.expression != "" {
					writes.anyNode = true
				} else {
					writes.add(&writes.labels, label.name)
				}
			}
		case item.key != "":
			writes.anyKey = true
		default:
			writes.add(&writes.keys, item.property)
		}
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
