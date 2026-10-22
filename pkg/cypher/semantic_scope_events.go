package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// semanticScopeRead retains the grammar identity and lexical window of a read.
// Both the typed walk and statement binder consume this ordered description.
// Nested query bodies own their reads; projection tails do not promote names
// into their enclosing projection. No row values participate in these facts.
type semanticScopeRead struct {
	node           cypherantlr.SyntaxNode
	tail           bool
	let            int
	lexical, reuse bool
}

var semanticScopeReadCache = newBoundedCache[cypherantlr.SyntaxNode, []semanticScopeRead](4096, func(node cypherantlr.SyntaxNode) int {
	return boundedCacheStringShardIndex(node.Text())
})

func semanticScopeReads(source cypherantlr.SyntaxNode) []semanticScopeRead {
	if !source.Valid() {
		return nil
	}
	if reads, found := semanticScopeReadCache.get(source); found {
		return reads
	}
	var reads []semanticScopeRead
	depth := 0
	letIndex := -1
	source.Walk(func(node cypherantlr.SyntaxNode) func() {
		// A CALL clause owns its header, but its body owns the reads below
		// subqueryBody even when CALL itself is this traversal's root.
		// Importing-WITH reads are reported through referenceCallHeaders.
		if source.Rule() == cypherantlr.CypherParserRULE_callSubquery && node.Rule() == cypherantlr.CypherParserRULE_subqueryBody {
			depth++
			return func() { depth-- }
		}
		if node.ID() != source.ID() {
			switch node.Rule() {
			case cypherantlr.CypherParserRULE_callSubquery, cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist, cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery:
				depth++
				return func() { depth-- }
			}
		}
		if depth == 0 && node.Rule() == cypherantlr.CypherParserRULE_letItem {
			letIndex++
		}
		if depth != 0 || !variableSyntaxRead(node) {
			return nil
		}
		read := semanticScopeRead{node: node, let: -1}
		name, _ := node.Name()
		if binding, found := syntaxLocalBindingAt(node, source, name); found {
			read.lexical, read.reuse = true, binding.reuse
		}
		for parent, ok := node.Parent(); ok && parent.ID() != source.ID(); parent, ok = parent.Parent() {
			switch parent.Rule() {
			case cypherantlr.CypherParserRULE_where, cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
				owner, direct := parent.Parent()
				read.tail = direct && owner.ID() == source.ID()
			case cypherantlr.CypherParserRULE_letItem:
				read.let = letIndex
			}
		}
		reads = append(reads, read)
		return nil
	})
	semanticScopeReadCache.put(source, reads)
	return reads
}

func (body *semanticBodyScope) readEvent(read semanticScopeRead, source cypherantlr.SyntaxNode, local typeScope, proof *semanticDependencyScope) {
	name, _ := read.node.Name()
	if read.lexical && (!read.reuse || !body.readable(name, local)) {
		return
	}
	if !read.tail {
		body.reference(name, local)
	}
	proof.read(name)
}

func (body *semanticBodyScope) readable(name string, local typeScope) bool {
	if _, found := local[name]; found {
		return true
	}
	if body != nil {
		_, found := body.carried[name]
		return found
	}
	return false
}

func (body *semanticBodyScope) readClause(ctx context.Context, clause pipelineClause, local typeScope, proof *semanticDependencyScope) {
	if !clause.syntax.Valid() {
		proof.incomplete()
		return
	}
	var tail *semanticDependencyScope
	for _, read := range semanticScopeReads(clause.syntax) {
		// LET is consumed item by item, after each previous alias is bound.
		if read.let >= 0 {
			continue
		}
		current := proof
		if read.tail && (clause.kind == pipelineClauseWith || clause.kind == pipelineClauseReturn) {
			if tail == nil {
				tail = proof.projectionTail(ctx, local, clause.text)
			}
			current = tail
		}
		body.readEvent(read, clause.syntax, local, current)
	}
}

func (body *semanticBodyScope) readLetItem(clause pipelineClause, index int, local typeScope, proof *semanticDependencyScope) {
	for _, read := range semanticScopeReads(clause.syntax) {
		if read.let == index {
			body.readEvent(read, clause.syntax, local, proof)
		}
	}
}
