package cypher

import cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

// metadataVisitor folds clause effects and scalar admission in the same
// traversal as binding. Each observer restores its own state; none may prune
// a CASE arm, argument, nested body or other dependency representation.
func (compilation *boundCompilation) metadataVisitor(rootEffects *pipelineClauseEffects) cypherantlr.SyntaxVisitor {
	type observer struct {
		root   int
		enter  cypherantlr.SyntaxVisitor
		finish func()
	}
	active := []observer{{root: -1, enter: pipelineEffectVisitor(compilation.view, rootEffects)}}
	return func(node cypherantlr.SyntaxNode) func() {
		if _, found := compilation.clauses[node.ID()]; found {
			effects := &pipelineClauseEffects{}
			active = append(active, observer{root: node.ID(), enter: pipelineEffectVisitor(compilation.view, effects), finish: func() {
				compilation.product.effects[node.ID()] = *effects
			}})
		}
		if node.Rule() == cypherantlr.CypherParserRULE_expression {
			shape := &scalarExpressionSyntax{}
			active = append(active, observer{root: node.ID(), enter: scalarAdmissionVisitor(shape), finish: func() {
				compilation.product.scalars[node.ID()] = *shape
				scalarExpressionSyntaxCache.put(node.Text(), *shape)
			}})
		}
		leaves := make([]func(), len(active))
		for index, observer := range active {
			leaves[index] = observer.enter(node)
		}
		return func() {
			for index := len(leaves) - 1; index >= 0; index-- {
				if leaves[index] != nil {
					leaves[index]()
				}
			}
			for len(active) > 1 && active[len(active)-1].root == node.ID() {
				observer := active[len(active)-1]
				observer.finish()
				active = active[:len(active)-1]
			}
		}
	}
}
