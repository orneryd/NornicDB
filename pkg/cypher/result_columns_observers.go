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
	active := []observer{{root: -1, enter: pipelineEffectVisitor(rootEffects)}}
	return func(node cypherantlr.SyntaxNode) func() {
		if clause, found := compilation.clauses[node.ID()]; found {
			effects := &pipelineClauseEffects{}
			active = append(active, observer{root: node.ID(), enter: pipelineEffectVisitor(effects), finish: func() {
				compilation.product.effects[node.ID()] = *effects
				pipelineClauseEffectCache.put(clause.text, *effects)
			}})
		}
		if node.Rule() == cypherantlr.CypherParserRULE_expression {
			shape := &scalarExpressionSyntax{}
			active = append(active, observer{root: node.ID(), enter: scalarAdmissionVisitor(shape), finish: func() {
				compilation.product.scalars[node.ID()] = *shape
				scalarExpressionSyntaxCache.put(node.Text(), *shape)
			}})
		}
		var leaves []func()
		for _, observer := range active {
			if leave := observer.enter(node); leave != nil {
				leaves = append(leaves, leave)
			}
		}
		// Most grammar nodes neither change observer state nor finish an
		// observer. They need no leave list or restoration closure.
		if len(leaves) == 0 && (len(active) == 1 || active[len(active)-1].root != node.ID()) {
			return nil
		}
		return func() {
			for index := len(leaves) - 1; index >= 0; index-- {
				leaves[index]()
			}
			for len(active) > 1 && active[len(active)-1].root == node.ID() {
				observer := active[len(active)-1]
				observer.finish()
				active = active[:len(active)-1]
			}
		}
	}
}
