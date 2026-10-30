package cypher

// Author: #1663 implementation lane. This adapter consumes the single shared
// declaration/admission product. It contains no declaration or brace grammar.
func knowledgePolicyCommand(view braceView) (any, bool, error) {
	command, handled, err := view.syntax.product.PolicyCommand(true)
	if err != nil {
		return nil, handled, err
	}
	if view.syntax.err != nil {
		return nil, handled, view.syntax.err
	}
	return command, handled, nil
}
