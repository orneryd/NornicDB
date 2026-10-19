package cypher

import "strings"

// rowEndpointPropertyNode selects a property without decoding the endpoint's
// other properties. Both the text evaluator and compiled value program use
// this same reader and the ordinary endpoint existence/type contract.
type rowEndpointPropertyNode struct {
	argument, property string
	end                bool
}

func endpointPropertyNode(base, property string) *rowEndpointPropertyNode {
	function, argument, call := parseFunctionCallWS(base)
	if !call || (!equalFoldASCII(function, "startNode") && !equalFoldASCII(function, "endNode")) || !isValidIdentifier(property) {
		return nil
	}
	return &rowEndpointPropertyNode{argument: strings.TrimSpace(argument), property: property, end: equalFoldASCII(function, "endNode")}
}

func (n *rowEndpointPropertyNode) eval(e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	value, resolved, err := e.evaluateRowValue(n.argument, values)
	if !resolved || err != nil {
		return nil, resolved, err
	}
	edge, err := relationshipEndpointArgument(value, n.end)
	if err != nil {
		return nil, false, err
	}
	node := e.relationshipEndpointWithProperties(edge, n.end, []string{n.property})
	value, ok := evaluateRowPropertyChain(e.queryVisibleValue(node), n.property)
	return value, ok, nil
}
