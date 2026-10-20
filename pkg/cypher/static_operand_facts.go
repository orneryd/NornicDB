package cypher

// Exclusions can be explicit constraints or implied by a known category. Joins
// must retain a guaranteed fact regardless of which representation supplied it.
func (operand staticOperand) excludesInteger() bool {
	return operand.nonInteger || operand.known() && operand.kind != "Integer" && operand.kind != "Number" && operand.kind != "Float, Integer or Number" && operand.kind != "Null"
}

func (operand staticOperand) excludesBoolean() bool {
	return operand.nonBoolean || operand.known() && operand.kind != "Boolean" && operand.kind != "Null"
}

func (operand staticOperand) excludesEntity() bool {
	if operand.nonEntity {
		return true
	}
	if len(operand.alternatives) > 0 {
		for _, kind := range operand.alternatives {
			if !knownOperand(kind).excludesEntity() {
				return false
			}
		}
		return true
	}
	switch operand.kind {
	case "", "Null", "Node", "Relationship", "Path", "List<Node>", "List<Relationship>", "List<T>":
		return false
	}
	return true
}

func joinedOperandAlternatives(left, right staticOperand) ([]string, bool) {
	choices := func(operand staticOperand) []string {
		if len(operand.alternatives) != 0 {
			return operand.alternatives
		}
		if operand.known() {
			return []string{operand.kind}
		}
		return nil
	}
	l, r := choices(left), choices(right)
	if len(l) == 0 || len(r) == 0 {
		return nil, false
	}
	seen := make(map[string]bool, len(l)+len(r))
	var result []string
	for _, alternatives := range [][]string{l, r} {
		for _, name := range alternatives {
			if !seen[name] {
				seen[name] = true
				result = append(result, name)
			}
		}
	}
	return result, true
}
