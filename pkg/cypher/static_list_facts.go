package cypher

// staticListFacts is the shared Cypher list-literal policy. Unlike a CASE
// alternative, different elements coexist: a heterogeneous literal has an
// open element type. Homogeneous nested facts stay precise at every depth.
type staticListFacts struct {
	element              string
	count                int
	homogeneous, numeric bool
}

func (facts *staticListFacts) add(operand staticOperand) {
	name := operand.typeName()
	if facts.count == 0 {
		facts.element, facts.homogeneous, facts.numeric = name, name != "" && name != "Null", operand.numeric()
	} else {
		facts.homogeneous = facts.homogeneous && name == facts.element
		facts.numeric = facts.numeric && operand.numeric()
	}
	facts.count++
}

func (facts staticListFacts) operand() staticOperand {
	name := facts.element
	if !facts.homogeneous {
		if !facts.numeric {
			return knownOperand("List<T>")
		}
		name = "Float, Integer or Number"
	}
	if name == "Map" {
		name = "Map, Node or Relationship"
	}
	return typedOperand(listOfType(name))
}
