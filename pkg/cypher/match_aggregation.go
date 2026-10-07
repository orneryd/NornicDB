package cypher

// nodeOrderSpec represents a single ORDER BY specification for nodes
type nodeOrderSpec struct {
	propName   string
	descending bool
}

// orderNodes sorts nodes by the given expression, supporting multiple columns
