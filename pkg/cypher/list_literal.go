package cypher

// isListLiteral asks the shared group reader whether text is one bracket
// group. A subscript, slice or concatenation is an expression over a list.
func isListLiteral(text string) bool {
	_, enclosed := lexicalEnclosedGroup(text, '[', ']')
	return enclosed
}
