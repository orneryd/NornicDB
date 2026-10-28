package cypher

// inlineOverflowAt owns physical slot selection for parser buffers with an
// inline prefix. Their existing callers retain logical length and admission;
// the buffer's capacity and element layout remain unchanged.
func inlineOverflowAt[T any](inline, overflow []T, index int) T {
	if index < len(inline) {
		return inline[index]
	}
	return overflow[index-len(inline)]
}
