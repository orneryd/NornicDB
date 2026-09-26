package cypher

// rowPropertyChainShape reports whether expr is exactly identifiers joined by
// dots (e.uuid, n.a.b), and returns the first identifier and the chain after
// it. Anything else (literals, backticks, calls, operators, spaces) is not.
func rowPropertyChainShape(expr string) (variable, chain string, ok bool) {
	dot := -1
	start := true
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch {
		case c == '.':
			if start || i == len(expr)-1 {
				return "", "", false
			}
			if dot < 0 {
				dot = i
			}
			start = true
		case c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
			start = false
		case c >= '0' && c <= '9':
			if start {
				return "", "", false
			}
		default:
			return "", "", false
		}
	}
	if dot < 0 {
		return "", "", false
	}
	return expr[:dot], expr[dot+1:], true
}
