package cyphertext

import "strings"

// SymbolicNameValue decodes a symbolic name as written. Backtick delimiters
// disappear and a doubled backtick becomes one backtick; plain names stay as is.
func SymbolicNameValue(written string) string {
	if len(written) >= 2 && strings.HasPrefix(written, "`") && strings.HasSuffix(written, "`") {
		return strings.ReplaceAll(written[1:len(written)-1], "``", "`")
	}
	return written
}
