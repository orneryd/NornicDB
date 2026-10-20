package cyphertext

import "strings"

// SymbolicNameValue decodes a symbolic name, preserving an unquoted name and
// replacing doubled backticks within a quoted name with one backtick.
func SymbolicNameValue(written string) string {
	if len(written) >= 2 && written[0] == '`' && written[len(written)-1] == '`' {
		return strings.ReplaceAll(written[1:len(written)-1], "``", "`")
	}
	return written
}
