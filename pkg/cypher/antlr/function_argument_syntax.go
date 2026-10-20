package antlr

import "strings"

// FunctionArgumentSyntax describes a named syntax operand, rather than a value
// expression. Native roles and the existing lowering share this contract.
type FunctionArgumentSyntax struct {
	Position int
	Names    []string
	Vector   bool
}

func FunctionSymbolArgument(name string) (FunctionArgumentSyntax, bool) {
	switch strings.ToLower(name) {
	case "vector":
		return FunctionArgumentSyntax{Position: 2, Vector: true}, true
	case "vector_distance":
		return FunctionArgumentSyntax{Position: 2, Names: []string{"EUCLIDEAN", "EUCLIDEAN_SQUARED", "MANHATTAN", "COSINE", "DOT", "HAMMING"}, Vector: true}, true
	case "vector_norm":
		return FunctionArgumentSyntax{Position: 1, Names: []string{"EUCLIDEAN", "MANHATTAN"}, Vector: true}, true
	case "normalize":
		return FunctionArgumentSyntax{Position: 1, Names: []string{"NFC", "NFD", "NFKC", "NFKD"}}, true
	}
	return FunctionArgumentSyntax{}, false
}
