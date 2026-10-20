package fabric

import "github.com/orneryd/nornicdb/pkg/localization"

func UnionColumnMismatchMessage(expected, actual []string) localization.Message {
	if len(expected) != len(actual) {
		return localization.CypherResidualUnionColumnCountMismatch(len(expected), len(actual))
	}
	return localization.CypherResidualUnionColumnNamesMismatch(expected, actual)
}

// UnionColumnsError preserves the Cypher syntax status across Fabric adapters.
type UnionColumnsError struct{ Message localization.Message }

func (e *UnionColumnsError) Error() string         { return e.Message.Fallback }
func (e *UnionColumnsError) BoltErrorCode() string { return "Neo.ClientError.Statement.SyntaxError" }
