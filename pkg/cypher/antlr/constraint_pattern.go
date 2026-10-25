package antlr

import "github.com/orneryd/nornicdb/internal/cyphertext"

// ConstraintPattern retains the shared owner's opaque matching plan at the
// parser metadata boundary. This adapter introduces no syntax reader.
type ConstraintPattern = cyphertext.ConstraintPattern

func ParseConstraintPattern(text string) (ConstraintPattern, bool) {
	return cyphertext.CompileConstraintPattern(text)
}
