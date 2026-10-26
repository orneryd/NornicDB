package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Author: #1663 implementation lane. Shell execution consumes the shared grammar
// producer's extents; admission and execution cannot frame different values.
func shellCommandSpans(text string, closeAt func(int) (int, bool)) (cyphertext.Span, cyphertext.Span, bool) {
	return cypherantlr.ShellCommandSpans(text, closeAt)
}

type shellParameterInput struct {
	key, value cyphertext.Span
	mapForm    bool
}

func readShellParameterInput(text string) (shellParameterInput, error) {
	input, err := cypherantlr.ReadShellParameterInput(text)
	return shellParameterInput{key: input.Key, value: input.Value, mapForm: input.MapForm}, err
}
