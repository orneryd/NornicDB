package antlr

import (
	_ "embed"
	"strings"
)

// The vocabulary is the maintained generator's artifact, not a second keyword
// list. Reading names needs its literal tokens, not the lexer's executable ATN.
//
//go:embed CypherLexer.tokens
var lexerVocabulary string

// KeywordName reads native vocabulary metadata without initializing prediction
// or retaining another keyword index. It has the same cold and warm behavior.
func KeywordName(name string) bool {
	for line := range strings.SplitSeq(lexerVocabulary, "\n") {
		end := strings.LastIndexByte(line, '=')
		if end > 1 && line[0] == '\'' && line[end-1] == '\'' && strings.EqualFold(strings.Trim(line[:end], "'"), name) {
			return true
		}
	}
	return false
}
