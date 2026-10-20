package antlr

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"
)

func TestKeywordVocabularyMatchesNativeLexer(t *testing.T) {
	lexer := NewCypherLexer(antlr.NewInputStream(""))
	expected := make(map[string]bool)
	for _, literal := range lexer.GetLiteralNames() {
		name := strings.Trim(literal, "'")
		if name == "" {
			continue
		}
		expected[strings.ToUpper(name)] = true
		require.True(t, KeywordName(name), literal)
		require.True(t, KeywordName(strings.ToLower(name)), literal)
	}
	require.False(t, KeywordName("not_a_native_keyword"))
	for line := range strings.SplitSeq(lexerVocabulary, "\n") {
		end := strings.LastIndexByte(line, '=')
		if end > 1 && line[0] == '\'' && line[end-1] == '\'' {
			require.True(t, expected[strings.ToUpper(strings.Trim(line[:end], "'"))], line)
		}
	}
}

// Metadata consumers must not initialize parser/lexer prediction. A separate
// process proves the cold contract even when other tests already parsed text.
func TestKeywordVocabularyDoesNotInitializePrediction(t *testing.T) {
	if os.Getenv("NORNICDB_KEYWORD_VOCABULARY_CHILD") == "1" {
		require.Nil(t, CypherLexerLexerStaticData.atn)
		require.Nil(t, CypherParserParserStaticData.atn)
		require.True(t, KeywordName("MATCH"))
		require.True(t, KeywordName("graph"))
		require.False(t, KeywordName("ordinary_name"))
		require.Nil(t, CypherLexerLexerStaticData.atn)
		require.Nil(t, CypherParserParserStaticData.atn)
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestKeywordVocabularyDoesNotInitializePrediction$", "-test.timeout=15s")
	command.Env = append(os.Environ(), "NORNICDB_KEYWORD_VOCABULARY_CHILD=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
