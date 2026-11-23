package heimdall

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAutocompleteGenerationDoesNotStopInsideCypher(t *testing.T) {
	nor281InstallAutocompleteSubsystem(t)
	completion := "RETURN 'The user IMPORTANT Rules:\n\n\nAvailable Complete query:' AS value"
	generator := NewMockGenerator("/test/model.gguf")
	generator.generateFunc = func(_ context.Context, _ string, params GenerateParams) (string, error) {
		assert.Empty(t, params.StopTokens, "text stops truncate quoted Cypher before extraction can protect it")
		assert.Equal(t, 128, params.MaxTokens, "autocomplete must retain its output bound")
		return completion, nil
	}
	manager := newTestManager(generator)
	response := nor281PostAutocomplete(t, testHandler(manager, manager.config), `{"query":"RETURN"}`)
	assert.Equal(t, completion, nor281Suggestion(t, response))
	assert.Equal(t, int64(1), generator.GetGenerateCount())
}

func TestAutocompletePreservesQuotedSourceAcrossCleanupStages(t *testing.T) {
	nor281InstallAutocompleteSubsystem(t)
	for _, completion := range []string{
		"RETURN 1 AS `a\nRETURN b`",
		"RETURN 1 AS `a\nIMPORTANT b`",
		"RETURN 1 AS `a\n  b`",
		"RETURN 1 AS `a IMPORTANT b`",
		"RETURN 1 AS `a Rules: b`",
		"RETURN 1 AS `a Available b`",
		"RETURN 1 AS `a Complete b`",
		"RETURN 1 AS `a The user b`",
		"RETURN 1 AS `RULES`",
		"RETURN 'a  b' AS value",
		"RETURN 'a\nThey are here' AS value",
		"RETURN \"a IMPORTANT b\" AS value",
		"RETURN 'a\\' The user b' AS value",
		"RETURN 1 /* IMPORTANT\nRETURN 2 */ AS value",
		"RETURN  1 AS value",
	} {
		t.Run(completion, func(t *testing.T) {
			handler := nor281AutocompleteHandler(t, completion, nil)
			response := nor281PostAutocomplete(t, handler, `{"query":"RETURN"}`)
			assert.Equal(t, completion, nor281Suggestion(t, response))
		})
	}
}

func TestAutocompletePreservesOutsideQuoteExtractionControls(t *testing.T) {
	nor281InstallAutocompleteSubsystem(t)
	for _, tc := range []struct{ completion, want string }{
		{"RETURN 1 AS value The user wants help", "RETURN 1 AS value"},
		{"RETURN 1 AS value IMPORTANT follow rules", "RETURN 1 AS value"},
		{"Here is the query:\nRETURN 1 AS value", "RETURN 1 AS value"},
		{"RETURN 1 AS value\nRETURN 2 AS value", "RETURN 1 AS value"},
		{"RETURN 1 AS value\nIMPORTANT follow rules", "RETURN 1 AS value"},
		{"IMPORTANT follow rules\nRETURN 1 AS value", ""},
		{"RETURN 1 AS RULES", ""},
		{"RETURN 1 AS value\u00a0The user wants help", "RETURN 1 AS value\u00a0The user wants help"},
		{"```cypher\nRETURN 1 AS value", "RETURN 1 AS value"},
		{"```cypher\nMATCH (n:Person)\nRETURN n\nLIMIT 25\n```\nThe user is asking for help", "MATCH (n:Person)"},
	} {
		t.Run(tc.completion, func(t *testing.T) {
			handler := nor281AutocompleteHandler(t, tc.completion, nil)
			response := nor281PostAutocomplete(t, handler, `{"query":"RETURN"}`)
			assert.Equal(t, tc.want, nor281Suggestion(t, response))
		})
	}
}
