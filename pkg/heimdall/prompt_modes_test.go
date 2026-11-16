package heimdall

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPromptModesRetainContextAndTheirActionContract(t *testing.T) {
	p := &PromptContext{
		ActionPrompt:           "inspect_schema",
		AdditionalInstructions: "The selected database contains Person nodes.",
		Examples:               []PromptExample{{UserSays: "Show the schema", ActionJSON: `{"action":"inspect_schema"}`}},
	}
	for _, build := range []func() string{p.BuildFinalPrompt, p.BuildFinalPromptForTools} {
		prompt := build()
		require.Equal(t, 1, strings.Count(prompt, p.AdditionalInstructions))
		require.Equal(t, 1, strings.Count(prompt, "User: \"Show the schema\""))
		require.Contains(t, prompt, "inspect_schema")
	}
	require.Contains(t, p.BuildFinalPrompt(), "ACTION MODE")
	require.Contains(t, p.BuildFinalPromptForTools(), "RESPONSE RULES")
	require.NotContains(t, p.BuildFinalPromptForTools(), "ACTION MODE")

	p.AdditionalInstructions = strings.Repeat("large context ", MaxSystemPromptTokens())
	require.Equal(t, p.buildMinimalPrompt(), p.BuildFinalPrompt())
	require.Equal(t, p.buildMinimalPrompt(), p.BuildFinalPromptForTools())

	// Private conversations must report budget errors instead of losing scope
	// through the stateless fallback, regardless of provider mode.
	p.ConversationID, p.SourceDatabase = "private-chat", "catalog"
	for _, build := range []func() string{p.BuildFinalPrompt, p.BuildFinalPromptForTools} {
		prompt := build()
		require.Contains(t, prompt, "Selected database: catalog")
		require.Contains(t, prompt, p.AdditionalInstructions)
	}
	require.Contains(t, p.BuildFinalPrompt(), "output only")
	require.Contains(t, p.BuildFinalPromptForTools(), "Use registered tools")
}
