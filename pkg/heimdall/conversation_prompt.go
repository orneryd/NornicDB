package heimdall

import "strings"

// Conversation context must never disappear through a generic prompt fallback.
// Budget validation reports an oversized prompt instead of dropping its scope.
func (p *PromptContext) buildConversationSystemPrompt(nativeTools bool) string {
	var prompt strings.Builder
	prompt.WriteString("You are Heimdall, the NornicDB assistant. Help the user inspect their selected database. Use the supplied database name and schema; do not ask for information already present. Never invent records or query results. All graph queries are read-only.\n")
	if p.SourceDatabase != "" {
		prompt.WriteString("Selected database: " + p.SourceDatabase + "\n")
	} else {
		prompt.WriteString("No database is selected.\n")
	}
	if nativeTools {
		prompt.WriteString("Use registered tools when needed, then answer in clear language.\n")
	} else {
		prompt.WriteString("When a tool is needed, output only {\"action\":\"registered_name\",\"params\":{...}} using an action below. Otherwise answer the user directly.\n")
	}
	prompt.WriteString(p.ActionPrompt)
	prompt.WriteString("\nCURRENT DATABASE CONTEXT AND INSTRUCTIONS:\n")
	prompt.WriteString(p.AdditionalInstructions)
	return prompt.String()
}
