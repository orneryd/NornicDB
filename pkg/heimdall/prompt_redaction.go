package heimdall

import (
	"encoding/json"

	"github.com/orneryd/nornicdb/pkg/observability"
)

func redactGeneratorPrompt(prompt string) (string, []string) {
	credentials := observability.ContextCredentials(prompt)
	return observability.RedactText(prompt, credentials...), credentials
}

func redactToolRoundInput(messages []ToolRoundMessage, tools []MCPTool) ([]ToolRoundMessage, []MCPTool, []string) {
	credentials := make([]string, 0)
	seen := make(map[string]struct{})
	collect := func(value string) {
		for _, credential := range observability.ContextCredentials(value) {
			if credential == "" {
				continue
			}
			if _, exists := seen[credential]; exists {
				continue
			}
			seen[credential] = struct{}{}
			credentials = append(credentials, credential)
		}
	}
	for _, message := range messages {
		collect(message.Content)
		collect(message.ToolCallID)
		for _, call := range message.ToolCalls {
			collect(call.Id)
			collect(call.Name)
			collect(call.Arguments)
		}
	}
	for _, tool := range tools {
		collect(tool.Name)
		collect(tool.Description)
		collect(string(tool.InputSchema))
	}

	safeMessages := append([]ToolRoundMessage(nil), messages...)
	for i := range safeMessages {
		safeMessages[i].Content = observability.RedactText(safeMessages[i].Content, credentials...)
		safeMessages[i].ToolCallID = observability.RedactText(safeMessages[i].ToolCallID, credentials...)
		if len(messages[i].ToolCalls) > 0 {
			safeMessages[i].ToolCalls = append([]ParsedToolCall(nil), messages[i].ToolCalls...)
			for j := range safeMessages[i].ToolCalls {
				safeMessages[i].ToolCalls[j].Id = observability.RedactText(safeMessages[i].ToolCalls[j].Id, credentials...)
				safeMessages[i].ToolCalls[j].Name = observability.RedactText(safeMessages[i].ToolCalls[j].Name, credentials...)
				safeMessages[i].ToolCalls[j].Arguments = observability.RedactText(safeMessages[i].ToolCalls[j].Arguments, credentials...)
			}
		}
	}
	safeTools := append([]MCPTool(nil), tools...)
	for i := range safeTools {
		safeTools[i].Name = observability.RedactText(safeTools[i].Name, credentials...)
		safeTools[i].Description = observability.RedactText(safeTools[i].Description, credentials...)
		if len(safeTools[i].InputSchema) > 0 {
			safeTools[i].InputSchema = json.RawMessage(observability.RedactText(string(safeTools[i].InputSchema), credentials...))
		}
	}
	return safeMessages, safeTools, credentials
}
