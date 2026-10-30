package heimdall

import (
	"context"
	"encoding/json"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// chatMessage is the common message wire contract supported by both chat APIs.
// Request/response envelopes and streaming protocols remain provider-specific.
type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type chatToolCall struct {
	Id       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type chatTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

// chatCompletionGenerator owns non-streaming generation and tool-round mapping.
// Embedding promotes the actual implementations to each provider; there are no
// provider forwarding methods. Bound completion functions retain live client and
// credential state, including transport injection after construction.
type chatCompletionGenerator struct {
	complete         func(context.Context, []chatMessage, []chatTool, GenerateParams) (chatMessage, error)
	credentials      func() (configuredURL, apiKey string)
	toolContextLimit int
	toolContent      func(string) string
}

func (g *chatCompletionGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (_ string, err error) {
	defer func() {
		configuredURL, apiKey := g.credentials()
		err = observability.RedactClientError(err, configuredURL, apiKey)
	}()
	message, err := g.complete(ctx, []chatMessage{{Role: "user", Content: prompt}}, nil, params)
	if err != nil {
		return "", err
	}
	return message.Content, nil
}

func (g *chatCompletionGenerator) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (content string, toolCalls []ParsedToolCall, err error) {
	defer func() {
		configuredURL, apiKey := g.credentials()
		err = observability.RedactClientError(err, configuredURL, apiKey)
	}()
	if g.toolContextLimit > 0 {
		messages = trimMessagesForContext(messages, g.toolContextLimit)
	}
	wire := make([]chatMessage, 0, len(messages))
	for _, m := range messages {
		msg := chatMessage{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
		if g.toolContent != nil {
			msg.Content = g.toolContent(msg.Content)
		}
		if len(m.ToolCalls) > 0 {
			msg.ToolCalls = make([]chatToolCall, len(m.ToolCalls))
			for i, tc := range m.ToolCalls {
				msg.ToolCalls[i].Id = tc.Id
				msg.ToolCalls[i].Type = "function"
				msg.ToolCalls[i].Function.Name = tc.Name
				msg.ToolCalls[i].Function.Arguments = tc.Arguments
			}
		}
		wire = append(wire, msg)
	}
	wireTools := make([]chatTool, len(tools))
	for i, tool := range tools {
		wireTools[i].Type = "function"
		wireTools[i].Function.Name = tool.Name
		wireTools[i].Function.Description = tool.Description
		wireTools[i].Function.Parameters = tool.InputSchema
	}
	message, err := g.complete(ctx, wire, wireTools, params)
	if err != nil {
		return "", nil, err
	}
	for _, call := range message.ToolCalls {
		toolCalls = append(toolCalls, ParsedToolCall{
			Id: call.Id, Name: call.Function.Name, Arguments: call.Function.Arguments,
		})
	}
	return message.Content, toolCalls, nil
}
