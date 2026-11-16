package heimdall

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The prompt owner renders exactly one response contract per PromptMode.
func TestPromptModesRetainContextAndTheirActionContract(t *testing.T) {
	p := &PromptContext{
		ActionPrompt:           "inspect_schema",
		AdditionalInstructions: "The selected database contains Person nodes.",
		Examples:               []PromptExample{{UserSays: "Show the schema", ActionJSON: `{"action":"inspect_schema"}`}},
	}

	jsonPrompt := p.BuildFinalPrompt(PromptModeJSONAction)
	nativePrompt := p.BuildFinalPrompt(PromptModeNativeTools)

	// Both modes keep the immutable action list, the mutable context and the examples once.
	for _, prompt := range []string{jsonPrompt, nativePrompt} {
		require.True(t, strings.HasPrefix(prompt, "You are Heimdall"))
		require.Equal(t, 1, strings.Count(prompt, p.AdditionalInstructions))
		require.Equal(t, 1, strings.Count(prompt, `User: "Show the schema"`))
		require.Contains(t, prompt, "inspect_schema")
	}

	// Each mode renders its own instructions and never mixes in the other contract.
	require.Equal(t, 1, strings.Count(jsonPrompt, "ACTION MODE"))
	require.NotContains(t, jsonPrompt, "RESPONSE RULES")
	require.Equal(t, 1, strings.Count(nativePrompt, "RESPONSE RULES"))
	require.NotContains(t, nativePrompt, "ACTION MODE")

	var zero PromptMode
	require.Equal(t, jsonPrompt, p.BuildFinalPrompt(zero))
}

// Both modes share the same stateless token-budget fallback.
func TestBuildFinalPromptBudgetFallbackIsSharedByBothModes(t *testing.T) {
	p := &PromptContext{
		ActionPrompt:           "inspect_schema",
		AdditionalInstructions: strings.Repeat("large context ", MaxSystemPromptTokens()),
	}
	minimal := p.buildMinimalPrompt()
	require.Equal(t, minimal, p.BuildFinalPrompt(PromptModeJSONAction))
	require.Equal(t, minimal, p.BuildFinalPrompt(PromptModeNativeTools))
}

// Conversations keep their database-scoped prompt and only swap the action instruction.
func TestBuildFinalPromptKeepsConversationRulesPerMode(t *testing.T) {
	p := &PromptContext{
		ActionPrompt:           "inspect_schema",
		AdditionalInstructions: "The selected database contains Person nodes.",
		ConversationID:         "private-chat",
		SourceDatabase:         "catalog",
	}

	jsonPrompt := p.BuildFinalPrompt(PromptModeJSONAction)
	nativePrompt := p.BuildFinalPrompt(PromptModeNativeTools)

	for _, prompt := range []string{jsonPrompt, nativePrompt} {
		require.Contains(t, prompt, "Selected database: catalog")
		require.Contains(t, prompt, "CURRENT DATABASE CONTEXT AND INSTRUCTIONS:")
		require.Contains(t, prompt, p.AdditionalInstructions)
		require.Contains(t, prompt, "inspect_schema")
		// The stateless minimal fallback never replaces a conversation prompt.
		require.NotEqual(t, p.buildMinimalPrompt(), prompt)
	}
	require.Contains(t, jsonPrompt, "output only")
	require.NotContains(t, jsonPrompt, "Use registered tools when needed")
	require.Contains(t, nativePrompt, "Use registered tools when needed")
	require.NotContains(t, nativePrompt, "output only")
}

// promptModeCapture records the system prompt each generation path received.
type promptModeCapture struct {
	mu      sync.Mutex
	prompts map[PromptMode][]string
}

func (c *promptModeCapture) record(mode PromptMode, prompt string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prompts == nil {
		c.prompts = make(map[PromptMode][]string)
	}
	c.prompts[mode] = append(c.prompts[mode], prompt)
}

func (c *promptModeCapture) captured(mode PromptMode) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.prompts[mode])
}

// promptOnlyProvider is a provider without native tool calling.
func promptOnlyProvider(capture *promptModeCapture) *MockGenerator {
	generator := NewMockGenerator("/test/prompt-mode.gguf")
	generator.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		capture.record(PromptModeJSONAction, prompt)
		return "prompt contract answer", nil
	}
	generator.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
		capture.record(PromptModeJSONAction, prompt)
		return callback("prompt contract answer")
	}
	return generator
}

// promptModeNativeProvider adds native tool calling to the prompt-only provider.
type promptModeNativeProvider struct {
	*MockGenerator
	capture *promptModeCapture
}

func nativeToolProvider(capture *promptModeCapture) *promptModeNativeProvider {
	return &promptModeNativeProvider{MockGenerator: promptOnlyProvider(capture), capture: capture}
}

func (p *promptModeNativeProvider) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (string, []ParsedToolCall, error) {
	if len(messages) > 0 {
		p.capture.record(PromptModeNativeTools, messages[0].Content)
	}
	return "native contract answer", nil, nil
}

// newPromptModeSubsystem installs a clean plugin/action subsystem plus one action so that
// native tool calling has a tool to dispatch.
func newPromptModeSubsystem(t *testing.T) *SubsystemManager {
	t.Helper()
	prior := globalManager
	globalManager = &SubsystemManager{
		plugins:    make(map[string]*LoadedHeimdallPlugin),
		actions:    make(map[string]ActionFunc),
		orderDirty: true,
	}
	globalManager.SetContext(SubsystemHost{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}})
	t.Cleanup(func() {
		_ = globalManager.StopAll()
		globalManager = prior
	})
	RegisterBuiltinAction(ActionFunc{
		Name:        "prompt_mode_probe",
		Description: "Prompt mode probe action",
		Category:    "test",
		Handler: func(ActionContext) (*ActionResult, error) {
			return &ActionResult{Success: true, Message: "probe ran"}, nil
		},
	})
	return globalManager
}

// A stateless request sends the prompt contract selected for its provider.
func TestStatelessRequestUsesTheSelectedPromptContract(t *testing.T) {
	newPromptModeSubsystem(t)

	for _, tc := range []struct {
		name   string
		native bool
	}{
		{name: "json action provider", native: false},
		{name: "native tool provider", native: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &promptModeCapture{}
			var generator Generator = promptOnlyProvider(capture)
			if tc.native {
				generator = nativeToolProvider(capture)
			}
			manager := newTestManager(generator)
			handler := testHandler(manager, manager.config)

			body := `{"messages":[{"role":"user","content":"explain the schema"}]}`
			recorder := httptest.NewRecorder()
			handler.handleChatCompletions(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", strings.NewReader(body)))
			require.Equal(t, http.StatusOK, recorder.Code)

			if tc.native {
				systems := capture.captured(PromptModeNativeTools)
				require.Len(t, systems, 1)
				require.Empty(t, capture.captured(PromptModeJSONAction))
				require.Equal(t, 1, strings.Count(systems[0], "RESPONSE RULES"))
				require.NotContains(t, systems[0], "ACTION MODE")
				return
			}
			require.Empty(t, capture.captured(PromptModeNativeTools))
			prompts := capture.captured(PromptModeJSONAction)
			require.Len(t, prompts, 1)
			require.Equal(t, 1, strings.Count(prompts[0], "ACTION MODE"))
			require.NotContains(t, prompts[0], "RESPONSE RULES")
		})
	}
}

// A conversation sends the prompt contract selected for its provider.
func TestConversationRequestUsesTheSelectedPromptContract(t *testing.T) {
	manager := newPromptModeSubsystem(t)
	require.NoError(t, manager.RegisterPlugin(&turnTestPlugin{MockHeimdallPlugin: NewMockPlugin("prompt-mode-turn")}, "", true))

	for _, tc := range []struct {
		name   string
		native bool
	}{
		{name: "json action provider", native: false},
		{name: "native tool provider", native: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &promptModeCapture{}
			var generator Generator = promptOnlyProvider(capture)
			if tc.native {
				generator = nativeToolProvider(capture)
			}
			handlerManager := newTestManager(generator)
			handler := testHandler(handlerManager, handlerManager.config)

			request := ChatRequest{ConversationID: "thread", MessageID: "message", Messages: []ChatMessage{{Role: "user", Content: "Explain this database"}}}
			body, err := json.Marshal(request)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			handler.handleChatCompletions(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", strings.NewReader(string(body))))
			require.Equal(t, http.StatusOK, recorder.Code)

			if tc.native {
				systems := capture.captured(PromptModeNativeTools)
				require.Len(t, systems, 1)
				require.Empty(t, capture.captured(PromptModeJSONAction))
				require.Equal(t, 1, strings.Count(systems[0], "Use registered tools when needed"))
				require.Contains(t, systems[0], "CURRENT DATABASE CONTEXT AND INSTRUCTIONS:")
				require.NotContains(t, systems[0], "output only")
				return
			}
			require.Empty(t, capture.captured(PromptModeNativeTools))
			prompts := capture.captured(PromptModeJSONAction)
			require.Len(t, prompts, 1)
			require.Equal(t, 1, strings.Count(prompts[0], "output only"))
			require.Contains(t, prompts[0], "CURRENT DATABASE CONTEXT AND INSTRUCTIONS:")
			require.NotContains(t, prompts[0], "Use registered tools when needed")
		})
	}
}
