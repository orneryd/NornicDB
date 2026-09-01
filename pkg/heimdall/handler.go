package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
)

// Handler provides HTTP endpoints for Bifrost chat.
// Uses standard HTTP/SSE - no external dependencies required.
// Bifrost is the rainbow bridge that connects to Heimdall.
//
// Endpoints:
//   - GET  /api/bifrost/status           - Heimdall and Bifrost status
//   - POST /api/bifrost/chat/completions - Chat with Heimdall
//   - GET  /v1/models                    - OpenAI-compatible single-model list
//   - POST /v1/chat/completions          - OpenAI-compatible alias for Bifrost chat
//   - GET  /api/bifrost/events           - SSE stream for real-time events
type Handler struct {
	manager        *Manager
	bifrost        *Bifrost
	config         Config
	database       DatabaseRouter
	metrics        MetricsReader
	inMemoryRunner InMemoryToolRunner // optional: e.g. MCP store/recall/discover for agentic loop
	localizer      *localization.Manager
}

var leakedChatTemplateMarker = regexp.MustCompile(`(?s)<\|im_(?:start|end)\|>`)
var actionEnvelopePrefix = regexp.MustCompile(`(?s)\{\s*"action"\s*:\s*"`)

func sanitizeAssistantResponse(content string) string {
	content = strings.TrimSpace(content)
	if content == "" {
		return ""
	}
	if idx := leakedChatTemplateMarker.FindStringIndex(content); idx != nil {
		content = strings.TrimSpace(content[:idx[0]])
	}
	if idx := strings.Index(strings.ToLower(content), "<|start_of_turn|>"); idx >= 0 {
		content = strings.TrimSpace(content[:idx])
	}
	content = strings.TrimRight(content, "\r\n")
	return content + "\n"
}

func parseActionEnvelope(response string) *ParsedAction {
	response = strings.TrimSpace(response)
	start := strings.Index(response, "{")
	if start == -1 {
		return nil
	}
	jsonStr := extractFirstJSONObject(response[start:])
	if jsonStr == "" {
		return nil
	}
	var parsed ParsedAction
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return nil
	}
	if strings.TrimSpace(parsed.Action) == "" {
		return nil
	}
	if parsed.Params == nil {
		parsed.Params = make(map[string]interface{})
	}
	return &parsed
}

// extractFirstJSONObject returns the first balanced JSON object from the input,
// or an empty string if the input does not contain a complete object.
func extractFirstJSONObject(s string) string {
	if len(s) == 0 || s[0] != '{' {
		return ""
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[:i+1]
			}
		}
	}
	return ""
}

func looksLikeActionEnvelopePrefix(response string) bool {
	response = strings.TrimSpace(response)
	if response == "" {
		return false
	}
	if parseActionEnvelope(response) != nil {
		return true
	}
	start := strings.Index(response, "{")
	if start == -1 {
		return false
	}
	return actionEnvelopePrefix.MatchString(response[start:])
}

func buildPassThroughToolCall(action *ParsedAction) ChatToolCallWire {
	arguments := "{}"
	if action != nil && action.Params != nil {
		if data, err := json.Marshal(action.Params); err == nil {
			arguments = string(data)
		}
	}
	name := ""
	if action != nil {
		name = action.Action
	}
	return ChatToolCallWire{
		ID:   "call_" + generateID(),
		Type: "function",
		Function: ChatToolFunctionWire{
			Name:      name,
			Arguments: arguments,
		},
	}
}

func chatRequestToolsToMCPTools(defs []ChatToolDefinition) []MCPTool {
	if len(defs) == 0 {
		return nil
	}
	tools := make([]MCPTool, 0, len(defs))
	for _, def := range defs {
		if strings.TrimSpace(def.Type) != "" && def.Type != "function" {
			continue
		}
		name := strings.TrimSpace(def.Function.Name)
		if name == "" {
			continue
		}
		schema := def.Function.Parameters
		if len(schema) == 0 {
			schema = DefaultActionInputSchema
		}
		tools = append(tools, MCPTool{
			Name:        name,
			Description: strings.TrimSpace(def.Function.Description),
			InputSchema: schema,
		})
	}
	return tools
}

func mergeMCPTools(groups ...[]MCPTool) []MCPTool {
	merged := make([]MCPTool, 0)
	seen := make(map[string]struct{})
	for _, group := range groups {
		for _, tool := range group {
			name := strings.TrimSpace(tool.Name)
			if name == "" {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			merged = append(merged, tool)
		}
	}
	return merged
}

func (h *Handler) announcedModel() string {
	if strings.TrimSpace(h.config.Model) != "" {
		return strings.TrimSpace(h.config.Model)
	}
	return "nornicdb-heimdall"
}

// NewHandler creates a Bifrost HTTP handler.
// Returns nil if Heimdall is disabled (manager is nil).
// Automatically creates Bifrost bridge when Heimdall is enabled.
func NewHandler(manager *Manager, cfg Config, db DatabaseRouter, metrics MetricsReader) *Handler {
	if manager == nil {
		return nil
	}
	// Bifrost is automatically enabled when Heimdall is enabled
	bifrost := NewBifrost(cfg)
	return &Handler{
		manager:  manager,
		bifrost:  bifrost,
		config:   cfg,
		database: db,
		metrics:  metrics,
	}
}

// Bifrost returns the BifrostBridge for plugin communication.
// Returns NoOpBifrost if Bifrost is not available.
func (h *Handler) Bifrost() BifrostBridge {
	if h.bifrost == nil {
		return &NoOpBifrost{}
	}
	return h.bifrost
}

// SetInMemoryToolRunner sets the runner for MCP-style tools (store, recall, discover, etc.)
// so the agentic loop can call them in process. When set, tools from the runner are merged
// into the tool list and execution is dispatched in memory instead of via HTTP.
func (h *Handler) SetInMemoryToolRunner(runner InMemoryToolRunner) {
	h.inMemoryRunner = runner
}

// SetLocalizer sets the immutable message catalog used for HTTP responses.
func (h *Handler) SetLocalizer(manager *localization.Manager) {
	h.localizer = manager
}

func (h *Handler) writeLocalizedError(w http.ResponseWriter, r *http.Request, message localization.Message, status int) {
	h.writeLocalizedContextError(w, h.localizationContext(r), message, status)
}

func (h *Handler) localizationContext(r *http.Request) context.Context {
	ctx := r.Context()
	if h.localizer == nil || len(localization.PreferencesFromContext(ctx)) > 0 {
		return ctx
	}
	preferences, _, err := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))
	if err == nil && len(preferences) > 0 {
		match := h.localizer.Resolve("http", preferences...)
		return localization.WithPreferences(ctx, match.Tag)
	}
	return ctx
}

func (h *Handler) writeLocalizedContextError(w http.ResponseWriter, ctx context.Context, message localization.Message, status int) {
	text := message.Fallback
	tag := language.AmericanEnglish
	if h.localizer != nil {
		if rendered, resolved, err := h.localizer.Render(ctx, message); err == nil {
			text = rendered
			tag = resolved
		}
	}
	w.Header().Set("Content-Language", tag.String())
	w.Header().Add("Vary", "Accept-Language")
	http.Error(w, text, status)
}

// ServeHTTP routes requests to appropriate handlers.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = r.WithContext(h.localizationContext(r))
	switch {
	case r.URL.Path == "/api/bifrost/status":
		h.handleStatus(w, r)
	case r.URL.Path == "/v1/models":
		h.handleModels(w, r)
	case r.URL.Path == "/api/bifrost/chat/completions":
		h.handleChatCompletions(w, r)
	case r.URL.Path == "/v1/chat/completions":
		h.handleChatCompletions(w, r)
	case r.URL.Path == "/api/bifrost/events":
		h.handleEvents(w, r)
	case r.URL.Path == "/api/bifrost/autocomplete":
		h.handleAutocomplete(w, r)
	default:
		http.NotFound(w, r)
	}
}

// handleStatus returns Heimdall status and stats.
// GET /api/bifrost/status
func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeLocalizedError(w, r, localization.HeimdallMethodNotAllowed(), http.StatusMethodNotAllowed)
		return
	}

	stats := h.manager.Stats()

	// Include Bifrost stats if available
	var bifrostStats map[string]interface{}
	if h.bifrost != nil {
		bifrostStats = h.bifrost.Stats()
	} else {
		bifrostStats = map[string]interface{}{
			"enabled":          false,
			"connection_count": 0,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"model":  h.announcedModel(),
		"heimdall": map[string]interface{}{
			"enabled": h.config.Enabled,
			"stats":   stats,
		},
		"bifrost": bifrostStats,
	})
}

func (h *Handler) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeLocalizedError(w, r, localization.HeimdallMethodNotAllowed(), http.StatusMethodNotAllowed)
		return
	}

	model := h.announcedModel()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"object": "list",
		"data": []map[string]interface{}{
			{
				"id":       model,
				"object":   "model",
				"created":  time.Now().Unix(),
				"owned_by": "nornicdb",
			},
		},
	})
}

// handleEvents provides an SSE stream for real-time Bifrost events.
// GET /api/bifrost/events
//
// This endpoint allows clients to receive real-time notifications, messages,
// and system events from Heimdall and its plugins.
func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeLocalizedError(w, r, localization.HeimdallMethodNotAllowed(), http.StatusMethodNotAllowed)
		return
	}

	// Verify Bifrost is enabled
	if h.bifrost == nil {
		h.writeLocalizedError(w, r, localization.HeimdallBifrostNotEnabled(), http.StatusServiceUnavailable)
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		h.writeLocalizedError(w, r, localization.HeimdallStreamingNotSupported(), http.StatusInternalServerError)
		return
	}

	// Generate client ID
	clientID := generateID()

	// Register this connection with Bifrost
	h.bifrost.RegisterClient(clientID, w, flusher)
	defer h.bifrost.UnregisterClient(clientID)

	// Send initial connection message
	connMsg := BifrostMessage{
		Type:      "connected",
		Timestamp: time.Now().Unix(),
		Content:   "Connected to Bifrost",
		Data: map[string]interface{}{
			"client_id": clientID,
		},
	}
	data, _ := json.Marshal(connMsg)
	fmt.Fprintf(w, "data: %s\n\n", string(data))
	flusher.Flush()

	// Keep connection alive until client disconnects
	<-r.Context().Done()
}

// handleAutocomplete provides Cypher query autocomplete suggestions.
// POST /api/bifrost/autocomplete
//
// Request body:
//
//	{
//	  "query": "MATCH (n",
//	  "cursor_position": 10  // optional
//	}
//
// Response:
//
//	{
//	  "suggestion": "MATCH (n) RETURN n LIMIT 25",
//	  "schema": {
//	    "labels": ["Person", "File", ...],
//	    "properties": ["name", "age", ...],
//	    "relTypes": ["KNOWS", "OWNS", ...]
//	  }
//	}
func (h *Handler) handleAutocomplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeLocalizedError(w, r, localization.HeimdallMethodNotAllowed(), http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Query          string `json:"query"`
		CursorPosition int    `json:"cursor_position,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeLocalizedError(w, r, localization.HeimdallInvalidRequestBody(), http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		h.writeLocalizedError(w, r, localization.HeimdallQueryParameterRequired(), http.StatusBadRequest)
		return
	}

	// Invoke the autocomplete action
	ctx := ActionContext{
		Context:            r.Context(),
		Params:             map[string]interface{}{"query": req.Query, "cursor_position": req.CursorPosition},
		Database:           h.database,
		Metrics:            h.metrics,
		Bifrost:            h.bifrost,
		PrincipalRoles:     PrincipalRolesFromContext(r.Context()),
		DatabaseAccessMode: DatabaseAccessModeFromContext(r.Context()),
		ResolvedAccess:     ResolvedAccessResolverFromContext(r.Context()),
	}

	result, err := ExecuteAction("heimdall_autocomplete_suggest", ctx)
	if err != nil {
		h.writeLocalizedError(w, r, localization.HeimdallAutocompleteFailed(err), http.StatusInternalServerError)
		return
	}

	// If we have schema info but no suggestion, use SLM to generate one
	if result != nil && result.Success {
		schema, _ := result.Data["schema"].(map[string]interface{})
		suggestion, _ := result.Data["suggestion"].(string)

		// If no suggestion from action, use SLM to generate one
		if suggestion == "" && h.manager != nil {
			// Build prompt with schema context
			labels, _ := schema["labels"].([]interface{})
			properties, _ := schema["properties"].([]interface{})
			relTypes, _ := schema["relTypes"].([]interface{})

			labelStrs := make([]string, 0, len(labels))
			for _, l := range labels {
				if s, ok := l.(string); ok {
					labelStrs = append(labelStrs, s)
				}
			}
			propStrs := make([]string, 0, len(properties))
			for _, p := range properties {
				if s, ok := p.(string); ok {
					propStrs = append(propStrs, s)
				}
			}
			relStrs := make([]string, 0, len(relTypes))
			for _, r := range relTypes {
				if s, ok := r.(string); ok {
					relStrs = append(relStrs, s)
				}
			}

			// Build a clearer prompt that explains the difference between properties and relationship types
			var schemaContext strings.Builder
			if len(labelStrs) > 0 {
				schemaContext.WriteString(fmt.Sprintf("Node labels: %s\n", strings.Join(labelStrs, ", ")))
			}
			if len(propStrs) > 0 {
				// Limit properties to avoid overwhelming the prompt
				propsToShow := propStrs
				if len(propsToShow) > 20 {
					propsToShow = propsToShow[:20]
				}
				schemaContext.WriteString(fmt.Sprintf("Node properties (use as n.propertyName): %s\n", strings.Join(propsToShow, ", ")))
			}
			if len(relStrs) > 0 {
				// Limit relationship types
				relsToShow := relStrs
				if len(relsToShow) > 10 {
					relsToShow = relsToShow[:10]
				}
				schemaContext.WriteString(fmt.Sprintf("Relationship types (use in patterns like (a)-[:TYPE]->(b)): %s\n", strings.Join(relsToShow, ", ")))
			}

			prompt := fmt.Sprintf(`Complete this Cypher query. Output ONLY the single completed Cypher line. No explanations, no reasoning, no commentary about the user.

%s
Rules:
- Output ONLY the Cypher query line, nothing else
- No "the user", "they are", "I need to", or any meta-commentary
- Properties: n.propertyName. Relationships: (a)-[:TYPE]->(b)
- If missing LIMIT statement at the end, add LIMIT 25 at the end

Current query:
%s

Complete Cypher query (one line only):`,
				schemaContext.String(),
				req.Query)

			// Use raw prompt for direct completion with stricter parameters to prevent repetition
			slmResult, err := h.manager.Generate(r.Context(), prompt, GenerateParams{
				MaxTokens:   128, // Reduced to prevent repetition
				Temperature: 0.2, // Lower temperature for more focused output
				TopP:        0.8,
				TopK:        20,
				StopTokens:  []string{"The user", "They are", "I need to", "IMPORTANT", "Rules:", "Available", "Complete query:", "\n\n\n"},
			})
			if err == nil && slmResult != "" {
				// Clean up the suggestion - extract only the first valid Cypher query
				cleanSuggestion := strings.TrimSpace(slmResult)

				// Remove markdown code blocks
				cleanSuggestion = strings.TrimPrefix(cleanSuggestion, "```cypher")
				cleanSuggestion = strings.TrimPrefix(cleanSuggestion, "```")
				cleanSuggestion = strings.TrimSuffix(cleanSuggestion, "```")
				cleanSuggestion = strings.TrimSpace(cleanSuggestion)

				// Split by newlines and extract the first complete query
				lines := strings.Split(cleanSuggestion, "\n")
				cypherKeywordRegex := regexp.MustCompile(`^(MATCH|CREATE|MERGE|DELETE|SET|RETURN|WITH|UNWIND|CALL|LOAD|START|UNION|OPTIONAL)`)
				instructionPattern := regexp.MustCompile(`(?i)^(IMPORTANT|Rules?:|Available|Complete)`)

				var queryParts []string
				foundQuery := false

				for _, line := range lines {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}

					// Stop immediately if we hit instruction patterns
					if instructionPattern.MatchString(line) {
						break
					}

					// If we found a query line, start collecting
					if cypherKeywordRegex.MatchString(line) {
						// If we already found a query and hit another one, stop (prevent repetition)
						if foundQuery {
							break
						}
						foundQuery = true
						queryParts = []string{line}
					} else if foundQuery {
						// Continue collecting query parts until we hit an instruction or another query
						if instructionPattern.MatchString(line) || cypherKeywordRegex.MatchString(line) {
							break
						}
						queryParts = append(queryParts, line)
					}
				}

				if len(queryParts) > 0 {
					cleanSuggestion = strings.Join(queryParts, " ")
					// Remove any remaining instruction text using regex
					cleanSuggestion = regexp.MustCompile(`(?i)\s+(IMPORTANT|Rules?:|Available|Complete).*$`).ReplaceAllString(cleanSuggestion, "")
					// Strip trailing model prose (e.g. "The user is a student..." or "They are asking for help...")
					proseStart := regexp.MustCompile(`(?i)\s+(The user|They are|I need to|I don't know|So they|Probably not|correct syntax|asking for help).*$`)
					cleanSuggestion = proseStart.ReplaceAllString(cleanSuggestion, "")
					cleanSuggestion = strings.TrimSpace(cleanSuggestion)

					// Remove any repetition patterns (query repeated multiple times)
					// Split by common delimiters and take the first unique query
					parts := regexp.MustCompile(`\s{2,}|\n{2,}`).Split(cleanSuggestion, -1)
					if len(parts) > 0 {
						cleanSuggestion = parts[0]
					}

					// Only use if it's a valid improvement and doesn't contain instruction text
					if cleanSuggestion != "" &&
						len(cleanSuggestion) > len(req.Query) &&
						cleanSuggestion != req.Query &&
						!strings.Contains(strings.ToUpper(cleanSuggestion), "IMPORTANT") &&
						!strings.Contains(strings.ToUpper(cleanSuggestion), "RULES") {
						suggestion = cleanSuggestion
					}
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"suggestion": suggestion,
			"schema":     schema,
		})
		return
	}

	// Fallback response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"suggestion": "",
		"schema":     map[string]interface{}{},
	})
}

// sendCancellationResponse sends a cancellation response to the client.
// This is called when a lifecycle hook cancels the request.
func (h *Handler) sendCancellationResponse(w http.ResponseWriter, requestID, phase, cancelledBy, reason string) {
	// Log the cancellation
	log.Printf("[Bifrost] Request %s cancelled in %s by %s: %s", requestID, phase, cancelledBy, reason)

	// Send notification via Bifrost if available
	if h.bifrost != nil {
		h.bifrost.SendNotification("warning", "Request Cancelled",
			fmt.Sprintf("Request cancelled by %s: %s", cancelledBy, reason))
	}

	// Build cancellation response (OpenAI-compatible format)
	resp := ChatResponse{
		ID:      requestID,
		Object:  "chat.completion",
		Model:   h.config.Model,
		Created: time.Now().Unix(),
		Choices: []ChatChoice{
			{
				Index: 0,
				Message: &ChatMessage{
					Role: "assistant",
					Content: fmt.Sprintf("⚠️ Request cancelled by plugin\n\n"+
						"**Phase:** %s\n"+
						"**Cancelled by:** %s\n"+
						"**Reason:** %s",
						phase, cancelledBy, reason),
				},
				FinishReason: "stop",
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleChatCompletions handles OpenAI-compatible chat completion requests via Bifrost.
// POST /api/bifrost/chat/completions
//
// Non-streaming returns JSON response.
// Streaming uses Server-Sent Events (SSE) - standard HTTP, no WebSocket needed.
//
// Request Lifecycle:
//  1. PrePrompt hook - plugins can modify prompt context
//  2. Build prompt with immutable ActionPrompt first
//  3. Send to Heimdall SLM
//  4. PreExecute hook - plugins can validate/modify before action runs
//  5. Execute action
//  6. PostExecute hook - plugins can log/update state
func (h *Handler) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeLocalizedError(w, r, localization.HeimdallMethodNotAllowed(), http.StatusMethodNotAllowed)
		return
	}

	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeLocalizedError(w, r, localization.HeimdallInvalidRequestBody(), http.StatusBadRequest)
		return
	}

	// Ignore requested model and normalize to the single announced model.
	req.Model = h.announcedModel()

	// Extract user message for lifecycle context
	userMessage := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			userMessage = req.Messages[i].Content
			break
		}
	}

	// Create PromptContext with immutable ActionPrompt
	requestID := generateID()
	promptCtx := &PromptContext{
		Context:       r.Context(),
		RequestID:     requestID,
		RequestTime:   time.Now(),
		ActionPrompt:  ActionPrompt(), // IMMUTABLE - always first
		UserMessage:   userMessage,
		Messages:      req.Messages,
		ExternalTools: chatRequestToolsToMCPTools(req.Tools),
		Examples:      nil, // Plugins add examples via PrePrompt hooks
		PluginData:    make(map[string]interface{}),
	}
	// Set Bifrost for notifications (fire-and-forget SSE messages)
	promptCtx.SetBifrost(h.bifrost)

	// === Phase 1: PrePrompt hooks (optional) ===
	// Plugins that implement PrePromptHook can modify the prompt context
	// Plugins can call promptCtx.Cancel() to abort the request
	CallPrePromptHooks(promptCtx)
	if promptCtx.Cancelled() {
		log.Printf("[Bifrost] Request cancelled by %s: %s", promptCtx.CancelledBy(), promptCtx.CancelReason())
		h.sendCancellationResponse(w, promptCtx.RequestID, "PrePrompt", promptCtx.CancelledBy(), promptCtx.CancelReason())
		return
	}

	// === Phase 2: Build final prompt ===
	// ActionPrompt is always at the start (immutable)
	systemContent := promptCtx.BuildFinalPrompt()
	systemMsg := ChatMessage{Role: "system", Content: systemContent}

	// Validate token budget before proceeding
	if err := promptCtx.ValidateTokenBudget(); err != nil {
		budgetInfo := promptCtx.GetBudgetInfo()
		log.Printf("[Bifrost] Token budget exceeded: %v (system: %d, user: %d, total: %d)",
			err, budgetInfo.SystemTokens, budgetInfo.UserTokens, budgetInfo.TotalTokens)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build messages: system + user message
	messages := []ChatMessage{systemMsg}
	for _, msg := range promptCtx.Messages {
		if msg.Role != "system" { // Skip original system messages
			messages = append(messages, msg)
		}
	}

	prompt := BuildPrompt(messages)

	// Generation params
	params := GenerateParams{
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		TopK:        20, // Qwen3 0.6B instruct best practice to reduce repetition
		StopTokens:  []string{"<|im_end|>", "<|endoftext|>", "</s>"},
	}
	if params.MaxTokens == 0 {
		params.MaxTokens = h.config.MaxTokens
	}
	if params.Temperature == 0 {
		params.Temperature = h.config.Temperature
	}

	// Use a longer timeout so agentic loops (many tool calls) can complete without "network error".
	// Should be at least as long as the HTTP server's WriteTimeout for streaming (see server default).
	ctx, cancel := context.WithTimeout(r.Context(), 300*time.Second)
	defer cancel()

	// Store PromptContext in request context for later phases; attach RBAC from request context
	lifecycleCtx := &requestLifecycle{
		promptCtx:              promptCtx,
		requestID:              requestID,
		database:               h.database,
		metrics:                h.metrics,
		compatMode:             r.URL.Path == "/v1/chat/completions",
		principalRoles:         PrincipalRolesFromContext(r.Context()),
		databaseAccessMode:     DatabaseAccessModeFromContext(r.Context()),
		resolvedAccessResolver: ResolvedAccessResolverFromContext(r.Context()),
	}

	if req.Stream {
		if h.manager.SupportsTools() {
			h.handleStreamingWithTools(w, ctx, prompt, params, req.Model, lifecycleCtx)
		} else {
			h.handleStreamingResponse(w, ctx, prompt, params, req.Model, lifecycleCtx)
		}
	} else {
		h.handleNonStreamingResponse(w, ctx, prompt, params, req.Model, lifecycleCtx)
	}
}

// requestLifecycle holds state through the request lifecycle for hooks.
// When StreamWriter is set (streaming + tools path), the agentic loop sends
// PreExecute/PostExecute notifications as SSE chunks and the handler streams the final response.
type requestLifecycle struct {
	promptCtx     *PromptContext
	requestID     string
	database      DatabaseRouter
	metrics       MetricsReader
	compatMode    bool
	StreamWriter  http.ResponseWriter // optional: for streaming notifications during agentic loop
	StreamFlusher http.Flusher        // optional: flush after each SSE chunk
	StreamModel   string              // optional: model name for SSE chunk payloads
	// RBAC: from request context (set by server when Bifrost is behind auth)
	principalRoles         []string
	databaseAccessMode     auth.DatabaseAccessMode
	resolvedAccessResolver func(string) auth.ResolvedAccess
}

// sendStreamNotifications writes queued notifications as SSE chunks (used when streaming with tools).
func (h *Handler) sendStreamNotifications(lifecycle *requestLifecycle, notifs []QueuedNotification) {
	if lifecycle.compatMode {
		return
	}
	if lifecycle.StreamWriter == nil || lifecycle.StreamFlusher == nil || lifecycle.StreamModel == "" {
		return
	}
	for _, notif := range notifs {
		icon := "ℹ️"
		switch notif.Type {
		case "error":
			icon = "❌"
		case "warning":
			icon = "⚠️"
		case "success":
			icon = "✅"
		case "progress":
			icon = "🔄"
		}
		chunk := ChatResponse{
			ID:      lifecycle.requestID,
			Object:  "chat.completion.chunk",
			Model:   lifecycle.StreamModel,
			Created: time.Now().Unix(),
			Choices: []ChatChoice{
				{
					Index: 0,
					Delta: &ChatMessage{
						Role:    "heimdall",
						Content: fmt.Sprintf("[Heimdall]: %s %s: %s\n", icon, notif.Title, notif.Message),
					},
				},
			},
		}
		data, _ := json.Marshal(chunk)
		fmt.Fprintf(lifecycle.StreamWriter, "data: %s\n\n", data)
		lifecycle.StreamFlusher.Flush()
	}
}

// runAgenticLoop runs the agentic loop for any provider: execute actions, feed results back, repeat until final answer.
// For tool-capable providers (OpenAI/Ollama) uses GenerateWithTools; for local GGUF uses prompt-based multi-round.
// When inMemoryRunner is set (e.g. MCP server), its tools (store, recall, discover, etc.) are included so the LLM can manage memories in process.
func (h *Handler) runAgenticLoop(ctx context.Context, lifecycle *requestLifecycle, systemPrompt, userMessage string, params GenerateParams) (finalResponse string, err error) {
	tools := mergeMCPTools(ActionsAsMCPTools(), lifecycle.promptCtx.ExternalTools)
	if h.inMemoryRunner != nil {
		tools = mergeMCPTools(tools, h.inMemoryRunner.ToolDefinitions())
	}
	if h.manager.SupportsTools() && len(tools) > 0 {
		return h.runAgenticLoopWithTools(ctx, lifecycle, systemPrompt, userMessage, tools, params)
	}
	return h.runAgenticLoopPromptBased(ctx, lifecycle, systemPrompt, userMessage, tools, params)
}

// runAgenticLoopWithTools uses native tool calling (OpenAI/Ollama). Execute toolCalls, append results, repeat.
// systemPrompt is the full prompt built from plugins (PrePrompt hooks set AdditionalInstructions etc.); no domain logic here.
func (h *Handler) runAgenticLoopWithTools(ctx context.Context, lifecycle *requestLifecycle, systemPrompt, userMessage string, tools []MCPTool, params GenerateParams) (string, error) {
	messages := []ToolRoundMessage{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: userMessage},
	}
	var lastContent string
	for round := 0; round < MaxAgenticRounds; round++ {
		content, toolCalls, err := h.manager.GenerateWithTools(ctx, messages, tools, params)
		if err != nil {
			return "", err
		}
		lastContent = content
		if len(toolCalls) == 0 {
			if lastContent != "" {
				return lastContent, nil
			}
			continue
		}
		// Append assistant message with tool_calls
		assistantMsg := ToolRoundMessage{Role: "assistant", Content: content, ToolCalls: toolCalls}
		for i := range assistantMsg.ToolCalls {
			if assistantMsg.ToolCalls[i].Id == "" {
				assistantMsg.ToolCalls[i].Id = "call_" + generateID()
			}
		}
		messages = append(messages, assistantMsg)
		// Execute each tool and append tool result messages
		for _, tc := range toolCalls {
			var paramsMap map[string]interface{}
			if tc.Arguments != "" {
				_ = json.Unmarshal([]byte(tc.Arguments), &paramsMap)
			}
			if paramsMap == nil {
				paramsMap = make(map[string]interface{})
			}
			logToolCall(lifecycle.requestID, tc.Name, paramsMap)
			preExecCtx := &PreExecuteContext{
				Context:   ctx,
				RequestID: lifecycle.requestID, RequestTime: lifecycle.promptCtx.RequestTime,
				Action: tc.Name, Params: paramsMap, PluginData: lifecycle.promptCtx.PluginData,
				Database: lifecycle.database, Metrics: lifecycle.metrics,
				PrincipalRoles:     lifecycle.principalRoles,
				DatabaseAccessMode: lifecycle.databaseAccessMode,
				ResolvedAccess:     lifecycle.resolvedAccessResolver,
			}
			preExecCtx.SetBifrost(h.bifrost)
			preExecResult := CallPreExecuteHooks(preExecCtx)
			h.sendStreamNotifications(lifecycle, preExecCtx.DrainNotifications())
			if preExecCtx.Cancelled() {
				messages = append(messages, ToolRoundMessage{Role: "tool", ToolCallID: tc.Id, Content: "Cancelled: " + preExecCtx.CancelReason()})
				continue
			}
			if !preExecResult.Continue {
				messages = append(messages, ToolRoundMessage{Role: "tool", ToolCallID: tc.Id, Content: preExecResult.AbortMessage})
				continue
			}
			if preExecResult.ModifiedParams != nil {
				paramsMap = preExecResult.ModifiedParams
			}
			startTime := time.Now()
			var toolContent string
			if h.inMemoryRunner != nil && sliceContains(h.inMemoryRunner.ToolNames(), tc.Name) {
				dbName := lifecycle.database.DefaultDatabaseName()
				raw, execErr := h.inMemoryRunner.CallTool(ctx, tc.Name, paramsMap, dbName)
				toolContent = FormatInMemoryToolResult(raw, execErr)
				execDuration := time.Since(startTime)
				logToolResult(lifecycle.requestID, tc.Name, execDuration, execErr)
				hookResult := &ActionResult{Success: execErr == nil}
				if execErr != nil {
					hookResult.Message = execErr.Error()
				} else {
					hookResult.Message = "tool completed successfully"
					hookResult.Data = map[string]interface{}{"result": raw}
				}
				postExecCtx := &PostExecuteContext{Context: ctx, RequestID: lifecycle.requestID, Action: tc.Name, Params: paramsMap, Result: hookResult, Duration: execDuration, PluginData: lifecycle.promptCtx.PluginData}
				CallPostExecuteHooks(postExecCtx)
				h.sendStreamNotifications(lifecycle, postExecCtx.DrainNotifications())
			} else {
				actCtx := ActionContext{
					Context: ctx, UserMessage: userMessage, Params: paramsMap,
					Bifrost: h.bifrost, Database: lifecycle.database, Metrics: lifecycle.metrics,
					PrincipalRoles:     lifecycle.principalRoles,
					DatabaseAccessMode: lifecycle.databaseAccessMode,
					ResolvedAccess:     lifecycle.resolvedAccessResolver,
				}
				result, execErr := ExecuteAction(tc.Name, actCtx)
				execDuration := time.Since(startTime)
				logToolResult(lifecycle.requestID, tc.Name, execDuration, execErr)
				if execErr != nil {
					toolContent = FormatActionResultForModel(&ActionResult{Success: false, Message: execErr.Error()})
				} else {
					postExecCtx := &PostExecuteContext{Context: ctx, RequestID: lifecycle.requestID, Action: tc.Name, Params: paramsMap, Result: result, Duration: execDuration, PluginData: lifecycle.promptCtx.PluginData}
					CallPostExecuteHooks(postExecCtx)
					h.sendStreamNotifications(lifecycle, postExecCtx.DrainNotifications())
					toolContent = FormatActionResultForModel(result)
				}
			}
			messages = append(messages, ToolRoundMessage{Role: "tool", ToolCallID: tc.Id, Content: toolContent})
		}
	}
	if lastContent != "" {
		return lastContent, nil
	}
	return "I've completed the available actions. Is there anything else?", nil
}

func sliceContains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func logToolCall(requestID, action string, params map[string]interface{}) {
	log.Printf("[Heimdall] Tool call: request=%s action=%s params=%v", requestID, action, params)
}

func logToolResult(requestID, action string, duration time.Duration, err error) {
	if err != nil {
		log.Printf("[Heimdall] Tool result: request=%s action=%s duration=%v error=%v", requestID, action, duration, err)
		return
	}
	log.Printf("[Heimdall] Tool result: request=%s action=%s duration=%v success=true", requestID, action, duration)
}

// runAgenticLoopPromptBased uses prompt-based multi-round for local GGUF (or any provider without native tools).
func (h *Handler) runAgenticLoopPromptBased(ctx context.Context, lifecycle *requestLifecycle, systemPrompt, userMessage string, tools []MCPTool, params GenerateParams) (string, error) {
	answerFromContextHint := "If the ADDITIONAL CONTEXT above contains KNOWLEDGE FROM GRAPH DATABASE, answer from it in one short sentence. Otherwise output one line: {\"action\": \"<name>\", \"params\": {...}} or a direct answer.\n\nAssistant: "
	prompt := systemPrompt
	if len(tools) > 0 {
		prompt += "\n\nTOOLS AVAILABLE TO YOU:\n"
		for _, tool := range tools {
			name := strings.TrimSpace(tool.Name)
			if name == "" {
				continue
			}
			prompt += "- " + name
			if desc := strings.TrimSpace(tool.Description); desc != "" {
				prompt += ": " + desc
			}
			if len(tool.InputSchema) > 0 {
				prompt += " | inputSchema: " + string(tool.InputSchema)
			}
			prompt += "\n"
		}
	}
	prompt += "\nUser: " + userMessage + "\n\n" + answerFromContextHint
	var lastResponse string
	for round := 0; round < MaxAgenticRounds; round++ {
		response, err := h.manager.Generate(ctx, prompt, params)
		if err != nil {
			return "", err
		}
		lastResponse = response
		parsedAction, actionError := h.tryParseAction(response)
		// Treat "none" as "no action, answering from context" and return any text the model produced
		if parsedAction != nil && (parsedAction.Action == "none" || parsedAction.Action == "") {
			return response, nil
		}
		if actionError != "" {
			if lifecycle.compatMode && parseActionEnvelope(response) != nil {
				return response, nil
			}
			return actionError, nil
		}
		if parsedAction == nil {
			return response, nil
		}
		preExecCtx := &PreExecuteContext{
			Context:   ctx,
			RequestID: lifecycle.requestID, RequestTime: lifecycle.promptCtx.RequestTime,
			Action: parsedAction.Action, Params: parsedAction.Params, RawResponse: response, PluginData: lifecycle.promptCtx.PluginData,
			Database: lifecycle.database, Metrics: lifecycle.metrics,
			PrincipalRoles:     lifecycle.principalRoles,
			DatabaseAccessMode: lifecycle.databaseAccessMode,
			ResolvedAccess:     lifecycle.resolvedAccessResolver,
		}
		preExecCtx.SetBifrost(h.bifrost)
		preExecResult := CallPreExecuteHooks(preExecCtx)
		if preExecCtx.Cancelled() {
			return preExecCtx.CancelReason(), nil
		}
		if !preExecResult.Continue {
			if preExecResult.AbortMessage != "" {
				return preExecResult.AbortMessage, nil
			}
			return "Action aborted.", nil
		}
		paramsToUse := parsedAction.Params
		if preExecResult.ModifiedParams != nil {
			paramsToUse = preExecResult.ModifiedParams
		}
		logToolCall(lifecycle.requestID, parsedAction.Action, paramsToUse)
		startTime := time.Now()
		var resultStr string
		if h.inMemoryRunner != nil && sliceContains(h.inMemoryRunner.ToolNames(), parsedAction.Action) {
			dbName := lifecycle.database.DefaultDatabaseName()
			raw, execErr := h.inMemoryRunner.CallTool(ctx, parsedAction.Action, paramsToUse, dbName)
			resultStr = FormatInMemoryToolResult(raw, execErr)
			execDuration := time.Since(startTime)
			logToolResult(lifecycle.requestID, parsedAction.Action, execDuration, execErr)
			hookResult := &ActionResult{Success: execErr == nil}
			if execErr != nil {
				hookResult.Message = execErr.Error()
			} else {
				hookResult.Message = "tool completed successfully"
				hookResult.Data = map[string]interface{}{"result": raw}
			}
			CallPostExecuteHooks(&PostExecuteContext{Context: ctx, RequestID: lifecycle.requestID, Action: parsedAction.Action, Params: paramsToUse, Result: hookResult, Duration: execDuration, PluginData: lifecycle.promptCtx.PluginData})
		} else {
			actCtx := ActionContext{
				Context: ctx, UserMessage: userMessage, Params: paramsToUse,
				Bifrost: h.bifrost, Database: lifecycle.database, Metrics: lifecycle.metrics,
				PrincipalRoles:     lifecycle.principalRoles,
				DatabaseAccessMode: lifecycle.databaseAccessMode,
				ResolvedAccess:     lifecycle.resolvedAccessResolver,
			}
			result, execErr := ExecuteAction(parsedAction.Action, actCtx)
			execDuration := time.Since(startTime)
			logToolResult(lifecycle.requestID, parsedAction.Action, execDuration, execErr)
			if execErr != nil {
				prompt = prompt + response + "\n\nTool result: " + FormatActionResultForModel(&ActionResult{Success: false, Message: execErr.Error()}) + "\n\nOutput exactly one line: either {\"action\": \"<name>\", \"params\": {...}} or a brief direct answer. no repetition.\n\nAssistant: "
				continue
			}
			CallPostExecuteHooks(&PostExecuteContext{Context: ctx, RequestID: lifecycle.requestID, Action: parsedAction.Action, Params: paramsToUse, Result: result, Duration: execDuration, PluginData: lifecycle.promptCtx.PluginData})
			resultStr = FormatActionResultForModel(result)
		}
		prompt = prompt + response + "\n\nTool result: " + resultStr + "\n\nOutput exactly one line: either {\"action\": \"<name>\", \"params\": {...}} or a brief direct answer to the user. No thinking, no examples, no repetition.\n\nAssistant: "
	}
	return lastResponse, nil
}

// handleNonStreamingResponse generates complete response with lifecycle hooks.
// Uses agentic loop for any provider: execute actions, feed results back, LLM infers and formats final response.
func (h *Handler) handleNonStreamingResponse(w http.ResponseWriter, ctx context.Context, prompt string, params GenerateParams, model string, lifecycle *requestLifecycle) {
	systemPrompt := lifecycle.promptCtx.BuildFinalPrompt()
	if h.manager.SupportsTools() && len(ActionsAsMCPTools()) > 0 {
		systemPrompt = lifecycle.promptCtx.BuildFinalPromptForTools()
	}
	finalResponse, err := h.runAgenticLoop(ctx, lifecycle, systemPrompt, lifecycle.promptCtx.UserMessage, params)
	if err != nil {
		h.writeLocalizedContextError(w, ctx, localization.HeimdallGenerationFailed(err), http.StatusInternalServerError)
		return
	}
	if lifecycle.compatMode {
		if passThrough := parseActionEnvelope(finalResponse); passThrough != nil {
			if _, ok := GetHeimdallAction(passThrough.Action); !ok {
				resp := ChatResponse{
					ID:      lifecycle.requestID,
					Object:  "chat.completion",
					Model:   model,
					Created: time.Now().Unix(),
					Choices: []ChatChoice{{
						Index: 0,
						Message: &ChatMessage{
							Role:      "assistant",
							Content:   "",
							ToolCalls: []ChatToolCallWire{buildPassThroughToolCall(passThrough)},
						},
						FinishReason: "tool_calls",
					}},
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
				return
			}
		}
	}
	finalResponse = sanitizeAssistantResponse(finalResponse)

	log.Printf("[Bifrost] Agentic loop finished: %s", finalResponse)

	resp := ChatResponse{
		ID:      lifecycle.requestID,
		Object:  "chat.completion", // OpenAI API compatible
		Model:   model,
		Created: time.Now().Unix(),
		Choices: []ChatChoice{
			{
				Index: 0,
				Message: &ChatMessage{
					Role:    "assistant",
					Content: finalResponse,
				},
				FinishReason: "stop",
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleStreamingWithTools runs the agentic loop (tools path) and streams the final response.
// Used when stream=true and the provider supports tools (OpenAI/Ollama). The LLM sees tool
// results and produces a final answer (e.g. "Here's what a node looks like: ..."); we stream
// that answer and send PreExecute/PostExecute notifications during the loop.
func (h *Handler) handleStreamingWithTools(w http.ResponseWriter, ctx context.Context, prompt string, params GenerateParams, model string, lifecycle *requestLifecycle) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		h.writeLocalizedContextError(w, ctx, localization.HeimdallStreamingNotSupported(), http.StatusInternalServerError)
		return
	}

	// Set stream writer so PrePrompt and agentic-loop notifications use the same path
	lifecycle.StreamWriter = w
	lifecycle.StreamFlusher = flusher
	lifecycle.StreamModel = model
	h.sendStreamNotifications(lifecycle, lifecycle.promptCtx.DrainNotifications())

	// Use tools-friendly prompt so the model answers from context when possible instead of calling tools
	systemPrompt := lifecycle.promptCtx.BuildFinalPromptForTools()
	finalResponse, err := h.runAgenticLoop(ctx, lifecycle, systemPrompt, lifecycle.promptCtx.UserMessage, params)
	if err != nil {
		chunk := ChatResponse{
			ID: lifecycle.requestID, Object: "chat.completion.chunk", Model: model, Created: time.Now().Unix(),
			Choices: []ChatChoice{{
				Index: 0,
				Delta: &ChatMessage{Content: fmt.Sprintf("Error: %v", err)},
			}},
		}
		data, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	} else if finalResponse != "" {
		// Stream the final assistant response as one content chunk (OpenAI format)
		chunk := ChatResponse{
			ID:      lifecycle.requestID,
			Object:  "chat.completion.chunk",
			Model:   model,
			Created: time.Now().Unix(),
			Choices: []ChatChoice{{
				Index: 0,
				Delta: &ChatMessage{Content: finalResponse},
			}},
		}
		data, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Send finish chunk and [DONE]
	doneChunk := ChatResponse{
		ID: lifecycle.requestID, Object: "chat.completion.chunk", Model: model, Created: time.Now().Unix(),
		Choices: []ChatChoice{{Index: 0, Delta: &ChatMessage{}, FinishReason: "stop"}},
	}
	data, _ := json.Marshal(doneChunk)
	fmt.Fprintf(w, "data: %s\n\n", data)
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// tryParseAction parses action JSON from SLM response.
// Format: {"action": "heimdall_watcher_status", "params": {}}
// Returns (parsedAction, errorMessage). If errorMessage is set, action is invalid.
func (h *Handler) tryParseAction(response string) (*ParsedAction, string) {
	parsed := parseActionEnvelope(response)
	if parsed == nil {
		log.Printf("[Bifrost] tryParseAction: no parseable action envelope found")
		return nil, ""
	}

	log.Printf("[Bifrost] tryParseAction: looking up action: %s", parsed.Action)
	actions := ListHeimdallActions()
	log.Printf("[Bifrost] tryParseAction: registered actions: %v", actions)

	if _, ok := GetHeimdallAction(parsed.Action); !ok {
		log.Printf("[Bifrost] tryParseAction: action NOT FOUND: %s", parsed.Action)
		return nil, fmt.Sprintf("Sorry, I don't know how to perform the action '%s'. Try asking 'what can you do?' to see available actions.", parsed.Action)
	}

	log.Printf("[Bifrost] tryParseAction: action FOUND: %s", parsed.Action)
	return parsed, ""
}

// handleStreamingResponse uses Server-Sent Events (SSE) for streaming with lifecycle hooks.
// SSE is standard HTTP - works with any HTTP client, no WebSocket needed.
// After streaming completes, checks for action commands and executes them.
func (h *Handler) handleStreamingResponse(w http.ResponseWriter, ctx context.Context, prompt string, params GenerateParams, model string, lifecycle *requestLifecycle) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		h.writeLocalizedContextError(w, ctx, localization.HeimdallStreamingNotSupported(), http.StatusInternalServerError)
		return
	}

	id := lifecycle.requestID

	// === Send queued notifications from PrePrompt hooks inline ===
	// This ensures proper ordering - notifications appear before the AI response
	if lifecycle.compatMode {
		lifecycle.promptCtx.DrainNotifications()
	} else {
		notifications := lifecycle.promptCtx.DrainNotifications()
		for _, notif := range notifications {
			icon := "ℹ️"
			switch notif.Type {
			case "error":
				icon = "❌"
			case "warning":
				icon = "⚠️"
			case "success":
				icon = "✅"
			case "progress":
				icon = "🔄"
			}

			notifChunk := ChatResponse{
				ID:      id,
				Object:  "chat.completion.chunk",
				Model:   model,
				Created: time.Now().Unix(),
				Choices: []ChatChoice{
					{
						Index: 0,
						Delta: &ChatMessage{
							Role:    "heimdall",
							Content: fmt.Sprintf("[Heimdall]: %s %s: %s\n", icon, notif.Title, notif.Message),
						},
					},
				},
			}
			data, _ := json.Marshal(notifChunk)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}

	// Collect the full response before deciding whether it is an action envelope.
	// This keeps the legacy SSE path deterministic and prevents partial JSON/tool
	// envelopes from leaking into the client stream.
	var fullResponse strings.Builder

	err := h.manager.GenerateStream(ctx, prompt, params, func(token string) error {
		fullResponse.WriteString(token)
		return nil
	})

	if err != nil {
		// Send error event
		fmt.Fprintf(w, "data: {\"error\": \"%s\"}\n\n", err.Error())
		flusher.Flush()
		return
	}

	// Check if response contains an action command
	response := fullResponse.String()
	response = sanitizeAssistantResponse(response)
	log.Printf("[Bifrost] Streaming complete, checking for action: %s", response)

	parsedAction, actionError := h.tryParseAction(response)
	if parsedAction == nil && actionError == "" {
		chunk := ChatResponse{
			ID:      id,
			Object:  "chat.completion.chunk",
			Model:   model,
			Created: time.Now().Unix(),
			Choices: []ChatChoice{{
				Index: 0,
				Delta: &ChatMessage{Content: response},
			}},
		}
		data, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Handle action not found error
	if actionError != "" {
		if lifecycle.compatMode {
			if passThrough := parseActionEnvelope(response); passThrough != nil {
				chunk := ChatResponse{
					ID:      id,
					Object:  "chat.completion.chunk",
					Model:   model,
					Created: time.Now().Unix(),
					Choices: []ChatChoice{{
						Index: 0,
						Delta: &ChatMessage{
							Role:      "assistant",
							ToolCalls: []ChatToolCallWire{buildPassThroughToolCall(passThrough)},
						},
						FinishReason: "tool_calls",
					}},
				}
				data, _ := json.Marshal(chunk)
				fmt.Fprintf(w, "data: %s\n\n", data)
				fmt.Fprintf(w, "data: [DONE]\n\n")
				flusher.Flush()
				return
			}
		}
		fmt.Fprintf(w, "data: %s\n\n", actionError)
		flusher.Flush()
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
		return
	}

	if parsedAction != nil {
		log.Printf("[Bifrost] Action detected in stream: %s", parsedAction.Action)

		// === Phase 4: PreExecute hooks ===
		preExecCtx := &PreExecuteContext{
			RequestID:          lifecycle.requestID,
			RequestTime:        lifecycle.promptCtx.RequestTime,
			Action:             parsedAction.Action,
			Params:             parsedAction.Params,
			RawResponse:        response,
			PluginData:         lifecycle.promptCtx.PluginData,
			Database:           lifecycle.database,
			Metrics:            lifecycle.metrics,
			PrincipalRoles:     lifecycle.principalRoles,
			DatabaseAccessMode: lifecycle.databaseAccessMode,
			ResolvedAccess:     lifecycle.resolvedAccessResolver,
		}
		// Set Bifrost for notifications (fire-and-forget SSE messages)
		preExecCtx.SetBifrost(h.bifrost)

		// === PreExecute hooks (optional) ===
		// Plugins that implement PreExecuteHook can validate/modify params
		preExecResult := CallPreExecuteHooks(preExecCtx)
		cancelled := preExecCtx.Cancelled()
		if cancelled {
			log.Printf("[Bifrost] Request cancelled by %s: %s", preExecCtx.CancelledBy(), preExecCtx.CancelReason())
			// Send cancellation as SSE chunk
			cancelChunk := ChatResponse{
				ID:      id,
				Object:  "chat.completion.chunk",
				Model:   model,
				Created: time.Now().Unix(),
				Choices: []ChatChoice{
					{
						Index: 0,
						Delta: &ChatMessage{
							Content: fmt.Sprintf("\n\n⚠️ Request cancelled by %s: %s",
								preExecCtx.CancelledBy(), preExecCtx.CancelReason()),
						},
					},
				},
			}
			data, _ := json.Marshal(cancelChunk)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}

		if !cancelled {
			// === Send PreExecute notifications inline ===
			preExecNotifications := preExecCtx.DrainNotifications()
			for _, notif := range preExecNotifications {
				icon := "ℹ️"
				switch notif.Type {
				case "error":
					icon = "❌"
				case "warning":
					icon = "⚠️"
				case "success":
					icon = "✅"
				case "progress":
					icon = "🔄"
				}
				notifChunk := ChatResponse{
					ID:      id,
					Object:  "chat.completion.chunk",
					Model:   model,
					Created: time.Now().Unix(),
					Choices: []ChatChoice{
						{
							Index: 0,
							Delta: &ChatMessage{
								Role:    "heimdall",
								Content: fmt.Sprintf("[Heimdall]: %s %s: %s\n", icon, notif.Title, notif.Message),
							},
						},
					},
				}
				data, _ := json.Marshal(notifChunk)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}

			var actionResponse string
			var result *ActionResult
			var execDuration time.Duration

			if !preExecResult.Continue {
				actionResponse = preExecResult.AbortMessage
				if actionResponse == "" {
					actionResponse = "Action aborted by plugin"
				}
			} else {
				// === Phase 5: Execute action ===
				startTime := time.Now()
				var err error
				if h.inMemoryRunner != nil && sliceContains(h.inMemoryRunner.ToolNames(), parsedAction.Action) {
					dbName := lifecycle.database.DefaultDatabaseName()
					raw, callErr := h.inMemoryRunner.CallTool(ctx, parsedAction.Action, parsedAction.Params, dbName)
					err = callErr
					execDuration = time.Since(startTime)
					if err != nil {
						actionResponse = fmt.Sprintf("Action failed: %v", err)
						result = &ActionResult{Success: false, Message: actionResponse}
					} else {
						actionResponse = "Action completed successfully"
						result = &ActionResult{Success: true, Message: actionResponse, Data: map[string]interface{}{"result": raw}}
					}
				} else {
					actCtx := ActionContext{
						Context:            ctx,
						UserMessage:        prompt,
						Params:             parsedAction.Params,
						Bifrost:            h.bifrost,
						Database:           h.database,
						Metrics:            h.metrics,
						PrincipalRoles:     lifecycle.principalRoles,
						DatabaseAccessMode: lifecycle.databaseAccessMode,
						ResolvedAccess:     lifecycle.resolvedAccessResolver,
					}
					result, err = ExecuteAction(parsedAction.Action, actCtx)
					execDuration = time.Since(startTime)
				}

				if err != nil {
					log.Printf("[Bifrost] Action execution failed: %v", err)
					actionResponse = fmt.Sprintf("Action failed: %v", err)
				} else if result != nil {
					log.Printf("[Bifrost] Action result: success=%v", result.Success)
				}

				// === Phase 6: PostExecute hooks (optional) ===
				// Plugins that implement PostExecuteHook get notified
				postExecCtx := &PostExecuteContext{
					RequestID:  lifecycle.requestID,
					Action:     parsedAction.Action,
					Params:     parsedAction.Params,
					Result:     result,
					Duration:   execDuration,
					PluginData: lifecycle.promptCtx.PluginData,
				}
				CallPostExecuteHooks(postExecCtx)

				// === Send PostExecute notifications inline ===
				postExecNotifications := postExecCtx.DrainNotifications()
				for _, notif := range postExecNotifications {
					icon := "ℹ️"
					switch notif.Type {
					case "error":
						icon = "❌"
					case "warning":
						icon = "⚠️"
					case "success":
						icon = "✅"
					case "progress":
						icon = "🔄"
					}
					notifChunk := ChatResponse{
						ID:      id,
						Object:  "chat.completion.chunk",
						Model:   model,
						Created: time.Now().Unix(),
						Choices: []ChatChoice{
							{
								Index: 0,
								Delta: &ChatMessage{
									Role:    "heimdall",
									Content: fmt.Sprintf("[Heimdall]: %s %s: %s\n", icon, notif.Title, notif.Message),
								},
							},
						},
					}
					data, _ := json.Marshal(notifChunk)
					fmt.Fprintf(w, "data: %s\n\n", data)
					flusher.Flush()
				}

				// === Phase 7: Synthesis hooks ===
				// Allow plugins to provide custom response formatting
				if result != nil {
					synthCtx := &SynthesisContext{
						Context:      ctx,
						RequestID:    lifecycle.requestID,
						UserQuestion: prompt,
						Action:       parsedAction.Action,
						Result:       result,
						PluginData:   lifecycle.promptCtx.PluginData,
						Database:     h.database,
					}

					// First, try plugin synthesis hooks
					if pluginResponse := CallSynthesisHooks(synthCtx); pluginResponse != "" {
						log.Printf("[Bifrost] Using plugin-provided synthesis")
						actionResponse = "\n\n" + pluginResponse
					} else {
						// Fall back to simple formatting (no LLM synthesis)
						log.Printf("[Bifrost] Using default format (no plugin synthesis)")
						actionResponse = "\n\n" + h.defaultFormatResponse(result)
					}
				}
			}

			// Send action result chunk
			resultChunk := ChatResponse{
				ID:      id,
				Object:  "chat.completion.chunk",
				Model:   model,
				Created: time.Now().Unix(),
				Choices: []ChatChoice{
					{
						Index: 0,
						Delta: &ChatMessage{
							Content: actionResponse,
						},
					},
				},
			}
			data, _ := json.Marshal(resultChunk)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}

	// Send final chunk with finish_reason (OpenAI format)
	doneChunk := ChatResponse{
		ID:      id,
		Object:  "chat.completion.chunk", // OpenAI API streaming format
		Model:   model,
		Created: time.Now().Unix(),
		Choices: []ChatChoice{
			{
				Index:        0,
				Delta:        &ChatMessage{},
				FinishReason: "stop",
			},
		},
	}
	data, _ := json.Marshal(doneChunk)
	fmt.Fprintf(w, "data: %s\n\n", data)
	// OpenAI sends [DONE] to signal stream end
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// defaultFormatResponse provides a simple fallback format for action results.
// This is a no-op that just formats the data - actual synthesis should be done by plugins.
//
// Plugins that implement SynthesisHook can provide rich, domain-specific formatting
// (e.g., the watcher plugin provides LLM-based prose synthesis).
func (h *Handler) defaultFormatResponse(result *ActionResult) string {
	if result == nil {
		return "No results available."
	}

	if !result.Success {
		return "Action failed: " + result.Message
	}

	// If there's no structured data, just return the message
	if len(result.Data) == 0 {
		return result.Message
	}

	// Simple format: message + JSON data
	dataJSON, err := json.MarshalIndent(result.Data, "", "  ")
	if err != nil {
		return result.Message
	}

	return result.Message + "\n\n```json\n" + string(dataJSON) + "\n```"
}
