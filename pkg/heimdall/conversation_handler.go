package heimdall

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

func BuildModelPrompt(model string, messages []ChatMessage, thinking bool) string {
	prompt := BuildPrompt(messages)
	// Qwen3.5's published template closes an empty thinking block when thinking
	// is disabled. Omitting it leaves raw-completion inference in thinking mode.
	if strings.Contains(strings.ToLower(model), "qwen3.5") {
		if thinking {
			prompt += "<think>\n"
		} else {
			prompt += "<think>\n\n</think>\n\n"
		}
	}
	return prompt
}

// Private conversations use one turn lifecycle for both response transports.
// The plugin owns history; a successful terminal event follows its durable save.
func (h *Handler) handleConversationTurn(w http.ResponseWriter, r *http.Request, req ChatRequest) {
	content := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			content = req.Messages[i].Content
			break
		}
	}
	turn, err := OpenConversationTurn(WithConversationAttachments(r.Context(), req.AttachmentIDs), req.ConversationID, req.MessageID, content)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, ErrConversationBusy) {
			status = http.StatusConflict
		}
		if errors.Is(err, ErrConversationNotFound) {
			status = http.StatusNotFound
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		valueconvert.PropertyJSON.NewEncoder(w).Encode(map[string]string{"message": err.Error()})
		return
	}
	defer turn.Release()
	original := turn.Context
	// Source tools retain the actor's live grants, narrowed to read-only. Private
	// transcript writes are separately authorized by ChatWorkspaceHost.
	ctx := auth.WithReadOnlyDatabaseScope(original, turn.Database)
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	ctx = WithConversationScope(ctx, req.ConversationID)
	prompt := &PromptContext{Context: ctx, Thinking: req.Thinking, ConversationID: req.ConversationID, SourceDatabase: turn.Database, RequestID: generateID(), RequestTime: time.Now(), ActionPrompt: ActionPromptForRequest(ctx, turn.Database), UserMessage: content, Messages: turn.Messages, PluginData: map[string]interface{}{"message_id": req.MessageID}}
	prompt.SetBifrost(h.bifrost)
	lifecycle := &requestLifecycle{promptCtx: prompt, requestID: prompt.RequestID, database: newConversationDatabase(h.database, prompt), metrics: h.metrics, principalRoles: PrincipalRolesFromContext(ctx), databaseAccessMode: DatabaseAccessModeFromContext(ctx), resolvedAccessResolver: ResolvedAccessResolverFromContext(ctx)}
	var flusher http.Flusher
	lifecycle.contextLimit = h.config.ContextSize
	if lifecycle.contextLimit <= 0 {
		lifecycle.contextLimit = 8192
	}
	if req.Stream {
		var ok bool
		flusher, ok = w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming is unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		lifecycle.StreamWriter = w
		lifecycle.StreamFlusher = flusher
		lifecycle.StreamModel = req.Model
	}
	fail := func(failure error) {
		if req.Stream {
			data, _ := valueconvert.PropertyJSON.Marshal(map[string]string{"message": failure.Error()})
			fmt.Fprintf(w, "event: nornic.error\ndata: %s\n\n", data)
			flusher.Flush()
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			valueconvert.PropertyJSON.NewEncoder(w).Encode(map[string]string{"message": failure.Error()})
		}
	}
	CallPrePromptHooks(prompt)
	h.sendStreamNotifications(lifecycle, prompt.DrainNotifications())
	params := DefaultGenerateParams()
	params.MaxTokens = req.MaxTokens
	if params.MaxTokens <= 0 {
		params.MaxTokens = h.config.MaxTokens
	}
	if params.MaxTokens <= 0 {
		params.MaxTokens = 1024
	}
	if req.Temperature != 0 {
		params.Temperature = req.Temperature
	}
	if req.TopP != 0 {
		params.TopP = req.TopP
	}
	var reply string
	if prompt.Cancelled() {
		err = fmt.Errorf("%s", prompt.CancelReason())
	} else if budgetErr := prompt.ValidateTokenBudget(); budgetErr != nil {
		err = budgetErr
	} else {
		// Select the response contract first so the turn builds its system prompt once.
		system := prompt.BuildFinalPrompt(h.promptModeForProvider())
		reply, err = h.runAgenticLoop(ctx, lifecycle, system, prompt.UserMessage, params)
	}
	reply = strings.TrimSpace(sanitizeAssistantResponse(reply))
	status := "completed"
	if err != nil {
		status = "error"
		if ctx.Err() != nil {
			status = "cancelled"
		}
		if reply == "" {
			reply = err.Error()
		}
	}
	// A browser cancellation must not discard the already accepted user turn.
	saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(original), 5*time.Second)
	saveErr := turn.Complete(saveCtx, ConversationReply{Content: reply, Reasoning: lifecycle.reasoning.String(), Status: status, Tools: lifecycle.tools, ContextTokens: lifecycle.contextTokens, ContextLimit: lifecycle.contextLimit})
	saveCancel()
	if saveErr != nil {
		fail(fmt.Errorf("could not save this response: %w", saveErr))
		return
	}
	if err != nil {
		fail(err)
		return
	}
	response := ChatResponse{ID: prompt.RequestID, Object: "chat.completion", Model: req.Model, Created: time.Now().Unix(), Choices: []ChatChoice{{Index: 0, Message: &ChatMessage{Role: "assistant", Content: reply}, FinishReason: "stop"}}}
	if !req.Stream {
		w.Header().Set("Content-Type", "application/json")
		valueconvert.PropertyJSON.NewEncoder(w).Encode(response)
		return
	}
	response.Object = "chat.completion.chunk"
	response.Choices[0].Message = nil
	response.Choices[0].Delta = &ChatMessage{Role: "assistant"}
	if lifecycle.streamedAnswer.Len() == 0 {
		response.Choices[0].Delta.Content = reply
	}
	data, _ := valueconvert.PropertyJSON.Marshal(response)
	fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", data)
	flusher.Flush()
}
