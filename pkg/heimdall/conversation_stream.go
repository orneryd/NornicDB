package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// reasoningDecoder preserves marker fragments across arbitrary token boundaries.
// Model-provided reasoning is transported separately and never put in history.
type reasoningDecoder struct {
	pending  string
	thinking bool
	emit     func(bool, string) error
}

func (d *reasoningDecoder) feed(text string, final bool) error {
	d.pending += text
	markers := []string{"<think>", "</think>"}
	for d.pending != "" {
		at := -1
		marker := ""
		for _, candidate := range markers {
			if index := strings.Index(d.pending, candidate); index >= 0 && (at < 0 || index < at) {
				at = index
				marker = candidate
			}
		}
		if at >= 0 {
			if at > 0 {
				if err := d.emit(d.thinking, d.pending[:at]); err != nil {
					return err
				}
			}
			d.thinking = marker == "<think>"
			d.pending = d.pending[at+len(marker):]
			continue
		}
		keep := 0
		if !final {
			for _, candidate := range markers {
				for n := 1; n < len(candidate) && n <= len(d.pending); n++ {
					if strings.HasSuffix(d.pending, candidate[:n]) && n > keep {
						keep = n
					}
				}
			}
		}
		visible := len(d.pending) - keep
		if visible > 0 {
			if err := d.emit(d.thinking, d.pending[:visible]); err != nil {
				return err
			}
			d.pending = d.pending[visible:]
		}
		break
	}
	return nil
}

func (h *Handler) generateConversationRound(ctx context.Context, lifecycle *requestLifecycle, prompt string, params GenerateParams) (string, error) {
	if err := lifecycle.updateContextUsage(EstimateTokens(prompt), params.MaxTokens); err != nil {
		return "", err
	}
	var answer strings.Builder
	decision := 0 // 0: empty/whitespace, 1: direct answer, 2: possible tool envelope
	emitText := func(text string) error {
		if lifecycle.StreamWriter == nil {
			return nil
		}
		chunk := ChatResponse{ID: lifecycle.requestID, Object: "chat.completion.chunk", Model: lifecycle.StreamModel, Created: lifecycle.promptCtx.RequestTime.Unix(), Choices: []ChatChoice{{Index: 0, Delta: &ChatMessage{Content: text}}}}
		data, _ := json.Marshal(chunk)
		if _, err := fmt.Fprintf(lifecycle.StreamWriter, "data: %s\n\n", data); err != nil {
			return err
		}
		lifecycle.StreamFlusher.Flush()
		lifecycle.streamedAnswer.WriteString(text)
		return nil
	}
	decoder := reasoningDecoder{thinking: lifecycle.promptCtx.Thinking && strings.Contains(strings.ToLower(h.announcedModel()), "qwen3.5")}
	decoder.emit = func(thinking bool, text string) error {
		if text == "" {
			return nil
		}
		if thinking {
			lifecycle.reasoning.WriteString(text)
			if lifecycle.StreamWriter != nil {
				data, _ := json.Marshal(map[string]string{"text": text})
				if _, err := fmt.Fprintf(lifecycle.StreamWriter, "event: nornic.reasoning\ndata: %s\n\n", data); err != nil {
					return err
				}
				lifecycle.StreamFlusher.Flush()
			}
			return nil
		}
		answer.WriteString(text)
		if decision == 0 {
			trimmed := strings.TrimSpace(answer.String())
			if trimmed == "" {
				return nil
			}
			if strings.HasPrefix(trimmed, "{") {
				decision = 2
				return nil
			}
			decision = 1
			return emitText(answer.String())
		}
		if decision == 1 {
			return emitText(text)
		}
		return nil
	}
	err := h.manager.GenerateStream(ctx, prompt, params, func(token string) error { return decoder.feed(token, false) })
	if flushErr := decoder.feed("", true); err == nil {
		err = flushErr
	}
	// JSON answers are retained until they can be distinguished from tool calls.
	// The turn handler emits them once if they are the final answer.
	return answer.String(), err
}
