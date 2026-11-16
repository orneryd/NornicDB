package heimdall

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConversationReasoningDecoderPreservesSplitMarkers(t *testing.T) {
	for _, size := range []int{1, 2, 5, 100} {
		var thought, answer strings.Builder
		decoder := reasoningDecoder{emit: func(reasoning bool, text string) error {
			if reasoning {
				thought.WriteString(text)
			} else {
				answer.WriteString(text)
			}
			return nil
		}}
		input := "<think>Check schema</think>The answer contains <literal> text."
		for start := 0; start < len(input); start += size {
			end := start + size
			if end > len(input) {
				end = len(input)
			}
			require.NoError(t, decoder.feed(input[start:end], false))
		}
		require.NoError(t, decoder.feed("", true))
		require.Equal(t, "Check schema", thought.String())
		require.Equal(t, "The answer contains <literal> text.", answer.String())
	}
}

func TestConversationStreamsBeforeGenerationCompletes(t *testing.T) {
	recorder := httptest.NewRecorder()
	generator := NewMockGenerator("test")
	generator.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
		require.NoError(t, callback("Check schema</thi"))
		require.Contains(t, recorder.Body.String(), "nornic.reasoning")
		require.NoError(t, callback("nk>Hello "))
		require.Contains(t, recorder.Body.String(), "Hello ", "first answer tokens must be sent before the rest are generated")
		return callback("world")
	}
	handler := testHandler(newTestManager(generator), Config{Model: "qwen3.5-0.8b"})
	lifecycle := &requestLifecycle{promptCtx: &PromptContext{Thinking: true}, StreamWriter: recorder, StreamFlusher: recorder, StreamModel: "qwen3.5-0.8b"}
	answer, err := handler.generateConversationRound(t.Context(), lifecycle, "prompt", DefaultGenerateParams())
	require.NoError(t, err)
	require.Equal(t, "Hello world", answer)
	require.Equal(t, "Check schema", lifecycle.reasoning.String())
	require.NotContains(t, lifecycle.streamedAnswer.String(), "Check schema")
}

func TestConversationDoesNotStreamToolEnvelopeAsAnswer(t *testing.T) {
	recorder := httptest.NewRecorder()
	generator := NewMockGenerator("test")
	generator.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
		return callback(`{"action":"heimdall_watcher_db_stats","params":{}}`)
	}
	handler := testHandler(newTestManager(generator), Config{})
	lifecycle := &requestLifecycle{promptCtx: &PromptContext{}, StreamWriter: recorder, StreamFlusher: recorder}
	answer, err := handler.generateConversationRound(t.Context(), lifecycle, "prompt", DefaultGenerateParams())
	require.NoError(t, err)
	require.Contains(t, answer, "heimdall_watcher_db_stats")
	require.NotContains(t,recorder.Body.String(),"heimdall_watcher_db_stats")
	require.NotContains(t,recorder.Body.String(),`"delta"`)
}
