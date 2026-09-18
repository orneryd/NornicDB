package voyage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMultimodalContentAcceptsMixedTextAndImageRepresentations(t *testing.T) {
	parts, err := ParseMultimodalContent([]any{
		map[string]any{"type": "text", "text": "caption"},
		map[string]any{"type": "image_url", "image_url": "https://example.invalid/image.png"},
	})
	require.NoError(t, err)
	require.Len(t, parts, 2)
	require.Equal(t, "caption", parts[0].Text)
	require.Equal(t, "https://example.invalid/image.png", parts[1].ImageURL)

	parts, err = ParseMultimodalContent(`[{"type":"image_base64","image_base64":"data:image/png;base64,YQ=="}]`)
	require.NoError(t, err)
	require.Equal(t, "data:image/png;base64,YQ==", parts[0].ImageBase64)
}

func TestEmbedMultimodalRejectsTooManyInputsAsTerminal(t *testing.T) {
	client, err := NewClient(Config{APIKey: "key", BaseURL: "http://127.0.0.1", MaxRetries: -1})
	require.NoError(t, err)
	inputs := make([]any, MultimodalMaxInputs+1)
	for index := range inputs {
		inputs[index] = MultimodalInput{Content: []MultimodalPart{{Type: "text", Text: "caption"}}}
	}
	_, err = client.EmbedMultimodal(context.Background(), inputs, MultimodalOptions{})
	require.Error(t, err)
	var classified interface{ Retryable() bool }
	require.True(t, errors.As(err, &classified))
	require.False(t, classified.Retryable())
}

func TestParseMultimodalContentRejectsUnsafeOrOversizedImages(t *testing.T) {
	invalid := []any{
		[]any{map[string]any{"type": "image_url", "image_url": "file:///etc/passwd"}},
		[]any{map[string]any{"type": "image_base64", "image_base64": "data:image/svg+xml;base64,YQ=="}},
		[]any{map[string]any{"type": "text", "text": "caption", "image_url": "https://example.invalid/image.png"}},
		[]any{},
	}
	for _, value := range invalid {
		_, err := ParseMultimodalContent(value)
		require.Error(t, err)
	}
}
