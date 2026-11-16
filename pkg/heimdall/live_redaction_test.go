package heimdall

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

func TestLiveModelRedactionStreamsSafeWordsWithOwnedCredentials(t *testing.T) {
	var result strings.Builder
	redactor := liveModelRedactor{credentials: []string{"private phrase"}, emit: func(text string) error { result.WriteString(text); return nil }}
	require.NoError(t, redactor.write("Visible answer ", false))
	require.Equal(t, "Visible answer ", result.String(), "unrelated owned credentials must not force whole-response buffering")
	require.NoError(t, redactor.write("private ", false))
	require.NotContains(t, result.String(), "private")
	require.NoError(t, redactor.write("phrase and more", false))
	require.NoError(t, redactor.write("", true))
	require.Equal(t, observability.RedactText("Visible answer private phrase and more", "private phrase"), result.String())
}

func TestLiveModelRedactionProtectsEveryCredentialSplit(t *testing.T) {
	for _, input := range []string{"Prefix private phrase suffix", "Prefix https://user:pass@example.invalid/path?api_key=hidden suffix", "Prefix private%20phrase suffix", `{"password":"private phrase"}`} {
		for size := 1; size <= len(input); size++ {
			var result strings.Builder
			redactor := liveModelRedactor{credentials: []string{"private phrase"}, emit: func(text string) error { result.WriteString(text); return nil }}
			for start := 0; start < len(input); start += size {
				end := start + size
				if end > len(input) {
					end = len(input)
				}
				require.NoError(t, redactor.write(input[start:end], false))
			}
			require.NoError(t, redactor.write("", true))
			require.Equal(t, observability.RedactText(input, "private phrase"), result.String(), "chunk size %d", size)
		}
	}
}
