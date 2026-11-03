package heimdall

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

// closeClassification is the transport/retry class a generator close failure
// carries; redaction must keep errors.As able to find it.
type closeClassification struct{ code string }

func (e *closeClassification) Error() string { return "synthetic close classification " + e.code }

// closeRedactionGenerator fails Close with the credentials its manager was
// configured with, and counts how often the one stop reaches it.
type closeRedactionGenerator struct {
	err    error
	closes atomic.Int32
}

func (g *closeRedactionGenerator) Generate(context.Context, string, GenerateParams) (string, error) {
	return "", nil
}

func (g *closeRedactionGenerator) GenerateStream(context.Context, string, GenerateParams, func(string) error) error {
	return nil
}

func (g *closeRedactionGenerator) Close() error {
	g.closes.Add(1)
	return g.err
}

func (g *closeRedactionGenerator) ModelPath() string { return "close-redaction" }

func newCloseRedactionManager(t *testing.T, cfg Config, generator *closeRedactionGenerator) *Manager {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, cfg.Model+".gguf"), []byte("synthetic"), 0o644))
	cfg.Enabled = true
	cfg.ModelsDir = dir
	original := SetGeneratorLoader(func(string, int, int, int, int) (Generator, error) { return generator, nil })
	t.Cleanup(func() { SetGeneratorLoader(original) })
	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)
	// This helper owns the manager it constructed: every exit completes that
	// manager's one stop, including an exit that fails a later assertion. The
	// injected generator close failure is the test's explicitly expected
	// outcome, so cleanup joins the retained outcome and checks it instead of
	// discarding a new failure.
	t.Cleanup(func() {
		outcome := manager.Close()
		require.True(t, errors.Is(outcome, generator.err),
			"the test-owned manager's stop lost the injected generator close failure: %v", outcome)
		var carried *closeClassification
		require.True(t, errors.As(outcome, &carried),
			"the test-owned manager's stop lost the generator close classification: %v", outcome)
		require.Equal(t, "E-CLOSE", carried.code)
		require.EqualValues(t, 1, generator.closes.Load(),
			"the test-owned manager did not close its generator exactly once")
	})
	return manager
}

// Manager.Close reports the one generator-close outcome to every caller. An
// actual generator close failure that contains the configured APIURL userinfo
// and API key must be published redacted, keep its classification, and be the
// same retained outcome when Close is repeated.
func TestHeimdallManagerCloseRedactsGeneratorCloseError(t *testing.T) {
	endpoint := "https://synthetic-close-user:synthetic-close-pass@example.invalid/v1"
	apiKey := "synthetic-close-key"
	fragments := observability.ClientCredentials(endpoint, apiKey)
	require.NotEmpty(t, fragments)

	classification := &closeClassification{code: "E-CLOSE"}
	sentinel := errors.New("synthetic generator close failure")
	generatorFailure := fmt.Errorf("generator close failed for %s key %s: %w",
		endpoint, apiKey, errors.Join(sentinel, classification))
	require.True(t, managerOutputContainsCredential(generatorFailure.Error(), fragments),
		"synthetic generator failure did not carry the configured credentials: %q", generatorFailure.Error())

	generator := &closeRedactionGenerator{err: generatorFailure}
	manager := newCloseRedactionManager(t, Config{
		Model:  "close-redaction",
		APIURL: endpoint,
		APIKey: apiKey,
	}, generator)

	first := manager.Close()
	require.Error(t, first, "the injected generator close failure was not reported")
	require.True(t, errors.Is(first, sentinel), "lost the generator close cause: %v", first)
	var carried *closeClassification
	require.True(t, errors.As(first, &carried), "lost the generator close classification: %v", first)
	require.Equal(t, "E-CLOSE", carried.code)
	require.False(t, managerOutputContainsCredential(first.Error(), fragments),
		"Close exposed a configured credential: %q", first.Error())
	require.Contains(t, first.Error(), observability.RedactedPlaceholder())
	require.Contains(t, first.Error(), "generator close failed")
	require.EqualValues(t, 1, generator.closes.Load())

	// The stop is joined, not repeated: later callers receive the same retained
	// outcome, still redacted and still classifiable.
	for _, repeated := range []error{manager.Close(), manager.Close()} {
		require.Error(t, repeated, "a repeated Close dropped the retained outcome")
		require.Equal(t, first.Error(), repeated.Error(), "a repeated Close published a different outcome")
		require.True(t, errors.Is(repeated, sentinel), "a repeated Close lost the cause: %v", repeated)
		var repeatedClass *closeClassification
		require.True(t, errors.As(repeated, &repeatedClass), "a repeated Close lost the classification: %v", repeated)
		require.False(t, managerOutputContainsCredential(repeated.Error(), fragments),
			"a repeated Close exposed a configured credential: %q", repeated.Error())
	}
	require.EqualValues(t, 1, generator.closes.Load(), "the generator was closed more than once")
}
