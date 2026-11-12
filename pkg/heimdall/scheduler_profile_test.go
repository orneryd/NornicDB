package heimdall

// PROFILE ownership for the model provider boundary.
//
// Manager is the only owner that invokes the configured Generator, so each
// actual Generate, GenerateStream and GenerateWithTools call is reported once
// at that boundary and nowhere above it. Chat delegates to Generate and adds
// nothing of its own. The provider is an arbitrary implementation whose
// database access the caller cannot see, so its actual invocation stays
// explicitly unknown; every branch that returns before the call reports
// nothing.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// profiledManagerContext returns a fresh recorder and the context carrying it.
func profiledManagerContext() (*fixtureowner.QueryReadObserver, context.Context) {
	recorder := &fixtureowner.QueryReadObserver{}
	return recorder, queryprofile.WithObserver(context.Background(), recorder)
}

func TestManagerReportsActualGeneratorInvocationsOnly(t *testing.T) {
	t.Run("generate", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := newTestManager(NewMockGenerator("/test/model.gguf"))

		_, err := manager.Generate(ctx, "hello", DefaultGenerateParams())
		require.NoError(t, err)
		require.Equal(t, []string{"inference provider"}, recorder.Callbacks())
	})

	t.Run("generate stream", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := newTestManager(NewMockGenerator("/test/model.gguf"))

		require.NoError(t, manager.GenerateStream(ctx, "hello", DefaultGenerateParams(), func(string) error { return nil }))
		require.Equal(t, []string{"inference provider"}, recorder.Callbacks())
	})

	t.Run("generate with tools", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := newTestManager(&captureToolsGenerator{
			MockGenerator: NewMockGenerator("/test/model.gguf"),
			content:       "tool-response",
		})

		_, _, err := manager.GenerateWithTools(ctx,
			[]ToolRoundMessage{{Role: "user", Content: "hello"}},
			[]MCPTool{{Name: "lookup"}}, DefaultGenerateParams())
		require.NoError(t, err)
		require.Equal(t, []string{"inference provider"}, recorder.Callbacks())
	})

	// Chat delegates to Generate, and that owner reports the one actual call:
	// the wrapper adds no second invocation of its own.
	t.Run("chat reports the one call its delegate makes", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := newTestManager(NewMockGenerator("/test/model.gguf"))

		_, err := manager.Chat(ctx, ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "hello"}}})
		require.NoError(t, err)
		require.Equal(t, []string{"inference provider"}, recorder.Callbacks())
	})

	t.Run("failed generation is still an actual call", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := newTestManager(&ErrorGenerator{generateErr: errors.New("provider unavailable")})

		_, err := manager.Generate(ctx, "hello", DefaultGenerateParams())
		require.ErrorContains(t, err, "provider unavailable")
		require.Equal(t, []string{"inference provider"}, recorder.Callbacks())
	})

	t.Run("no generator loaded", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := &Manager{}

		_, err := manager.Generate(ctx, "hello", DefaultGenerateParams())
		require.ErrorContains(t, err, "no generator loaded")
		require.Empty(t, recorder.Callbacks())
	})

	t.Run("closed manager", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := newTestManager(NewMockGenerator("/test/model.gguf"))
		manager.work.CloseAdmission()

		_, err := manager.Generate(ctx, "hello", DefaultGenerateParams())
		require.ErrorContains(t, err, "closed")
		require.Empty(t, recorder.Callbacks())
	})

	t.Run("generator without tools", func(t *testing.T) {
		recorder, ctx := profiledManagerContext()
		manager := newTestManager(NewMockGenerator("/test/model.gguf"))

		_, _, err := manager.GenerateWithTools(ctx, nil, nil, DefaultGenerateParams())
		require.ErrorContains(t, err, "does not support tools")
		require.Empty(t, recorder.Callbacks())
	})
}
