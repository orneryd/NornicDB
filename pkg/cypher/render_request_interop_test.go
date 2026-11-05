package cypher_test

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

type retainedMetadataRenderer struct{ messages []*localization.Message }

var _ cypher.ProcedureMetadataRenderer = (*retainedMetadataRenderer)(nil)

func (r *retainedMetadataRenderer) Render(_ context.Context, request localization.RenderRequest) (string, language.Tag, error) {
	if request.Purpose != localization.MessageText || request.Message == nil {
		return "", language.AmericanEnglish, fmt.Errorf("metadata requires a message source")
	}
	r.messages = append(r.messages, request.Message)
	return "external:" + string(request.Message.ID), language.AmericanEnglish, nil
}

func TestShowProceduresSupportsExternalRequestRendererAndRetainedSnapshots(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	executor := cypher.NewStorageExecutor(base)
	renderer := &retainedMetadataRenderer{}
	executor.SetLocalizationRenderer(renderer)
	original := cypher.ListRegisteredProcedures()
	byName := make(map[string]cypher.ProcedureSpec, len(original))
	expectedRendered := 0
	for _, spec := range original {
		byName[spec.Name] = spec
		if spec.DescriptionMessage.ID != "" {
			expectedRendered++
		}
	}
	result, err := executor.Execute(t.Context(), "SHOW PROCEDURES YIELD name, description RETURN name, description", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, len(original))
	require.Positive(t, expectedRendered)
	require.Len(t, renderer.messages, expectedRendered)
	seen := make(map[*localization.Message]bool, expectedRendered)
	for _, message := range renderer.messages {
		seen[message] = true
	}
	require.Len(t, seen, expectedRendered, "each request references its own fresh descriptor snapshot")
	for _, row := range result.Rows {
		require.Len(t, row, 2)
		name, ok := row[0].(string)
		require.True(t, ok)
		spec, ok := byName[name]
		require.True(t, ok)
		description := spec.Description
		if spec.DescriptionMessage.ID != "" {
			description = "external:" + string(spec.DescriptionMessage.ID)
		}
		require.Equal(t, description, row[1], name)
	}
	// Requests remain usable after Execute returns, but their descriptor headers
	// belong to that call's list snapshot rather than the registry itself.
	for _, message := range renderer.messages {
		message.ID = "retained.header.mutation"
		message.Fallback = "retained fallback mutation"
	}
	current := cypher.ListRegisteredProcedures()
	require.Len(t, current, len(original))
	for _, spec := range current {
		prior, ok := byName[spec.Name]
		require.True(t, ok)
		require.Equal(t, prior.DescriptionMessage.ID, spec.DescriptionMessage.ID, spec.Name)
		require.Equal(t, prior.DescriptionMessage.Fallback, spec.DescriptionMessage.Fallback, spec.Name)
	}
}
