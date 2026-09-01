package search

import (
	"context"
	"errors"
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestSearchCandidateLocalizationDescriptors(t *testing.T) {
	cause := errors.New("backend unavailable")
	tests := []struct {
		name    string
		message localization.Message
		id      localization.MessageID
		english string
		cause   error
	}{
		{name: "gpu k-means clustered index required", message: localization.SearchGPUKMeansClusteredIndexRequired(), id: localization.MessageSearchGPUKMeansClusteredIndexRequired, english: "gpu k-means candidate gen requires clustered index"},
		{name: "cluster HNSW lookup missing", message: localization.SearchClusterHNSWLookupNotConfigured(), id: localization.MessageSearchClusterHNSWLookupNotConfigured, english: "cluster HNSW lookup not configured"},
		{name: "IVFPQ index missing", message: localization.SearchIVFPQIndexNotConfigured(), id: localization.MessageSearchIVFPQIndexNotConfigured, english: "ivfpq index not configured"},
		{name: "cluster query dimensions mismatch", message: localization.SearchClusterQueryDimensionsMismatch(2, 3), id: localization.MessageSearchClusterQueryDimensionsMismatch, english: "cluster search failed: query dimensions 2 != index dimensions 3"},
		{name: "cluster search failure", message: localization.SearchClusterFailed(cause), id: localization.MessageSearchClusterFailed, english: "cluster search failed: backend unavailable", cause: cause},
		{name: "HNSW index creation failure", message: localization.SearchHNSWIndexCreationFailed(cause), id: localization.MessageSearchHNSWIndexCreationFailed, english: "failed to create HNSW index: backend unavailable", cause: cause},
		{name: "vector index nil", message: localization.SearchVectorIndexNil(), id: localization.MessageSearchVectorIndexNil, english: "vector index is nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := localizedError(tt.message, tt.cause)
			require.EqualError(t, err, tt.english)
			var localizedErr *nornicerrors.Localized
			require.ErrorAs(t, err, &localizedErr)
			require.Equal(t, tt.id, localizedErr.Message.ID)
			if tt.cause == nil {
				require.NoError(t, errors.Unwrap(err))
			} else {
				require.ErrorIs(t, err, tt.cause)
			}
		})
	}
}

func TestSearchCandidateLocalizationDirectGenerators(t *testing.T) {
	tests := []struct {
		name    string
		search  func() error
		id      localization.MessageID
		english string
	}{
		{
			name: "GPU k-means",
			search: func() error {
				_, err := NewGPUKMeansCandidateGen(nil, 1).SearchCandidates(context.Background(), nil, 1, 0)
				return err
			},
			id:      localization.MessageSearchGPUKMeansClusteredIndexRequired,
			english: "gpu k-means candidate gen requires clustered index",
		},
		{
			name: "IVF-HNSW cluster index",
			search: func() error {
				_, err := NewIVFHNSWCandidateGen(nil, nil, 1).SearchCandidates(context.Background(), nil, 1, 0)
				return err
			},
			id:      localization.MessageSearchClusterNotClustered,
			english: "cluster index not clustered",
		},
		{
			name: "IVFPQ index",
			search: func() error {
				_, err := NewIVFPQCandidateGen(nil, 1).SearchCandidates(context.Background(), nil, 1, 0)
				return err
			},
			id:      localization.MessageSearchIVFPQIndexNotConfigured,
			english: "ivfpq index not configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.search()
			require.EqualError(t, err, tt.english)
			var localizedErr *nornicerrors.Localized
			require.ErrorAs(t, err, &localizedErr)
			require.Equal(t, tt.id, localizedErr.Message.ID)
		})
	}
}

func TestSearchCandidateLocalizationHNSWInitializationCause(t *testing.T) {
	service := &Service{}
	_, err := service.resolveStandardVectorStrategy(context.Background(), 0, 3, nil)
	require.EqualError(t, err, "failed to create HNSW index: vector index is nil")

	var outer *nornicerrors.Localized
	require.ErrorAs(t, err, &outer)
	require.Equal(t, localization.MessageSearchHNSWIndexCreationFailed, outer.Message.ID)

	cause := errors.Unwrap(err)
	require.Error(t, cause)
	require.ErrorIs(t, err, cause)
	var inner *nornicerrors.Localized
	require.ErrorAs(t, cause, &inner)
	require.Equal(t, localization.MessageSearchVectorIndexNil, inner.Message.ID)
}

func TestSearchCandidateLocalizationLocaleRendering(t *testing.T) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range []string{
		"../localization/catalog/active.searchcandidates.en-US.yaml",
		"../localization/catalog/active.searchcandidates.es-ES.yaml",
		"../localization/catalog/active.searchcandidates.en-XA.yaml",
	} {
		_, err := bundle.LoadMessageFile(path)
		require.NoError(t, err)
	}

	tests := []struct {
		name     string
		locale   language.Tag
		message  localization.Message
		expected string
	}{
		{name: "Spanish validation", locale: language.EuropeanSpanish, message: localization.SearchIVFPQIndexNotConfigured(), expected: "índice IVFPQ no configurado"},
		{name: "Spanish dimensions", locale: language.EuropeanSpanish, message: localization.SearchClusterQueryDimensionsMismatch(2, 3), expected: "error de búsqueda por grupos: las dimensiones de la consulta 2 != las dimensiones del índice 3"},
		{name: "pseudo-locale cause", locale: language.MustParse("en-XA"), message: localization.SearchClusterFailed(errors.New("backend unavailable")), expected: "[!! cluster search failed: backend unavailable !!]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localizer := i18n.NewLocalizer(bundle, tt.locale.String())
			text, renderErr := localizer.Localize(&i18n.LocalizeConfig{
				MessageID:    string(tt.message.ID),
				TemplateData: tt.message.Data,
			})
			require.NoError(t, renderErr)
			require.Equal(t, tt.expected, text)
		})
	}
}
