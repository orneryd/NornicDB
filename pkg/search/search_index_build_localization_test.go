package search

import (
	"context"
	"errors"
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/gpu/cuda"
	"github.com/orneryd/nornicdb/pkg/gpu/vulkan"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestSearchIndexBuildLocalizationDescriptors(t *testing.T) {
	cause := errors.New("backend unavailable")
	tests := []struct {
		name    string
		message localization.Message
		id      localization.MessageID
		english string
		cause   error
	}{
		{name: "vector file magic", message: localization.SearchVectorFileMagicInvalid(), id: localization.MessageSearchVectorFileMagicInvalid, english: "invalid vector file magic"},
		{name: "vector file version", message: localization.SearchVectorFileVersionUnsupported(2), id: localization.MessageSearchVectorFileVersionUnsupported, english: "unsupported vector file version 2"},
		{name: "vector file dimensions", message: localization.SearchVectorFileDimensionsMismatch(2, 3), id: localization.MessageSearchVectorFileDimensionsMismatch, english: "vector file dimensions 2 != store dimensions 3"},
		{name: "vector metadata dimensions", message: localization.SearchVectorMetaDimensionsMismatch(2, 3), id: localization.MessageSearchVectorMetaDimensionsMismatch, english: "meta dimensions 2 != store dimensions 3"},
		{name: "GPU disabled", message: localization.SearchGPUNotEnabled(), id: localization.MessageSearchGPUNotEnabled, english: "gpu not enabled"},
		{name: "vector dimensions", message: localization.SearchVectorDimensionsInvalid(), id: localization.MessageSearchVectorDimensionsInvalid, english: "invalid vector dimensions"},
		{name: "IVFPQ vector store required", message: localization.SearchIVFPQVectorStoreRequired(), id: localization.MessageSearchIVFPQVectorStoreRequired, english: "vector file store is required"},
		{name: "IVFPQ dimensions", message: localization.SearchIVFPQDimensionsInvalid(), id: localization.MessageSearchIVFPQDimensionsInvalid, english: "invalid dimensions"},
		{name: "IVFPQ segments", message: localization.SearchIVFPQSegmentsInvalid(3, 2), id: localization.MessageSearchIVFPQSegmentsInvalid, english: "invalid pq segments: dimensions=3 segments=2"},
		{name: "IVFPQ training vectors", message: localization.SearchIVFPQTrainingVectorsInsufficient(1, 2), id: localization.MessageSearchIVFPQTrainingVectorsInsufficient, english: "insufficient training vectors (1) for ivf lists (2)"},
		{name: "IVF coarse training", message: localization.SearchIVFCoarseTrainingFailed(cause), id: localization.MessageSearchIVFCoarseTrainingFailed, english: "ivf coarse training failed: backend unavailable", cause: cause},
		{name: "PQ codebook training", message: localization.SearchPQCodebookTrainingFailed(cause), id: localization.MessageSearchPQCodebookTrainingFailed, english: "pq codebook training failed: backend unavailable", cause: cause},
		{name: "HNSW GPU dimensions", message: localization.SearchHNSWGPUBuildDimensionInvalid(0), id: localization.MessageSearchHNSWGPUBuildDimensionInvalid, english: "invalid HNSW GPU build dimension 0"},
		{name: "GPU accelerator", message: localization.SearchGPUAcceleratorUnavailable(), id: localization.MessageSearchGPUAcceleratorUnavailable, english: "no GPU accelerator available"},
		{name: "CUDA unavailable", message: localization.SearchCUDANotAvailable(), id: localization.MessageSearchCUDANotAvailable, english: "cuda: CUDA is not available on this system", cause: cuda.ErrCUDANotAvailable},
		{name: "Vulkan unavailable", message: localization.SearchVulkanNotAvailable(), id: localization.MessageSearchVulkanNotAvailable, english: "vulkan: Vulkan is not available on this system", cause: vulkan.ErrVulkanNotAvailable},
		{name: "Vulkan device", message: localization.SearchVulkanDeviceNotInitialized(), id: localization.MessageSearchVulkanDeviceNotInitialized, english: "vulkan: device not initialized"},
		{name: "IVFPQ vector store unavailable", message: localization.SearchIVFPQVectorStoreUnavailable(), id: localization.MessageSearchIVFPQVectorStoreUnavailable, english: "vector file store unavailable for IVFPQ build"},
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

func TestSearchIndexBuildLocalizationDirectValidation(t *testing.T) {
	_, _, err := BuildIVFPQFromVectorStore(context.Background(), nil, IVFPQProfile{}, nil)
	assertLocalizedSearchIndexBuildError(t, err, localization.MessageSearchIVFPQVectorStoreRequired, "vector file store is required")

	err = NewCPUHNSWBuildAccelerator().Prepare(0, 0)
	assertLocalizedSearchIndexBuildError(t, err, localization.MessageSearchHNSWGPUBuildDimensionInvalid, "invalid HNSW GPU build dimension 0")

	err = (&Service{}).ensureGPUIndexSynced(nil, nil)
	assertLocalizedSearchIndexBuildError(t, err, localization.MessageSearchGPUNotEnabled, "gpu not enabled")

	_, err = (&Service{}).getOrBuildIVFPQIndex(context.Background(), IVFPQProfile{}, nil)
	assertLocalizedSearchIndexBuildError(t, err, localization.MessageSearchIVFPQVectorStoreUnavailable, "vector file store unavailable for IVFPQ build")
}

func TestSearchIndexBuildLocalizationLocaleRendering(t *testing.T) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range []string{
		"../localization/catalog/active.searchindexbuild.en-US.yaml",
		"../localization/catalog/active.searchindexbuild.es-ES.yaml",
		"../localization/catalog/active.searchindexbuild.en-XA.yaml",
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
		{name: "Spanish validation", locale: language.EuropeanSpanish, message: localization.SearchIVFPQSegmentsInvalid(3, 2), expected: "segmentos PQ no válidos: dimensiones=3 segmentos=2"},
		{name: "Spanish cause", locale: language.EuropeanSpanish, message: localization.SearchIVFCoarseTrainingFailed(errors.New("backend unavailable")), expected: "error en el entrenamiento grueso de IVF: backend unavailable"},
		{name: "pseudo validation", locale: language.MustParse("en-XA"), message: localization.SearchVectorFileVersionUnsupported(2), expected: "[!! unsupported vector file version 2 !!]"},
		{name: "pseudo cause", locale: language.MustParse("en-XA"), message: localization.SearchPQCodebookTrainingFailed(errors.New("backend unavailable")), expected: "[!! pq codebook training failed: backend unavailable !!]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localizer := i18n.NewLocalizer(bundle, tt.locale.String())
			text, err := localizer.Localize(&i18n.LocalizeConfig{MessageID: string(tt.message.ID), TemplateData: tt.message.Data})
			require.NoError(t, err)
			require.Equal(t, tt.expected, text)
		})
	}
}

func assertLocalizedSearchIndexBuildError(t *testing.T, err error, id localization.MessageID, english string) {
	t.Helper()
	require.EqualError(t, err, english)
	var localizedErr *nornicerrors.Localized
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, id, localizedErr.Message.ID)
}
