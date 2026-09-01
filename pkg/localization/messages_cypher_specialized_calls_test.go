package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestCypherSpecializedCallsDescriptorsPreserveEnglishAndData(t *testing.T) {
	cause := errors.New("forced specialized call failure")
	testCases := []struct {
		message Message
		text    string
	}{
		{CypherSpecializedCallsEmbedQueryTextFailed("needle", cause), "failed to embed query 'needle': forced specialized call failure"},
		{CypherSpecializedCallsParameterUnsupportedType("embedding", "bool"), "parameter $embedding has unsupported type for vector query: bool (expected []float32, []float64, []interface{}, or string)"},
		{CypherSpecializedCallsFulltextIndexNotFound("articles"), "there is no such fulltext schema index: articles"},
		{CypherSpecializedCallsFulltextOptionInvalid("skip", -1), "invalid fulltext options.skip: -1"},
		{CypherSpecializedCallsTemporalReadNodesFailed("History", cause), `failed to read nodes for label "History": forced specialized call failure`},
		{CypherSpecializedCallsTemporalOverlap("accountId", 42), "temporal overlap detected for accountId=42"},
		{CypherSpecializedCallsTxlogInvalidSequence("fromSeq", cause), "invalid fromSeq: forced specialized call failure"},
	}

	for _, testCase := range testCases {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
	}
	require.Equal(t, "embedding", testCases[1].message.Data["Parameter"])
	require.Equal(t, "bool", testCases[1].message.Data["ValueType"])
	require.Equal(t, "articles", testCases[2].message.Data["Index"])
	require.Equal(t, "accountId", testCases[5].message.Data["Property"])
	require.Equal(t, "42", testCases[5].message.Data["Value"])
}

func TestCypherSpecializedCallsLocalizedErrorPreservesCause(t *testing.T) {
	cause := errors.New("forced specialized call failure")
	err := NewLocalizedError(string(MessageCypherSpecializedCallsTxlogReadEntriesFailed), CypherSpecializedCallsTxlogReadEntriesFailed(cause), cause)

	require.ErrorIs(t, err, cause)
	var localizedErr *LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, MessageCypherSpecializedCallsTxlogReadEntriesFailed, localizedErr.Message.ID)
	require.Equal(t, cause.Error(), localizedErr.Message.Data["Cause"])
}

func TestCypherSpecializedCallsCatalogRendering(t *testing.T) {
	paths := []string{
		"catalog/active.cypherspecializedcalls.en-US.yaml",
		"catalog/active.cypherspecializedcalls.es-ES.yaml",
		"catalog/active.cypherspecializedcalls.en-XA.yaml",
	}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))

	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}

	message := CypherSpecializedCallsParameterNotProvided("embedding")
	config := &i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data}

	spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "no se proporcionó el parámetro $embedding", spanish)

	pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "[!! parameter $embedding not provided !!]", pseudo)

	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	rendered, tag, err := manager.Render(WithPreferences(context.Background(), language.AmericanEnglish), message)
	require.NoError(t, err)
	require.Equal(t, language.AmericanEnglish, tag)
	require.Equal(t, message.Fallback, rendered)
}
