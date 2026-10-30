package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestCypherMatchingCatalogRendering(t *testing.T) {
	paths := []string{
		"catalog/active.cyphermatching.en-US.yaml",
		"catalog/active.cyphermatching.es-ES.yaml",
		"catalog/active.cyphermatching.en-XA.yaml",
	}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))

	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}

	message := CypherMatchingTraversalPatternInvalid("(a)-[r")
	config := &i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data}

	spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "patrón de recorrido no válido: (a)-[r", spanish)

	pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "[!! invalid traversal pattern: (a)-[r !!]", pseudo)
}

func TestCypherMatchingLabelExpressionMessagesRender(t *testing.T) {
	paths := []string{
		"catalog/active.cyphermatching.en-US.yaml",
		"catalog/active.cyphermatching.es-ES.yaml",
		"catalog/active.cyphermatching.en-XA.yaml",
	}
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		message Message
		spanish string
	}{
		{CypherMatchingLabelExpressionMixedColon("A|(B&C)"), "No se permite mezclar los símbolos de expresión de etiquetas ('|', '&', '!' y '%') con los dos puntos (':') entre etiquetas. Use un solo conjunto de símbolos. Esta expresión podría escribirse como :A|(B&C)."},
		{CypherMatchingLabelExpressionMixedIs("A&B"), "No se permite mezclar la palabra clave IS con los dos puntos (':') entre etiquetas. Esta expresión podría escribirse como IS A&B."},
		{CypherMatchingRelationshipTypeColonDisjunction("R|S"), "Ya no se admite separar tipos de relación alternativos con dos puntos junto con\nuna variable, predicados de propiedades en línea o longitud variable.\nSepare los tipos de relación con `:R|S`."},
		{CypherMatchingRelationshipTypeColonConjunction(), "Los tipos de relación de una expresión de tipos de relación no se pueden combinar con ':'"},
		{CypherMatchingVariableLengthTypeExpression(), "Las relaciones de longitud variable no deben usar expresiones de tipos de relación."},
		{CypherMatchingLabelExpressionInWritePattern("CREATE"), "Las expresiones de etiquetas en patrones no se permiten en una cláusula CREATE, solo en una cláusula MATCH y en expresiones"},
		{CypherMatchingRelationshipTypeExpressionInWritePattern("MERGE"), "Las expresiones de tipos de relación en patrones no se permiten en una cláusula MERGE, solo en una cláusula MATCH"},
		{CypherMatchingIsNotOperandInvalid("Code"), "Entrada no válida 'Code': se esperaba '::', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NORMALIZED', 'NULL' o 'TYPED'"},
		{CypherMatchingSingleRelationshipTypeRequired("CREATE"), "Se debe especificar un único tipo de relación para CREATE"},
	} {
		config := &i18n.LocalizeConfig{MessageID: string(tc.message.ID), TemplateData: tc.message.Data}
		english, err := i18n.NewLocalizer(bundle, "en-US").Localize(config)
		require.NoError(t, err)
		require.Equal(t, tc.message.Fallback, english, tc.message.ID)
		spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
		require.NoError(t, err)
		require.Equal(t, tc.spanish, spanish, tc.message.ID)
		pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(config)
		require.NoError(t, err)
		require.Equal(t, "[!! "+tc.message.Fallback+" !!]", pseudo, tc.message.ID)
	}
}
