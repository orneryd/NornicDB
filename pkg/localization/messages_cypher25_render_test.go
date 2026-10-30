package localization

import (
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// The Cypher 25 batch's messages (#1028) render in every catalog: en-US as
// the fallback text, en-XA wrapped, es-ES translated (#907).
func TestCypher25BatchMessagesRender(t *testing.T) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, catalog := range []string{"cyphercore", "cypherschema", "cypherproceduremetadata"} {
		for _, locale := range []string{"en-US", "es-ES", "en-XA"} {
			_, err := bundle.LoadMessageFileFS(catalogFS, "catalog/active."+catalog+"."+locale+".yaml")
			require.NoError(t, err)
		}
	}
	for _, message := range []Message{
		CypherCoreRegexReplacementNoNamedGroup("g"),
		CypherCoreFormatTemplateValueType("%d", "'x'"),
		CypherCoreFormatTemplateValueCount(2, 1),
		CypherCoreFormatTemplateVerbInvalid("%q"),
		CypherCoreTemporalPatternInvalidCharacter("Date"),
		CypherCoreDurationPatternUnbalancedEscapes(),
		CypherCoreTemporalPatternMismatch("yyyy", "x", "Date"),
		CypherCoreRegexReplacementIllegalGroupReference(),
		CypherCoreRegexReplacementGroupIndexMissing(),
		CypherCoreRegexReplacementEscapeMissing(),
		CypherCoreRegexReplacementNamedGroupUnterminated(),
		CypherCoreRegexReplacementNoGroup(2),
		CypherCoreReduceFormInvalidSyntax("reduce", "reduce(acc = init, x IN list | expression)"),
		CypherCorePredicateNotBoolean("1"),
		CypherCoreTemporalPatternRequiresString(),
		CypherCoreTrimSpecificationUnknown(),
		CypherCorePatternNodeValueType("n", "Integer"),
		CypherCoreEntityReferencedInSameClause("node", "n", "CREATE"),
		CypherProcedureMetadataDbSchemaNodeTypeProperties(),
		CypherProcedureMetadataDbSchemaRelTypeProperties(),
		CypherSchemaConstraintEntityMismatch("IS UNIQUE", "(n:L)"),
	} {
		render := func(locale string) string {
			text, err := i18n.NewLocalizer(bundle, locale).Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
			require.NoError(t, err, "%s %s", message.ID, locale)
			return text
		}
		require.Equal(t, message.Fallback, render("en-US"), message.ID)
		require.Equal(t, "[!! "+message.Fallback+" !!]", render("en-XA"), message.ID)
		require.NotEmpty(t, render("es-ES"), message.ID)
	}
}
