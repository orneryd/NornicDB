package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestCypherKnowledgePolicyDescriptorsPreserveExactEnglish(t *testing.T) {
	cause := errors.New("forced knowledge policy failure")
	testCases := []struct {
		message Message
		text    string
	}{
		{CypherKnowledgePolicyExpectedAfter("PROFILE", "APPLY"), "expected PROFILE after APPLY"},
		{CypherKnowledgePolicyProfileNameExpectedAfter("CREATE DECAY PROFILE"), "expected profile name after CREATE DECAY PROFILE"},
		{CypherKnowledgePolicyExpectedAfterProfileName("OPTIONS", "slow-decay"), `expected OPTIONS after profile name "slow-decay"`},
		{CypherKnowledgePolicyPolicyNameExpectedAfter("CREATE PROMOTION POLICY"), "expected policy name after CREATE PROMOTION POLICY"},
		{CypherKnowledgePolicyLabelExpectedAfterColon(), "expected label after ':'"},
		{CypherKnowledgePolicyEdgeTypeExpectedAfterColon(), "expected edge type after ':'"},
		{CypherKnowledgePolicyEdgePatternExpectedFor("'('"), "expected '(' for edge pattern"},
		{CypherKnowledgePolicyEdgePatternExpectedIn("'-'"), "expected '-' in edge pattern"},
		{CypherKnowledgePolicyNumberExpectedAfter("DECAY FLOOR"), "expected number after DECAY FLOOR"},
		{CypherKnowledgePolicySecondsExpectedAfter("DECAY HALF LIFE"), "expected seconds after DECAY HALF LIFE"},
		{CypherKnowledgePolicyKalmanClosingBraceExpected(), "expected } in KALMAN config block"},
		{CypherKnowledgePolicyExpressionExpectedAfterSet(), "expected expression after SET"},
		{CypherKnowledgePolicyInvalidValue("scope", "UNIVERSE", true), `invalid scope: "UNIVERSE"`},
		{CypherKnowledgePolicyUnknownOption("bogusField"), `unknown option: "bogusField"`},
		{CypherKnowledgePolicyUnknownKalmanConfigKey("gain"), `unknown Kalman config key: "gain"`},
		{CypherKnowledgePolicyUnsupportedCommand("MATCH (n) RETURN n"), "unsupported knowledge policy command: MATCH (n) RETURN n"},
		{CypherKnowledgePolicySchemaManagerUnavailable(), "schema manager unavailable"},
		{CypherKnowledgePolicyUnsupportedCommandType("*cypher.unknown"), "unsupported knowledge policy command type *cypher.unknown"},
		{CypherKnowledgePolicyOperationFailed("CREATE DECAY PROFILE", cause), "forced knowledge policy failure"},
		{CypherKnowledgePolicyResolveTargetRequired(), "nornicdb.knowledgepolicy.resolve requires entityId, labels, or edgeType"},
		{CypherKnowledgePolicyBindingTableUnavailable(), "knowledge policy binding table unavailable"},
		{CypherKnowledgePolicyEntityNotFound("node-1"), "entity not found: node-1"},
		{CypherKnowledgePolicyArgumentStringRequired(2), "argument 2 must be a string"},
		{CypherKnowledgePolicyDeindexBadgerRequired(), "deindex status requires BadgerDB storage backend"},
		{CypherKnowledgePolicyDecayOptionsMapRequired("int"), "decayScore/decay options must be a map, got int"},
		{CypherKnowledgePolicyUnknownDecayOption("badKey"), `unknown decay option key: "badKey"`},
		{CypherKnowledgePolicyReasonNoEntityArgument(), "no entity argument"},
		{CypherKnowledgePolicyReasonEntityNotFound(), "entity not found"},
		{CypherKnowledgePolicyReasonNoBadgerEngine(), "no BadgerEngine"},
		{CypherKnowledgePolicyReasonDecayDisabled(), "decay subsystem disabled"},
		{CypherKnowledgePolicyReasonNoDecayProfile(), "no decay profile"},
		{CypherKnowledgePolicyReasonNoDecay(), "no decay"},
	}

	require.Len(t, testCases, 32)
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	seen := make(map[MessageID]struct{}, len(testCases))
	for _, testCase := range testCases {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
		_, duplicate := seen[testCase.message.ID]
		require.False(t, duplicate, testCase.message.ID)
		seen[testCase.message.ID] = struct{}{}
		rendered, tag, err := manager.Render(WithPreferences(context.Background(), language.AmericanEnglish), testCase.message)
		require.NoError(t, err, testCase.message.ID)
		require.Equal(t, language.AmericanEnglish, tag, testCase.message.ID)
		require.Equal(t, testCase.text, rendered, testCase.message.ID)
	}
}

func TestCypherKnowledgePolicyCatalogRenderingAndCauseIdentity(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := CypherKnowledgePolicyExpectedAfter("PROFILE", "APPLY")

	spanish, tag, err := manager.Render(WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "se esperaba PROFILE después de APPLY", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! expected PROFILE after APPLY !!]", pseudo)

	cause := errors.New("forced knowledge policy failure")
	localizedErr := NewLocalizedError(string(MessageCypherKnowledgePolicyOperationFailed), CypherKnowledgePolicyOperationFailed("ALTER DECAY PROFILE", cause), cause)
	require.ErrorIs(t, localizedErr, cause)
	require.Equal(t, "ALTER DECAY PROFILE", localizedErr.Message.Data["Operation"])
}
