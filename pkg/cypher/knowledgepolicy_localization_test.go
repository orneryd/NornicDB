package cypher

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireCypherKnowledgePolicyLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestKnowledgePolicyParserErrorsHaveTypedIdentityAndExactEnglish(t *testing.T) {
	testCases := []struct {
		name      string
		messageID localization.MessageID
		text      string
		parse     func() error
	}{
		{
			name:      "expected after",
			messageID: localization.MessageCypherKnowledgePolicyProfileNameExpectedAfter,
			text:      "expected profile name after CREATE DECAY PROFILE",
			parse: func() error {
				_, _, err := ParseKnowledgePolicyDDL("CREATE DECAY PROFILE")
				return err
			},
		},
		{
			name:      "expected for",
			messageID: localization.MessageCypherKnowledgePolicyEdgePatternExpectedFor,
			text:      "expected '(' for edge pattern",
			parse: func() error {
				_, _, err := parseEdgeTarget("", 0, knowledgePolicyTestBinding())
				return err
			},
		},
		{
			name:      "expected in",
			messageID: localization.MessageCypherKnowledgePolicyEdgePatternExpectedIn,
			text:      "expected '-' in edge pattern",
			parse: func() error {
				_, _, err := parseEdgeTarget("()", 0, knowledgePolicyTestBinding())
				return err
			},
		},
		{
			name:      "unknown option",
			messageID: localization.MessageCypherKnowledgePolicyUnknownOption,
			text:      `unknown option: "bogus"`,
			parse: func() error {
				_, _, err := ParseKnowledgePolicyDDL("CREATE DECAY PROFILE profile OPTIONS { bogus: 1 }")
				return err
			},
		},
		{
			name:      "unknown Kalman key",
			messageID: localization.MessageCypherKnowledgePolicyUnknownKalmanConfigKey,
			text:      `unknown Kalman config key: "gain"`,
			parse: func() error {
				_, err := parseOnAccessBlock("WITH KALMAN { gain: 1 } SET n.score = 1")
				return err
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requireCypherKnowledgePolicyLocalizedError(t, testCase.parse(), testCase.messageID, testCase.text)
		})
	}

	_, _, err := ParseKnowledgePolicyDDL("CREATE DECAY PROFILE profile OPTIONS { halfLifeSeconds: nope }")
	localizedErr := requireCypherKnowledgePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyInvalidValue, "invalid halfLifeSeconds: nope")
	require.ErrorIs(t, err, strconv.ErrSyntax)
	require.Equal(t, "halfLifeSeconds", localizedErr.Message.Data["Field"])
	require.Equal(t, "nope", localizedErr.Message.Data["Value"])
}

func knowledgePolicyTestBinding() knowledgepolicy.DecayProfileBinding {
	return knowledgepolicy.DecayProfileBinding{}
}

func TestKnowledgePolicyProcedureErrorsPreserveMachineData(t *testing.T) {
	_, err := optionalStringArg([]interface{}{42}, 0)
	localizedErr := requireCypherKnowledgePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyArgumentStringRequired, "argument 1 must be a string")
	require.Equal(t, 1, localizedErr.Message.Data["Position"])

	cause := errors.New("forced knowledge policy failure")
	_, wrapped := knowledgePolicySchemaResult("CREATE DECAY PROFILE", cause)
	requireCypherKnowledgePolicyLocalizedError(t, wrapped, localization.MessageCypherKnowledgePolicyOperationFailed, "forced knowledge policy failure")
	require.ErrorIs(t, wrapped, cause)
}

func TestKnowledgePolicyExecutionAndFunctionErrorsHaveTypedIdentity(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.executeKnowledgePolicyDDL(context.Background(), "MATCH (n) RETURN n")
	localizedErr := requireCypherKnowledgePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyUnsupportedCommand, "unsupported knowledge policy command: MATCH (n) RETURN n")
	require.Equal(t, "MATCH (n) RETURN n", localizedErr.Message.Data["Command"])

	_, err = validateDecayOptions(context.Background(), "42", nil, nil, exec)
	localizedErr = requireCypherKnowledgePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyDecayOptionsMapRequired, "decayScore/decay options must be a map, got int64")
	require.Equal(t, "int64", localizedErr.Message.Data["ValueType"])

	_, err = validateDecayOptions(context.Background(), "{badKey: 'x'}", nil, nil, exec)
	localizedErr = requireCypherKnowledgePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyUnknownDecayOption, `unknown decay option key: "badKey"`)
	require.Equal(t, "badKey", localizedErr.Message.Data["Option"])
}

func TestKnowledgePolicyReturnedHumanTextUsesDescriptors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	result, err := exec.callNornicDbKnowledgePolicyDeindexStatus()
	require.NoError(t, err)
	require.Equal(t, localization.CypherKnowledgePolicyDeindexBadgerRequired().Fallback, result.Rows[0][2])

	require.Equal(t, localization.CypherKnowledgePolicyReasonNoEntityArgument().Fallback, decayDisabledMap(localization.CypherKnowledgePolicyReasonNoEntityArgument().Fallback)["reason"])
	require.Equal(t, localization.CypherKnowledgePolicyReasonNoDecay().Fallback, resolutionToMap(knowledgepolicy.ScoringResolution{NoDecay: true})["reason"])
	require.Equal(t, localization.CypherKnowledgePolicyReasonNoDecayProfile().Fallback, resolutionToMap(knowledgepolicy.ScoringResolution{})["reason"])
}
