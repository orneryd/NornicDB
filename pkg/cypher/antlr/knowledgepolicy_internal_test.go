package antlr

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Author: #1663 lane (relocation). These release controls follow the policy
// grammar helpers they exercise into this package; inputs and expectations are
// unchanged, and declaration blocks are read through a carried policy input.

func testPolicyInput(text string) policyInput {
	return policyInput{source: cyphertext.NewSource(context.Background(), text, cyphertext.RawSource), product: &knowledgePolicySyntax{}}
}

func TestKnowledgePolicyExpectByte(t *testing.T) {
	require.Equal(t, 1, kpExpectByte("{x", 0, '{'))
	require.Equal(t, -1, kpExpectByte("{x", 1, '{'))
}

func TestKnowledgePolicyScanBraceBlock(t *testing.T) {
	for _, test := range []struct {
		name, source, value string
		end                 int
	}{
		{"TestKpScanBraceBlock_Balanced", "{ a: 1, b: { c: 2 } }", " a: 1, b: { c: 2 } ", 21},
		{"TestKpScanBraceBlock_NoTrailingWhitespace", "{abc}", "abc", 5},
		{"TestKpScanBraceBlock_Unbalanced", "{ a: 1, b: { c: 2 }", "", -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			block, end := kpScanBraceBlock(testPolicyInput(test.source), 0)
			assert.Equal(t, test.end, end)
			if end >= 0 {
				assert.Equal(t, test.value, block.text())
			}
		})
	}
}

func requirePolicyLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) {
	t.Helper()
	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
}

func TestKnowledgePolicyGrammarHelperErrorsHaveTypedIdentity(t *testing.T) {
	_, _, err := parseEdgeTarget("", 0, knowledgepolicy.DecayProfileBinding{})
	requirePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyEdgePatternExpectedFor, "expected '(' for edge pattern")
	_, _, err = parseEdgeTarget("()", 0, knowledgepolicy.DecayProfileBinding{})
	requirePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyEdgePatternExpectedIn, "expected '-' in edge pattern")
	_, err = parseOnAccessBlock(testPolicyInput("WITH KALMAN { gain: 1 } SET n.score = 1"))
	requirePolicyLocalizedError(t, err, localization.MessageCypherKnowledgePolicyUnknownKalmanConfigKey, `unknown Kalman config key: "gain"`)
}
