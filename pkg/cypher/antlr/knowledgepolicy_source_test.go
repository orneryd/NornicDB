package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Author: #1663 lane (relocation). Existing release control from 04c87de3e,
// Git author mm0nst3r; human/lane attribution is not inferred from that name.
func TestKnowledgePolicyStoredSourceValidity(t *testing.T) {
	source := policyDDLSource{Source: cyphertext.NewSource(t.Context(), "", cyphertext.AcceptedSource), original: true}
	text, err := source.storedText(0, 0)
	require.NoError(t, err)
	require.Empty(t, text)
	_, err = source.storedText(1, 1)
	require.ErrorIs(t, err, storage.ErrInvalidData)
	source = policyDDLSource{Source: &cyphertext.Source{Original: "x", Text: "xx"}, original: true}
	_, err = source.storedText(0, 2)
	require.ErrorIs(t, err, storage.ErrInvalidData)
}
