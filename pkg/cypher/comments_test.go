package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripCypherCommentsPreservesQuotedCommentMarkers(t *testing.T) {
	query := "RETURN 'https://example.test/a/*b*/' AS url, `a//b` AS name"
	require.Equal(t, query, stripCypherComments(query))
	require.Equal(t, query, stripCypherComments(query))
}

func TestStripCypherCommentsPreservesClauseBoundaries(t *testing.T) {
	require.Equal(t, "RETURN 1 AS value \nRETURN 2", stripCypherComments("RETURN 1 AS value // note\nRETURN 2"))
	require.Equal(t, "RETURN 1   AS value", stripCypherComments("RETURN 1 /* note */ AS value"))
}
