package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The shared eligibility consumer serves both RUN and COMMIT. A canceled
// classification is ineligible; its bool return cannot replace the commit cause.
func TestRetryConsumerPreservesCommitFailureOnCancellation(t *testing.T) {
	query := "MERGE (u:U {k:1}) SET u.k=1"
	failure := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "U", Properties: []string{"k"}}
	session := &Session{server: &Server{config: DefaultConfig()}, txHasMerge: true, txWriteInfos: []*cypher.QueryInfo{boltTxWriteAnalyzer.Analyze(t.Context(), query)}, txMergeStatements: []cypher.CommitStatement{{Query: query}}}
	require.True(t, session.canRetryMergeCommitConflict(t.Context(), failure))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := failure.Error()
	require.False(t, session.canRetryMergeCommitConflict(ctx, failure))
	require.Equal(t, before, failure.Error())
	require.True(t, session.canRetryMergeCommitConflict(t.Context(), failure), "canceled classification cannot poison later eligibility")
}
