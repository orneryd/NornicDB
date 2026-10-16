package differential

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type statementSequenceRecorder struct {
	database string
	queries  []string
}

func (s *statementSequenceRecorder) Execute(context.Context, string) Outcome {
	return Outcome{Code: "unexpected individual statement"}
}
func (s *statementSequenceRecorder) Reset(context.Context) error { return nil }
func (s *statementSequenceRecorder) ExecuteStatements(_ context.Context, database string, queries []string) Outcome {
	s.database = database
	s.queries = append([]string(nil), queries...)
	return Outcome{Code: "Neo.ClientError.Transaction.ForbiddenDueToTransactionType"}
}

func TestSweepStatementSequencesKeepOneRequest(t *testing.T) {
	queries := []string{"SHOW USERS", "CALL dbms.components()"}
	reference, actual := &statementSequenceRecorder{}, &statementSequenceRecorder{}
	results, err := RunSweep(context.Background(), SweepCorpus{Cases: []SweepCase{
		{ID: "sequence", Query: "SHOW USERS; CALL dbms.components()", Database: "system", Statements: queries},
	}}, &Pair{Neo4j: reference, NornicDB: actual}, DefaultOptions)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, results[0].Match)
	for _, route := range []*statementSequenceRecorder{reference, actual} {
		require.Equal(t, "system", route.database)
		require.Equal(t, queries, route.queries)
	}
}
