package antlr

import (
	"context"
	"os"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// lookaheadStream counts the tokens each prediction reads ahead of the parse.
type lookaheadStream struct {
	antlr4.TokenStream
	depth, reading int
	total, longest int
}

func (s *lookaheadStream) Mark() int {
	if s.depth == 0 {
		s.reading = 0
	}
	s.depth++
	return s.TokenStream.Mark()
}

func (s *lookaheadStream) Release(marker int) {
	s.depth--
	if s.depth == 0 {
		s.total += s.reading
		s.longest = max(s.longest, s.reading)
	}
	s.TokenStream.Release(marker)
}

func (s *lookaheadStream) Consume() {
	if s.depth > 0 {
		s.reading++
	}
	s.TokenStream.Consume()
}

// predictStatement parses source as the statement reader's first attempt
// does: fast prediction, CASE results in full prediction, on prediction state
// no other parse has filled. It returns the syntax errors of that attempt and
// what its predictions read ahead.
func predictStatement(t testing.TB, source string) (errors, tokens, total, longest int) {
	t.Helper()
	projection := cyphertext.NewSource(context.Background(), source, cyphertext.AcceptedSource)
	require.NoError(t, projection.Err)
	_, input := sourceInputStream(context.Background(), projection)
	lexer := NewCypherLexer(input)
	listener := &parseErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)
	stream := &lookaheadStream{TokenStream: antlr4.NewCommonTokenStream(lexer, antlr4.TokenDefaultChannel)}
	parser := NewCypherParser(stream)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)
	usePrediction(parser, newParserPrediction())
	parser.Interpreter.SetPredictionMode(antlr4.PredictionModeSLL)
	parser.AddParseListener(&caseResultPrediction{parser: parser, mode: antlr4.PredictionModeSLL})
	parser.Script()
	return len(listener.errors), stream.Size(), stream.total, stream.longest
}

// The fast prediction mode reads these statements without a syntax error, so
// the statement reader never parses them a second time in the full mode
// (NOR-407: EXISTS was read as a variable before EXISTS { ... }).
func TestFastPredictionReadsStatementsInOneAttempt(t *testing.T) {
	large, err := os.ReadFile("testdata/prediction_large_statement.cypher")
	require.NoError(t, err)
	for name, source := range map[string]string{
		"exists subquery":         "MATCH (d:Document) WHERE NOT EXISTS { MATCH (d)-[:MEMBER_OF]->(c:Collection) WHERE NOT coalesce(c.archived, false) } RETURN d.id",
		"exists in a conjunction": "MATCH (d) WHERE coalesce(d.current, true) AND (NOT EXISTS { MATCH (d)-[:MEMBER_OF]->(:Collection) }) RETURN d",
		"exists in an exists": `MATCH (h:Hit)
WHERE h.kind = "collection" OR (h.kind = "document" AND EXISTS {
  MATCH (d:Document {id: h.owner})
  MATCH (v:Version {id: d.current, document_id: d.id})
  WHERE v.source IN h.sources
    AND (NOT EXISTS { MATCH (d)-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (d)-[:MEMBER_OF]->(c:Collection) WHERE NOT coalesce(c.archived, false) }) })
RETURN h AS hit, null AS score`,
		"exists in a call union": `CALL () {
  MATCH (h:Hit)
  WHERE [x IN h.owners WHERE x.kind = "document" AND EXISTS {
      MATCH (d:Document {id: x.id})
      WHERE (NOT EXISTS { MATCH (d)-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (d)-[:MEMBER_OF]->(c:Collection) WHERE NOT coalesce(c.archived, false) }) }]
  RETURN h AS hit, null AS score
  UNION ALL
  CALL db.labels() YIELD label
  RETURN label AS hit, 1.0 AS score
}
RETURN hit, score`,
		"count subquery":  "MATCH (d) RETURN COUNT { MATCH (d)-[:MEMBER_OF]->(c) WHERE c.open } AS open",
		"union in a call": "CALL { MATCH (a:A) RETURN a.id AS id UNION ALL MATCH (b:B) RETURN b.id AS id } RETURN id",
		"dotted function": "RETURN apoc.coll.toSet([1, 1, 2]) AS values, apoc.text.join(['a', 'b'], '-') AS text",
		"large statement": string(large),
	} {
		t.Run(name, func(t *testing.T) {
			errors, _, _, _ := predictStatement(t, source)
			require.Zero(t, errors)
		})
	}
}

// What a prediction reads ahead belongs to the clause or expression it is in,
// not to the statement: a statement twice as long reads no further ahead for
// any single decision, and about twice as much in all (NOR-407: a dotted
// function name was read to the statement's end, a query to its next WITH).
func TestPredictionLookaheadDoesNotGrowWithTheStatement(t *testing.T) {
	large, err := os.ReadFile("testdata/prediction_large_statement.cypher")
	require.NoError(t, err)
	once := string(large)
	twice := once + "\nUNION ALL\n" + once
	errors, tokens, total, longest := predictStatement(t, once)
	require.Zero(t, errors)
	errorsTwice, tokensTwice, totalTwice, longestTwice := predictStatement(t, twice)
	require.Zero(t, errorsTwice)
	require.Greater(t, tokensTwice, 2*tokens-2)
	require.LessOrEqual(t, longestTwice, longest, "one decision reads further ahead in the longer statement")
	require.LessOrEqual(t, totalTwice, 2*total+tokens/100, "predictions read more than twice as far ahead in a statement twice as long")
	require.Less(t, total, 2*tokens, "predictions read ahead more than twice the statement")
	t.Logf("%d tokens: %d read ahead in all, %d at most for one decision", tokens, total, longest)
}
