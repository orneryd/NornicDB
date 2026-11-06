package cypher

import (
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDeletedEntityReadsPreserveQuotedIdentityAndNullTests(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		_, err := exec.Execute(t.Context(), "CREATE (:QuotedDeleted {p:1})", nil)
		require.NoError(t, err)
		for _, sample := range []struct {
			expression string
			notFound   bool
		}{
			{"labels(n)", true},
			{"`labels`(n)", true},
			{"labels(`n`)", true},
			{"`n`.p", true},
			{"n.p IS /* gap */ NULL", false},
			{"n.`p` IS NULL", false},
			{"n. /* gap */ p", true},
			{"n. /* gap */ `p` IS /* gap */ NULL", false},
		} {
			t.Run(sample.expression, func(t *testing.T) {
				_, err := exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, err := exec.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
				result, err := exec.Execute(t.Context(), "MATCH (n:QuotedDeleted) DELETE n RETURN "+sample.expression+" AS v", nil)
				if sample.notFound {
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)
				} else {
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{true}}, result.Rows)
				}
			})
		}
	})
}

func TestDeletedEntityIterationReadsCrossTrivia(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		_, err := exec.Execute(t.Context(), "CREATE (:IterationDeleted {p:1})", nil)
		require.NoError(t, err)
		for _, projection := range []string{
			"[x IN ns | x.p]",
			"[x IN /* gap */ ns | x.p]",
			"[x /* gap */ IN ns | x.p]",
			"[x IN `ns` | x.p]",
			"[x IN /* gap */ `ns` | x.p]",
		} {
			t.Run(projection, func(t *testing.T) {
				_, err := exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, err := exec.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
				_, err = exec.Execute(t.Context(), "MATCH (n:IterationDeleted) DELETE n WITH collect(n) AS ns RETURN "+projection+" AS v", nil)
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)
			})
		}
	})
}

func TestDeletedReadsDoNotBindParametersOrFunctionNamespaces(t *testing.T) {
	withPluginFunctions(t, map[string]interface{}{"deleted.echo": func(value int64) int64 { return value }})
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		_, err := exec.Execute(t.Context(), "CREATE (:NamespaceDeleted {p:1})", nil)
		require.NoError(t, err)
		for _, sample := range []struct {
			tail   string
			params map[string]interface{}
		}{
			{"DELETE n RETURN $n.p AS v", map[string]interface{}{"n": map[string]interface{}{"p": int64(7)}}},
			{"WITH n AS deleted DELETE deleted RETURN deleted.echo(7) AS v", nil},
			{"WITH n AS deleted DELETE deleted RETURN deleted /* ns */ . `echo`(7) AS v", nil},
		} {
			t.Run(sample.tail, func(t *testing.T) {
				_, err := exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, err := exec.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
				result, err := exec.Execute(t.Context(), "MATCH (n:NamespaceDeleted) "+sample.tail, sample.params)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
			})
		}
		t.Run("namespace arguments still read deleted entities", func(t *testing.T) {
			_, err := exec.Execute(t.Context(), "BEGIN", nil)
			require.NoError(t, err)
			defer func() { _, err := exec.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
			_, err = exec.Execute(t.Context(), "MATCH (n:NamespaceDeleted) WITH n AS deleted DELETE deleted RETURN deleted.echo(size(labels(deleted))) AS v", nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)
		})

	})
}

func TestMalformedDeletedReadRejectsBeforeMutation(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		_, err := exec.Execute(t.Context(), "CREATE (:MalformedDeleted {p:1})", nil)
		require.NoError(t, err)
		for _, suffix := range []string{"`n.p", "'n.p", "\"n.p"} {
			_, err := exec.Execute(t.Context(), "MATCH (n:MalformedDeleted) DELETE n RETURN "+suffix, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			result, err := exec.Execute(t.Context(), "MATCH (n:MalformedDeleted) RETURN count(n) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		}
	})
}

func TestCallableSuffixExtentDoesNotDecodeIdentity(t *testing.T) {
	for _, text := range []string{".echo", " /* gap */ . `echo`", ".`a``b` /* gap */ . café"} {
		suffix, end := dottedNameParts(text, 0, true, dottedNameExtent)
		require.Equal(t, text, suffix)
		require.Equal(t, len(text), end)
		allocations := testing.AllocsPerRun(100, func() {
			_, end = dottedNameParts(text, 0, true, dottedNameExtent)
		})
		require.Equal(t, float64(0), allocations, text)
		require.Equal(t, len(text), end)
	}
}
