package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestKeywordPositionsWithNonASCIIText: text whose upper- or lower-case form
// has a different length ('ı' upper-cases to one byte, 'İ' lower-cases to
// three) before a keyword doesn't shift where the statement is cut. These
// statements panicked, lost a variable or returned the wrong column (#748);
// the results are Neo4j 5.26.30's.
func TestKeywordPositionsWithNonASCIIText(t *testing.T) {
	cases := []struct {
		statement string
		columns   []string
		rows      [][]interface{}
	}{
		{"UNWIND ['ıı'] AS x RETURN x", []string{"x"}, [][]interface{}{{"ıı"}}},
		{"RETURN 'ıı' AS a, 1 AS b", []string{"a", "b"}, [][]interface{}{{"ıı", int64(1)}}},
		{"RETURN 'ıı' AS a SKIP 0 LIMIT 1", []string{"a"}, [][]interface{}{{"ıı"}}},
		{"WITH 'ıı' AS a CALL { WITH a RETURN a AS b } RETURN b", []string{"b"}, [][]interface{}{{"ıı"}}},
		{"RETURN CASE WHEN 'ıı' = 'ıı' THEN 1 ELSE 0 END AS c", []string{"c"}, [][]interface{}{{int64(1)}}},
		{"UNWIND ['ıı'] AS x WITH x WHERE x <> '' RETURN collect(x) AS xs", []string{"xs"}, [][]interface{}{{[]interface{}{"ıı"}}}},
		{"RETURN 'İİ' AS a, 1 AS b", []string{"a", "b"}, [][]interface{}{{"İİ", int64(1)}}},
		{"WITH 'İİ' AS a CALL { WITH a RETURN a AS b } RETURN b", []string{"b"}, [][]interface{}{{"İİ"}}},
		// toUpper() on data keeps Unicode case mapping ('ı' is 'I').
		{"RETURN toUpper('ıi') AS u", []string{"u"}, [][]interface{}{{"II"}}},
	}
	for _, c := range cases {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "k748"))
		result, err := exec.Execute(context.Background(), c.statement, nil)
		require.NoError(t, err, c.statement)
		require.Equal(t, c.columns, result.Columns, c.statement)
		require.Equal(t, c.rows, result.Rows, c.statement)
	}

	// Stored text: a write and a read around it.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "k748w"))
	_, err := exec.Execute(context.Background(), "CREATE (:P {name: 'ıı'}), (:P {name: 'İstanbul'})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(context.Background(), "MATCH (p:P) WHERE p.name <> 'ıı' RETURN p.name AS name ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"İstanbul"}}, result.Rows)
}

// TestCaseHelpersKeepPositions: upperASCII and lowerASCII change ASCII
// letters only, so every byte keeps its offset.
func TestCaseHelpersKeepPositions(t *testing.T) {
	for _, text := range []string{"", "RETURN", "return 'ıı' as a", "MATCH (n:İ) RETURN n", "straße ǅ ß"} {
		require.Len(t, upperASCII(text), len(text), text)
		require.Len(t, lowerASCII(text), len(text), text)
	}
	require.Equal(t, "RETURN 'ıı' AS A", upperASCII("return 'ıı' as a"))
	require.Equal(t, "match (n:İ) return n", lowerASCII("MATCH (n:İ) RETURN n"))
	same := "ALREADY UPPER"
	require.Equal(t, same, upperASCII(same))
}

func BenchmarkCaseHelpers(b *testing.B) {
	text := "MATCH (n:Person {name: $name}) WHERE n.age > 30 RETURN n.name AS name ORDER BY name LIMIT 10"
	b.Run("upperASCII", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = upperASCII(text)
		}
	})
	b.Run("lowerASCII", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = lowerASCII(text)
		}
	})
	b.Run("strings.ToUpper", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = strings.ToUpper(text)
		}
	})
}
