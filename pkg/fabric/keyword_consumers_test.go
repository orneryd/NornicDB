package fabric

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopLevelKeywordConsumerContracts(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		want        []string
	}{
		{"empty", "", []string{""}},
		{"terms", " a=1 AND b=2 and c=3 ", []string{"a=1", "b=2", "c=3"}},
		{"empty terms", "AND AND", []string{"", "", ""}},
		{"quoted", "a='AND' AND b=\"AND\" AND `AND`=1", []string{"a='AND'", "b=\"AND\"", "`AND`=1"}},
		{"nesting", "a=(x AND y) AND b=[x AND y] AND c={x:'AND'}", []string{"a=(x AND y)", "b=[x AND y]", "c={x:'AND'}"}},
		{"comments", "a=1 /* AND hidden */ AND b=2 // AND hidden\n AND c=3", []string{"a=1 /* AND hidden */", "b=2 // AND hidden", "c=3"}},
		{"name boundaries", "$AND=1 AND ANDerson=2 AND xAND=3", []string{"$AND=1", "ANDerson=2", "xAND=3"}},
	} {
		t.Run("AND/"+tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, splitTopLevelAnd(tc.input))
		})
	}
	for _, tc := range []struct {
		name, input, prefix, suffix string
		found                       bool
	}{
		{"empty", "", "", "", false},
		{"leading", "  USE db RETURN 1", "", "", false},
		{"later", "USE first WITH 1 AS x USE second RETURN x", "USE first WITH 1 AS x ", "USE second RETURN x", true},
		{"nested", "CALL { USE db RETURN 1 } RETURN 1", "", "", false},
		{"quoted", "RETURN 'USE db', `USE`", "", "", false},
		{"comment", "WITH 1 /* USE hidden */ USE db RETURN 1", "WITH 1 /* USE hidden */ ", "USE db RETURN 1", true},
		{"parameter", "RETURN $USE", "", "", false},
		{"name boundary", "WITH 1 AS xUSE RETURN xUSE", "", "", false},
	} {
		t.Run("USE/"+tc.name, func(t *testing.T) {
			prefix, suffix, found := splitAtTopLevelUse(tc.input, nil)
			require.Equal(t, tc.found, found)
			require.Equal(t, tc.prefix, prefix)
			require.Equal(t, tc.suffix, suffix)
		})
	}
}

func BenchmarkTopLevelKeywordConsumers(b *testing.B) {
	b.Run("AND", func(b *testing.B) {
		query := "n.id = seed AND n.text = 'quoted AND value' AND n.active = true"
		b.ReportAllocs()
		for b.Loop() {
			_ = splitTopLevelAnd(query)
		}
	})
	b.Run("USE", func(b *testing.B) {
		query := "WITH 'USE hidden' AS value USE cmp.a RETURN value"
		b.ReportAllocs()
		for b.Loop() {
			_, _, _ = splitAtTopLevelUse(query, nil)
		}
	})
}
