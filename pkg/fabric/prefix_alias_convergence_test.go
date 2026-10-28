package fabric

import (
	"reflect"
	"testing"
)

func TestPrefixRetainsWrittenAliasIdentities(t *testing.T) {
	for _, tc := range []struct {
		query   string
		aliases []string
		prefix  string
	}{
		{"WITH 1 AS x, 3 AS `null`, 4 AS `NULL`, 5 AS `true`, 6 AS `nan`, 7 AS `NaN`, 8 AS `Infinity`", []string{"x", "`null`", "`NULL`", "`true`", "`nan`", "`NaN`", "`Infinity`"}, "x, `null`, `NULL`, `true`, `nan`, `NaN`, `Infinity`"},
		{"WITH `a``b`, ``, `日本`, x", []string{"`a``b`", "``", "`日本`", "x"}, "`a``b`, ``, `日本`, x"},
		{"WITH 1 AS `a``b`, 2 AS ``, 3 AS `日本`", []string{"`a``b`", "``", "`日本`"}, "`a``b`, ``, `日本`"},
		{"WITH x, n.x, x + 1, x AS 1invalid, 1 AS `x` tail", []string{"x"}, "x"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			if got := trailingWithAliases(tc.query, nil); !reflect.DeepEqual(got, tc.aliases) {
				t.Fatalf("aliases=%q want%q", got, tc.aliases)
			}
			if got := ensureRowProducingPrefix(tc.query, nil); got != tc.query+" RETURN "+tc.prefix {
				t.Fatalf("prefix=%q", got)
			}
		})
	}
}
