package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestKeywordSplitPublicPolicies(t *testing.T) {
	for _, tc := range []struct {
		name, source, keyword string
		create                bool
		want                  []string
	}{
		{"generic empty source", "", "MATCH", false, []string{""}},
		{"generic empty keyword", "CREATE (a)", "", false, []string{"CREATE (a)"}},
		{"create empty source", "", "", true, []string{""}},
		{"generic repeated CREATE", "CREATE (a) CREATE (b)", "CREATE", false, []string{"", "(a) ", "(b)"}},
		{"create repeated CREATE", "CREATE (a) CREATE (b)", "", true, []string{"", "(a) ", "(b)"}},
		{"generic continues past punctuation", "CREATE(n) CREATE (m)", "CREATE", false, []string{"CREATE(n) ", "(m)"}},
		{"create stops at punctuation", "CREATE(n) CREATE (m)", "", true, []string{"CREATE(n) CREATE (m)"}},
		{"generic continues past comment", "CREATE/*gap*/(n) CREATE (m)", "CREATE", false, []string{"CREATE/*gap*/(n) ", "(m)"}},
		{"create stops at comment", "CREATE/*gap*/(n) CREATE (m)", "", true, []string{"CREATE/*gap*/(n) CREATE (m)"}},
		{"generic terminal keyword", "CREATE", "CREATE", false, []string{"CREATE"}},
		{"create terminal keyword", "CREATE", "", true, []string{"CREATE"}},
		{"generic braces remain visible", "{CREATE (a)} CREATE (b)", "CREATE", false, []string{"{", "(a)} ", "(b)"}},
		{"create current default braces visible", "{CREATE (a)} CREATE (b)", "", true, []string{"{", "(a)} ", "(b)"}},
		{"generic preserves quoted prefix", "RETURN 'CREATE x' CREATE (n)", "CREATE", false, []string{"RETURN 'CREATE x' ", "(n)"}},
		{"generic elastic multiword keyword", "ORDER\tBY x ORDER  BY y", "ORDER BY", false, []string{"", "x ", "y"}},
		{"create trims ASCII whitespace", "CREATE\t \r\n(a)", "", true, []string{"", "(a)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			if tc.create {
				got = SplitByCreate(tc.source)
			} else {
				got = SplitByKeyword(tc.source, tc.keyword)
			}
			require.Equal(t, tc.want, got)
		})
	}
}
