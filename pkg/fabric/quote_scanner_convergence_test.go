package fabric

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func fabricQuoteMatrix() []struct{ name, text string } {
	atoms := []string{",", ":", "{", "}", "[", "]", "(", ")", "'", "\"", "`", "a`b,c", "雪λ", " \t\n", "/*,:{}[]()*/", "//,:{}[]()\n"}
	value := "v,:{}[]()'\"`a``b 雪λ\t\n/*data*/ //data"
	single := "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'", "\n", "\\n", "\t", "\\t").Replace(value) + "'"
	double := "\"" + strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n", "\t", "\\t").Replace(value) + "\""
	values := []string{single, double, "(" + single + ")", "[" + single + ",'other,:{}[]()'][0]", "{`inner,``key`:" + single + "}['inner,`key']"}
	var cases []struct{ name, text string }
	for atomIndex, atom := range atoms {
		key := "k" + atom + "z"
		quoted := "`" + strings.ReplaceAll(key, "`", "``") + "`"
		for valueIndex, expression := range values {
			for layout, text := range []string{
				"{" + quoted + ":" + expression + ",keep:7,tail:2}",
				"{ \n\t" + quoted + " \t: " + expression + " , keep : 7 , tail : 2 \n}",
				"{/* ,:{}[]()'\"` */ " + quoted + " /* : , */ : // ,:{}[]()'\"`\n " + expression + " /* , : */ , /* { */ keep:7, tail:2 /* } */}",
			} {
				cases = append(cases, struct{ name, text string }{fmt.Sprintf("key%d/value%d/layout%d", atomIndex, valueIndex, layout), text})
			}
		}
	}
	return cases
}

func TestFabricStructuralReadersUseCanonicalQuotation(t *testing.T) {
	for _, tc := range fabricQuoteMatrix() {
		t.Run(tc.name, func(t *testing.T) {
			mapText := tc.text
			parts := splitTopLevelCSV("a, " + mapText + ", tail")
			for index := range parts {
				parts[index] = strings.TrimSpace(parts[index])
			}
			require.Equal(t, []string{"a", mapText, "tail"}, parts)

			parts, operators, union, err := splitTopLevelUnion("RETURN " + mapText + " AS x UNION ALL RETURN {plain: 2} AS x")
			require.NoError(t, err)
			require.True(t, union)
			require.Len(t, parts, 2)
			require.Equal(t, []bool{false}, operators)

			require.Equal(t, -1, indexTopLevelKeyword(mapText, "AND"))
			require.Len(t, splitTopLevelAnd("n.x = "+mapText+" AND n.y = 1"), 2)

			fragment, planErr := NewFabricPlanner(NewCatalog()).Plan("RETURN "+mapText+" AS x", "neo4j")
			require.NoError(t, planErr)
			require.NotNil(t, fragment)
		})
	}
}
