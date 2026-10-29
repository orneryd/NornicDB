package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDDLPropertyReferenceSpansAndModes(t *testing.T) {
	text := " /*gap*/ `v.x` . `a,b` , n.`space ` /*tail*/"
	refs, ok := readDDLPropertyReferences(text, false)
	require.True(t, ok)
	require.Len(t, refs, 2)
	require.Equal(t, "`v.x` . `a,b`", text[refs[0].Start:refs[0].End])
	require.Equal(t, "v.x", refs[0].Qualifier)
	require.Equal(t, "a,b", refs[0].Key)
	require.Equal(t, "n.`space `", text[refs[1].Start:refs[1].End])
	require.Equal(t, "space ", refs[1].Key)
	for _, invalid := range []string{"n.a,", ",n.a", "n.a,,n.b", "n.a.b", "n.a + 1", "n.`open", "n.a /*open", "n.a, bare"} {
		_, ok := readDDLPropertyReferences(invalid, false)
		require.False(t, ok, invalid)
	}
	_, ok = readDDLPropertyReferences("`a,b`, `space `", false)
	require.False(t, ok)
	refs, ok = readDDLPropertyReferences("`a,b`, `space `", true)
	require.True(t, ok)
	require.Equal(t, []string{"a,b", "space "}, ddlPropertyKeys(refs))
}

func TestNameDefinitionGoEmitterRoles(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "name_roles"))
			ctx := context.Background()
			q := cyphertext.QuoteName(name)
			_, err := e.Execute(ctx, "CREATE (:Role {"+q+":1})", nil)
			require.NoError(t, err)
			predicates := propertyMapPredicates("n", "{"+q+":1}")
			require.Len(t, predicates, 1)
			r, err := e.Execute(ctx, "MATCH (n:Role) WHERE "+predicates[0]+" RETURN n", nil)
			require.NoError(t, err)
			require.Len(t, r.Rows, 1)
			r, err = e.Execute(ctx, "SHOW INDEXES YIELD name AS "+q+" RETURN *", nil)
			require.NoError(t, err)
			require.Equal(t, []string{name}, r.Columns)
			// Quoting-removal control is independent of relationship brackets.
			if name == "two words" {
				_, err = e.Execute(ctx, "MATCH (n:Role) SET n."+name+" = 2 RETURN n", nil)
				require.Error(t, err)
			}
		})
	}
}

func TestNameDefinitionPolicyStringNames(t *testing.T) {
	for _, name := range append(cyphernames.Names(), "slash\\name", "quote'name", "ends\\") {
		text := quoteKnowledgePolicyName(name)
		value, end := kpScanQuotedString(text, 0)
		require.Equal(t, name, value)
		require.Equal(t, len(text), end)
		require.Contains(t, formatDecayBindingApply(knowledgepolicy.DecayProfileBinding{ProfileRef: name}), "DECAY PROFILE "+text)
		require.Contains(t, formatPromotionPolicyApply(knowledgepolicy.PromotionPolicyDef{WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{Predicate: "n.x > 0", ProfileRef: name}}}), "WHEN n.x > 0 APPLY PROFILE "+text)
		// Policy predicates and mutations are retained source, rather than
		// decoded symbolic names. The generated ledger dispositions say so.
	}
}

func TestNameEmissionValueNodeAndStarSubjects(t *testing.T) {
	for _, name := range []string{"two words", "space ", "tick`name", "A|B"} {
		t.Run(name, func(t *testing.T) {
			q := cyphertext.QuoteName(name)
			value, ok := cypher25ValueText(map[string]any{name: int64(7)})
			require.True(t, ok)
			token, valid := cyphertext.ScanIdentifier(value, 1)
			key := token.Value
			require.True(t, valid)
			require.Equal(t, name, key)
			expr, ok := parseLabelExpression(q + "|(Other&" + q + ")")
			require.True(t, ok)
			reparsed, ok := parseLabelExpression(expr.String())
			require.True(t, ok)
			require.Equal(t, expr.String(), reparsed.String())
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "node_star"))
			ctx := context.Background()
			mapResult, err := e.Execute(ctx, "RETURN "+value+" AS value", nil)
			require.NoError(t, err)
			require.EqualValues(t, 7, mapResult.Rows[0][0].(map[string]any)[name])
			items := starProjectionItems(ctx, []string{name}, []string{"1 AS extra"})
			star, err := e.Execute(ctx, "WITH 7 AS "+q+" RETURN "+strings.Join(items, ", "), nil)
			require.NoError(t, err)
			require.Equal(t, []string{name, "extra"}, star.Columns)
			require.EqualValues(t, 7, star.Rows[0][0])
			plan := returnProjectionPlanFor(ctx, "RETURN *, 1 AS extra").withStarExpanded([]string{name})
			require.Equal(t, []string{name, "extra"}, plan.columns)
			require.Equal(t, projectionVariableText(name), plan.projections[0].expr)
			for _, query := range []string{"CREATE (n:$($labels)) RETURN labels(n)", "WITH $labels AS labs CREATE (n:$(labs)) RETURN labels(n)"} {
				r, err := e.Execute(ctx, query, map[string]any{"labels": []any{name, "Other"}})
				require.NoError(t, err, query)
				require.Equal(t, [][]any{{[]any{name, "Other"}}}, r.Rows)
			}
			r, err := e.Execute(ctx, "MATCH (IS "+q+"|Other) RETURN count(*)", nil)
			require.NoError(t, err)
			require.EqualValues(t, 2, r.Rows[0][0])
			for _, query := range []string{"WITH 7 AS " + q + " RETURN *, 1 AS extra", "WITH 7 AS " + q + " WITH *, 1 AS extra RETURN " + q} {
				r, err := e.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Contains(t, r.Columns, name)
				for index, column := range r.Columns {
					if column == name {
						require.EqualValues(t, 7, r.Rows[0][index])
					}
				}
			}
			for _, suffix := range []string{"", " RETURN *", " WITH " + q + " RETURN *", " RETURN *, 1 AS extra"} {
				r, err := e.Execute(ctx, "SHOW INDEXES YIELD name AS "+q+suffix, nil)
				require.NoError(t, err)
				require.Contains(t, r.Columns, name)
			}
		})
	}
}
