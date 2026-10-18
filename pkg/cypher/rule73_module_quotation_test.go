package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRule73ModuleQuotationPublicMatrix(t *testing.T) {
	for index, tc := range []struct{ literal, value string }{
		{`'O\'Reilly, Sons'`, "O'Reilly, Sons"},
		{`"Say \"yes, [now]\""`, `Say "yes, [now]"`},
		{`'a\\b, c'`, `a\b, c`},
		{`'a\n, b'`, "a\n, b"},
		{`'\u03bb, \U0001F600'`, "λ, 😀"},
		{"'`a``b`,:{}[]() // /* */'", "`a``b`,:{}[]() // /* */"},
	} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			ctx := context.Background()
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), fmt.Sprintf("r73module%d", index)))
			_, err := e.Execute(ctx, "CREATE CONSTRAINT quoted_node FOR (n:R73Module) REQUIRE { n.name IN ["+tc.literal+", 'Other'] }", nil)
			require.NoError(t, err)
			_, err = e.Execute(ctx, "CREATE (:R73Module {name:$name})", map[string]interface{}{"name": tc.value})
			require.NoError(t, err)
			_, err = e.Execute(ctx, "CREATE (:R73Module {name:'not allowed'})", nil)
			require.Error(t, err)
			got, err := e.Execute(ctx, "MATCH (n:R73Module) RETURN n.name", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.value}}, got.Rows)
			_, err = e.Execute(ctx, "CREATE CONSTRAINT quoted_rel FOR ()-[r:R73ModuleRel]-() REQUIRE { r.name IN ["+tc.literal+", 'Other'] }", nil)
			require.NoError(t, err)
			_, err = e.Execute(ctx, "CREATE ()-[:R73ModuleRel {name:$name}]->()", map[string]interface{}{"name": tc.value})
			require.NoError(t, err)
			_, err = e.Execute(ctx, "CREATE PROMOTION POLICY quoted_policy FOR (n:R73Module) APPLY { ON ACCESS { SET n.name = "+tc.literal+" } }", nil)
			require.NoError(t, err)
			policies := e.storage.GetSchema().ShowPromotionPolicies()
			require.Len(t, policies, 1)
			require.NotNil(t, policies[0].OnAccess)
			require.Equal(t, "n.name = "+tc.literal, policies[0].OnAccess.Mutations[0].Expression)
		})
	}
}
