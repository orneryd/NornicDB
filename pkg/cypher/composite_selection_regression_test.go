package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementCompositeSelection(t *testing.T) {
	for _, query := range []string{
		"USE pcomp.other MATCH (n:EI) RETURN n.v AS v",
		"USE graph.byName('pcomp.other') MATCH (n:EI) RETURN n.v AS v",
		":USE pcomp.other\nMATCH (n:EI) RETURN n.v AS v",
		"CALL { USE pcomp.other MATCH (n:EI) RETURN n.v AS v } RETURN v",
	} {
		t.Run(query, func(t *testing.T) {
			exec := newCompositeFixture(t)
			allowed := WithDatabasePermissionResolver(context.Background(), "pcomp", func(string, string) bool { return true })
			_, err := exec.Execute(allowed, query, nil)
			require.NoError(t, err)
			ctx := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, _ string) bool { return database != "pcomp" })
			result, err := exec.Execute(ctx, query, nil)
			requireDatabaseAccessDenied(t, err, "composite access was denied")
			require.Nil(t, result)
		})
	}
}
