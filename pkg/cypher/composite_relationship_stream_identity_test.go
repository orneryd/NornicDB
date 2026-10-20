package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeRelationshipStreamIdentity(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit"
		}
		t.Run(mode, func(t *testing.T) {
			executor, _ := newCompositeIdentityExecutor(t, explicit)
			result, err := executor.Execute(context.Background(), `
                USE comp
                CALL { USE comp.one MATCH ()-[r:R]->() RETURN r }
                CALL { USE comp.two MATCH ()-[s:R]->() RETURN s }
                RETURN r = s, elementId(r), elementId(s),
                    elementId(startNode(r)), elementId(endNode(s))`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{false, "5:one:same-rel", "5:two:same-rel", "4:one:same", "4:two:target"}}, result.Rows)
		})
	}
}
