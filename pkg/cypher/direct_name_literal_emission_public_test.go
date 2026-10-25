package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestDirectNamePropertyExistsIdentity(t *testing.T) {
	exec := setupTestExecutor(t)
	for _, key := range []string{"key", " ", "\t", "a`b", "a'b", "a\\b", "a\nb"} {
		t.Run(key, func(t *testing.T) {
			name := cyphertext.QuoteName(key)
			query := "CREATE (n:DirectPropertyExists {" + name + ":1}) RETURN PROPERTY_EXISTS(n," + name + ") AS present"
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{true}}, result.Rows)
		})
	}
}
