package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type versionLookupCounter struct {
	context.Context
	reads atomic.Int64
}

func (c *versionLookupCounter) Value(key any) any {
	if _, version := key.(cypherVersionKey); version {
		c.reads.Add(1)
	}
	return c.Context.Value(key)
}

func TestQueryParameterVersionSnapshot(t *testing.T) {
	for _, version := range []string{"", "5", "25"} {
		t.Run(version, func(t *testing.T) {
			parent := context.Background()
			if version != "" {
				parent = context.WithValue(parent, cypherVersionKey{}, version)
			}
			counted := &versionLookupCounter{Context: parent}
			ctx := withQueryParams(counted, map[string]interface{}{"v": int64(1)})
			expected := version
			if expected == "" {
				expected = "5"
			}
			for repeat := 0; repeat < 100; repeat++ {
				require.Equal(t, expected, cypherVersionFromContext(ctx))
				require.Equal(t, version == "25", rowIsCypher25(parameterRowValues(ctx)))
			}
			require.Equal(t, int64(1), counted.reads.Load())
			nested := withCypherVersion(ctx, "CYPHER 25 RETURN $v")
			if version == "" {
				expected = "25"
			}
			require.Equal(t, expected, cypherVersionFromContext(nested))
			require.Equal(t, int64(1), getParamsFromContext(nested)["v"])
		})
	}
}
