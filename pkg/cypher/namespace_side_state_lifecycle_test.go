package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropPublicSideStateRecreate(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			dir := t.TempDir()
			f := openTypedCountScope(t, dir, buffered)
			for _, ns := range []string{"public_side_a", "public_side_b"} {
				typedCountRun(t, f.admin, "CREATE DATABASE "+ns)
				for _, id := range []string{"seed", "end"} {
					_, err := f.base.CreateNode(&storage.Node{ID: storage.NodeID(ns + ":" + id), Labels: []string{"Side"}})
					require.NoError(t, err)
				}
				require.NoError(t, f.base.CreateEdge(&storage.Edge{ID: storage.EdgeID(ns + ":edge"), StartNode: storage.NodeID(ns + ":seed"), EndNode: storage.NodeID(ns + ":end"), Type: "SIDE"}))
			}
			require.NoError(t, f.base.FlushWriteBehind())
			for _, ns := range []string{"public_side_a", "public_side_b"} {
				require.NoError(t, f.base.SetNodeManuallySuppressed(storage.NodeID(ns+":seed"), true))
				require.NoError(t, f.base.SetEdgeManuallySuppressed(storage.EdgeID(ns+":edge"), true))
				require.NoError(t, f.base.PutAccessMeta(ns+":seed", &knowledgepolicy.AccessMetaEntry{TargetID: ns + ":seed", Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 42}}))
			}
			typedCountRun(t, f.admin, "DROP DATABASE public_side_a")
			typedCountRun(t, f.admin, "CREATE DATABASE public_side_a")
			for _, id := range []string{"seed", "end"} {
				_, err := f.base.CreateNode(&storage.Node{ID: storage.NodeID("public_side_a:" + id), Labels: []string{"Side"}})
				require.NoError(t, err)
			}
			require.NoError(t, f.base.CreateEdge(&storage.Edge{ID: "public_side_a:edge", StartNode: "public_side_a:seed", EndNode: "public_side_a:end", Type: "SIDE"}))
			require.NoError(t, f.base.FlushWriteBehind())
			for restart := 0; restart < 2; restart++ {
				typedCountWant(t, f.executor(t, "public_side_a"), "MATCH (n:Side) RETURN count(n) AS n", 2)
				typedCountWant(t, f.executor(t, "public_side_a"), "MATCH (s:Side) WITH s MATCH (s)-[r:SIDE]->() RETURN count(r) AS n", 1)
				typedCountWant(t, f.executor(t, "public_side_b"), "MATCH (n:Side) RETURN count(n) AS n", 1)
				meta, err := f.base.GetAccessMeta("public_side_a:seed")
				require.NoError(t, err)
				require.Nil(t, meta)
				if restart == 0 {
					f.close(t)
					f = openTypedCountScope(t, dir, buffered)
				}
			}
		})
	}
}
