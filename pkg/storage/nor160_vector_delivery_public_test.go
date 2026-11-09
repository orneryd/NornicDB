// NOR-160 supported-type/vector delivery contract. Ported from the approved
// original head refs/codex/reference/pr-1688-20261104 (e1c1aa6f7) to the
// context-first storage API of release b098f7e708b4c701a743ffb0310a9b179a84c8f2.
// Original author: Codex, NOR-160 bounded storage delivery, 1 November 2026.
//
// Query reads may omit separately stored vectors. When vectors are present in a
// pending or cached image they belong to the caller that receives them, which
// pkg/storage/types.go QueryNodeReader and the query-cache fill now document.
// Red/green evidence: on the unmodified release both routes failed
// (sig/readiness-nor160/nor160-current-release.txt: chunk vector 1 became 99
// after the caller's write, "caller changed a retained chunk vector").
package storage_test

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Query reads may omit separately stored vectors. When vectors are present in
// a pending or cached image, they are still mutable caller-owned results.
func TestNOR160StorageReadVectorIsolation(t *testing.T) {
	for _, route := range []string{"pending", "cached"} {
		t.Run(route, func(t *testing.T) {
			engine := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			ctx := t.Context()
			writer, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			require.NoError(t, writer.SetNamespace("test"))
			_, err = writer.CreateNode(ctx, &storage.Node{ID: "test:vector", Labels: []string{"Vector"}, Properties: map[string]any{"nested": map[string]any{"list": []int64{7}}}, ChunkEmbeddings: [][]float32{{1, 2}}, NamedEmbeddings: map[string][]float32{"named": {3, 4}}})
			require.NoError(t, err)
			read := func() (*storage.Node, error) { return writer.GetQueryNode(ctx, "test:vector") }
			if route == "cached" {
				require.NoError(t, writer.Commit())
				_, err = engine.GetNode(ctx, "test:vector") // warm the full native image
				require.NoError(t, err)
				read = func() (*storage.Node, error) { return engine.GetQueryNode(ctx, "test:vector") }
			} else {
				t.Cleanup(func() { require.NoError(t, writer.Rollback()) })
			}
			first, err := read()
			require.NoError(t, err)
			require.NotEmpty(t, first.ChunkEmbeddings)
			require.NotEmpty(t, first.NamedEmbeddings)
			first.ChunkEmbeddings[0][0] = 99
			first.NamedEmbeddings["named"][0] = 99
			first.Properties["nested"].(map[string]any)["list"].([]int64)[0] = 99
			again, err := read()
			require.NoError(t, err)
			require.Equal(t, float32(1), again.ChunkEmbeddings[0][0], "caller changed a retained chunk vector")
			require.Equal(t, float32(3), again.NamedEmbeddings["named"][0], "caller changed a retained named vector")
			require.Equal(t, int64(7), again.Properties["nested"].(map[string]any)["list"].([]int64)[0])
		})
	}
}
