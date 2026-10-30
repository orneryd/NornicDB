package bolt

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"hash"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestApocUtilHashByteArrayBolt(t *testing.T) {
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "digest_bytes")
	defer store.Close()
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "digest_bytes"})
			defer session.Close(ctx)
			run := session.Run
			if explicit {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer tx.Close(ctx)
				run = func(ctx context.Context, query string, params map[string]interface{}, _ ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
					return tx.Run(ctx, query, params)
				}
			}
			for _, tc := range []struct {
				name, expression, text string
				data                   []byte
			}{
				{"binary", "[$data]", "[0, 1, 127, -128, -1]", []byte{0, 1, 127, 128, 255}},
				{"empty", "[$data]", "[]", []byte{}},
				{"nested-list", "[[$data]]", "[[0, 1, 127, -128, -1]]", []byte{0, 1, 127, 128, 255}},
				{"nested-map", "[{data:$data}]", "{data=[0, 1, 127, -128, -1]}", []byte{0, 1, 127, 128, 255}},
			} {
				for _, algorithm := range []struct {
					name string
					new  func() hash.Hash
				}{{"md5", md5.New}, {"sha1", sha1.New}, {"sha256", sha256.New}} {
					t.Run(tc.name+"/"+algorithm.name, func(t *testing.T) {
						digest := algorithm.new()
						_, err := digest.Write([]byte(tc.text))
						require.NoError(t, err)
						want := hex.EncodeToString(digest.Sum(nil))
						query := "RETURN apoc.util." + algorithm.name + "(" + tc.expression + "), apoc.util." + algorithm.name + "([$text]), $data"
						result, err := run(ctx, query, map[string]interface{}{"data": tc.data, "text": tc.text})
						require.NoError(t, err)
						record, err := result.Single(ctx)
						require.NoError(t, err)
						require.Equal(t, []interface{}{want, want, tc.data}, record.Values)
					})
				}
			}
		})
	}
}
