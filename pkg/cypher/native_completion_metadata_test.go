package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNativeCompletionMetadataBufferedParity(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, buffered := range []bool{false, true} {
		for _, dynamic := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%t/dynamic=%t", buffered, dynamic), func(t *testing.T) {
				base, _ := openHiddenRelationshipStorage(t, buffered)
				wal, err := storage.NewWAL(t.TempDir(), nil)
				require.NoError(t, err)
				backed := storage.NewWALEngine(base, wal)
				t.Cleanup(func() { require.NoError(t, backed.Close()) })
				managed := &recordingManagedCommitEngine{NamespacedEngine: storage.NewNamespacedEngine(backed, "managed")}
				executor := NewStorageExecutor(managed, QueryCachePolicy{MaxEntries: 0, TTL: time.Minute})
				body := "CREATE (n:AckMetadata {kind:'expected'}) RETURN n.kind AS v"
				query := body
				if dynamic {
					query = "CALL apoc.cypher.run($body,{}) YIELD value RETURN value.v AS v"
				}
				result, err := executor.Execute(t.Context(), query, map[string]interface{}{"body": body})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"expected"}}, result.Rows)
				require.NotNil(t, managed.last)
				require.Equal(t, buffered, managed.last.AcknowledgedBuffered(), "real native ACK mode")
				eventual, _ := result.Metadata["eventual"].(bool)
				require.Equal(t, buffered, eventual, "same physical ACK must expose the same HTTP consistency metadata")
				receipt, ok := result.Metadata["receipt"].(*storage.Receipt)
				require.True(t, ok)
				require.NotEmpty(t, receipt.TxID)
				require.Greater(t, receipt.WALSeqEnd, receipt.WALSeqStart)
				nodes, err := managed.GetNodesByLabel(t.Context(), "AckMetadata")
				require.NoError(t, err)
				require.Len(t, nodes, 1, "ACKed graph is already readable before explicit drain")
				require.NoError(t, base.FlushWriteBehind(t.Context()))
				nodes, err = managed.GetNodesByLabel(t.Context(), "AckMetadata")
				require.NoError(t, err)
				require.Len(t, nodes, 1, "replay preserves one actual entity")
				read, err := executor.Execute(t.Context(), "MATCH (n:AckMetadata) RETURN count(n) AS v", nil)
				require.NoError(t, err)
				require.NotEqual(t, true, read.Metadata["eventual"])
				noop, err := executor.Execute(t.Context(), "CALL apoc.cypher.run('MATCH (n:AckAbsent) SET n.value=1 RETURN count(n) AS v',{}) YIELD value RETURN value.v AS v", nil)
				require.NoError(t, err)
				require.NotEqual(t, true, noop.Metadata["eventual"])
			})
		}
	}
}
