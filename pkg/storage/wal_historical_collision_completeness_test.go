package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestHistoricalKeyRepairRejectsIncompleteCollisionClass(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	fixture := historicalAmbiguousRecords(t, true, historicalAmbiguousCollisionBatch(t))
	require.Len(t, fixture.commits, 1)
	commit := fixture.commits[0]
	for key := range commit.raw.Writes {
		if len(key) == 0 || key[0] != prefixIDDictNodeForward {
			continue
		}
		number, ok := decodeNodeRefNum(commit.raw.Writes[key])
		require.True(t, ok)
		original := string(append([]byte{prefixIDDictNodeReverse}, encodeNumID(number)...))
		damaged := historicalPhysicalKeyJSONForm(original)
		if damaged == original {
			continue
		}
		members := 0
		for candidate := range commit.raw.Writes {
			if historicalPhysicalKeyJSONForm(candidate) == damaged {
				members++
			}
		}
		if members < 2 {
			continue
		}
		incomplete := cloneBufferedCommit(commit.decoded)
		body := string(append([]byte{prefixNode}, key[1:]...))
		require.Contains(t, incomplete.Writes, body)
		delete(incomplete.Writes, body)
		_, err := repairV4BufferedCommitPhysicalKeys(incomplete)
		require.Error(t, err, "a surviving member must not certify a collision class whose other entity body is missing")
		return
	}
	t.Fatal("fixture must contain a damaged reverse-key collision")
}
