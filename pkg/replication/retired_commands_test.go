package replication

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRetiredCommandSlotsRejectBeforeJournal(t *testing.T) {
	require.Equal(t, CommandType(7), cmdRetiredSetProperty)
	require.Equal(t, CommandType(8), cmdRetiredBatchWrite)
	require.Equal(t, CommandType(9), cmdRetiredCypher)
	require.Equal(t, CommandType(19), CmdCommitTransaction)
	adapter := setupTestAdapter(t).adapter
	defer adapter.Close()
	before := adapter.wal.Sequence()
	for _, slot := range []CommandType{7, 8, 9} {
		require.ErrorIs(t, adapter.ApplyCommand(&Command{Type: slot, Data: []byte("historical payload")}), ErrRetiredCommand)
		require.Equal(t, before, adapter.wal.Sequence(), "retired records must not enter the journal")
	}
}
