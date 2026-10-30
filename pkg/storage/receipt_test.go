package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReceiptHash_Deterministic(t *testing.T) {
	ts := time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC)

	r1, err := NewReceipt("tx-123", 10, 15, "nornic", ts)
	require.NoError(t, err)

	r2, err := NewReceipt("tx-123", 10, 15, "nornic", ts)
	require.NoError(t, err)

	assert.Equal(t, r1.Hash, r2.Hash)
}

func TestReceiptHash_ChangesOnFields(t *testing.T) {
	ts := time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC)

	base, err := NewReceipt("tx-123", 10, 15, "nornic", ts)
	require.NoError(t, err)

	changedSeq, err := NewReceipt("tx-123", 10, 16, "nornic", ts)
	require.NoError(t, err)
	assert.NotEqual(t, base.Hash, changedSeq.Hash)

	changedTx, err := NewReceipt("tx-124", 10, 15, "nornic", ts)
	require.NoError(t, err)
	assert.NotEqual(t, base.Hash, changedTx.Hash)
}

func TestReceiptValidationAndUpdateHash(t *testing.T) {
	_, err := NewReceipt("", 10, 15, "nornic", time.Time{})
	require.ErrorContains(t, err, "tx_id is required")

	_, err = NewReceipt("tx-123", 0, 15, "nornic", time.Time{})
	require.ErrorContains(t, err, "wal sequence must be non-zero")

	_, err = NewReceipt("tx-123", 20, 15, "nornic", time.Time{})
	require.ErrorContains(t, err, "wal_seq_end")

	r, err := NewReceipt("tx-123", 10, 15, "nornic", time.Time{})
	require.NoError(t, err)
	assert.False(t, r.Timestamp.IsZero())

	require.ErrorContains(t, (*Receipt)(nil).UpdateHash(), "nil receiver")

	originalHash := r.Hash
	r.Database = "other"
	require.NoError(t, r.UpdateHash())
	assert.NotEqual(t, originalHash, r.Hash)
}
