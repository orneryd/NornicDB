// Package storage provides receipt generation for mutation auditing.
package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Receipt represents a mutation receipt tied to WAL sequencing.
// WALSeqEnd should refer to the commit marker sequence for the transaction.
type Receipt struct {
	TxID        string    `json:"tx_id"`
	WALSeqStart uint64    `json:"wal_seq_start"`
	WALSeqEnd   uint64    `json:"wal_seq_end"`
	Timestamp   time.Time `json:"timestamp"`
	Database    string    `json:"database,omitempty"`
	Hash        string    `json:"hash"`
}

// NewReceipt creates a receipt and computes its hash.
func NewReceipt(txID string, walSeqStart, walSeqEnd uint64, database string, timestamp time.Time) (*Receipt, error) {
	if txID == "" {
		return nil, localizedError(localization.StorageClientReceiptTransactionIDRequired(), nil)
	}
	if walSeqStart == 0 || walSeqEnd == 0 {
		return nil, localizedError(localization.StorageClientReceiptWALSequenceRequired(), nil)
	}
	if walSeqEnd < walSeqStart {
		return nil, localizedError(localization.StorageClientReceiptWALRangeInvalid(walSeqEnd, walSeqStart), nil)
	}
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	r := &Receipt{
		TxID:        txID,
		WALSeqStart: walSeqStart,
		WALSeqEnd:   walSeqEnd,
		Timestamp:   timestamp.UTC(),
		Database:    database,
	}
	if err := r.UpdateHash(); err != nil {
		return nil, err
	}
	return r, nil
}

// UpdateHash recomputes the receipt hash from canonical fields.
func (r *Receipt) UpdateHash() error {
	if r == nil {
		return localizedError(localization.StorageClientReceiptNilReceiver(), nil)
	}
	hash, err := computeReceiptHash(r)
	if err != nil {
		return err
	}
	r.Hash = hash
	return nil
}

type receiptHashPayload struct {
	TxID        string `json:"tx_id"`
	WALSeqStart uint64 `json:"wal_seq_start"`
	WALSeqEnd   uint64 `json:"wal_seq_end"`
	Timestamp   string `json:"timestamp"`
	Database    string `json:"database,omitempty"`
}

func computeReceiptHash(r *Receipt) (string, error) {
	payload := receiptHashPayload{
		TxID:        r.TxID,
		WALSeqStart: r.WALSeqStart,
		WALSeqEnd:   r.WALSeqEnd,
		Timestamp:   r.Timestamp.UTC().Format(time.RFC3339Nano),
		Database:    r.Database,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", localizedError(localization.StorageClientReceiptHashMarshalFailed(err), err)
	}

	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
