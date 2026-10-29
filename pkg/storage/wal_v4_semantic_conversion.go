package storage

import "fmt"

// TranscodeV4EntityTransaction converts one historical gob transaction into
// the current semantic MessagePack representation at the V4 upgrade boundary.
func TranscodeV4EntityTransaction(payload []byte) ([]byte, error) {
	envelope, ops, err := decodeV4GobEntityTransaction(payload)
	if err != nil {
		return nil, fmt.Errorf("decode historical entity transaction: %w", err)
	}
	wire, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: envelope.Namespace, LogicalID: envelope.LogicalTxID, Operations: ops, Mode: envelope.Mode})
	if err != nil {
		return nil, fmt.Errorf("encode semantic transaction wire: %w", err)
	}
	return wire, nil
}

// convertV4SemanticWALRecord rewrites one V4 WAL entry whose semantic entity
// commit payload still uses the historical gob envelope into the current
// semantic transaction wire format. Non-entity-commit entries are returned
// unchanged, and the caller's entry (including its Data bytes) is never
// mutated.
func convertV4SemanticWALRecord(entry WALEntry) (WALEntry, error) {
	if entry.Operation != OpEntityCommit {
		return entry, nil
	}
	var commit walEntityCommitData
	if err := decodeRecoveryPayload(entry.Data, &commit); err != nil {
		return entry, fmt.Errorf("decode WAL entity commit payload: %w", err)
	}
	wire, err := TranscodeV4EntityTransaction(commit.Payload)
	if err != nil {
		return entry, err
	}
	data, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: wire})
	if err != nil {
		return entry, fmt.Errorf("encode WAL entity commit payload: %w", err)
	}
	converted := entry
	converted.Data = data
	converted.Checksum = crc32Checksum(data)
	return converted, nil
}
