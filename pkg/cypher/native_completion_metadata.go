package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// attachNativeCommitMetadata projects the native publication's facts. A WAL
// receipt is retained for durable and buffered ACKs alike; eventual distinguishes
// the latter so HTTP answers 202 Accepted rather than 200. Neither statement
// shape nor configured buffering is a substitute for the actual native ACK.
func attachNativeCommitMetadata(result *ExecuteResult, receipt *storage.Receipt, acknowledgedBuffered bool) {
	if receipt == nil && !acknowledgedBuffered {
		return
	}
	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{})
	}
	if receipt != nil {
		result.Metadata["receipt"] = receipt
	}
	if acknowledgedBuffered {
		result.Metadata["eventual"] = true
	}
}
