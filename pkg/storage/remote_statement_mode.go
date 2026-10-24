package storage

import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

const (
	remoteModeUnknown int32 = iota
	remoteModeRead
	remoteModeWrite
)

type remoteQueryStats struct {
	ContainsUpdates       *bool `json:"contains_updates"`
	ContainsSystemUpdates *bool `json:"contains_system_updates"`
}

func remoteQueryTypeMode(queryType string) int32 {
	switch queryType {
	case "r":
		return remoteModeRead
	case "w", "rw", "s":
		return remoteModeWrite
	}
	return remoteModeUnknown
}

// Positive standard counters prove a write. Zero counters alone cannot prove
// READ: a WRITE callback can return no counters. HTTP has no standard query-
// type field; our optional owner-produced mode supplies that missing evidence.
// Older/foreign responses without it remain unknown and fail safe at admission.
func remoteHTTPQueryMode(stats *remoteQueryStats, queryType string) int32 {
	if stats == nil {
		return remoteQueryTypeMode(queryType)
	}
	if stats.ContainsUpdates != nil && *stats.ContainsUpdates || stats.ContainsSystemUpdates != nil && *stats.ContainsSystemUpdates {
		return remoteModeWrite
	}
	return remoteQueryTypeMode(queryType)
}

func remoteBoltQueryMode(summary neo4j.ResultSummary) int32 {
	if summary == nil {
		return remoteModeUnknown
	}
	switch summary.StatementType() {
	case neo4j.StatementTypeReadOnly:
		return remoteModeRead
	case neo4j.StatementTypeWriteOnly, neo4j.StatementTypeReadWrite, neo4j.StatementTypeSchemaWrite:
		return remoteModeWrite
	}
	if counters := summary.Counters(); counters != nil && counters.ContainsUpdates() {
		return remoteModeWrite
	}
	return remoteModeUnknown
}

func (tx *boltCypherTx) ResolvedWriteMode() (bool, bool) {
	mode := tx.resolvedMode.Load()
	return mode == remoteModeWrite, mode != remoteModeUnknown
}
func (tx *httpCypherTx) ResolvedWriteMode() (bool, bool) {
	mode := tx.resolvedMode.Load()
	return mode == remoteModeWrite, mode != remoteModeUnknown
}
