// Author: Codex, NOR-180 implementation lane, 2026-10-26.
package localization

const (
	MessageCypherTransactionsWALAbortFailed  MessageID = "cyphertransactions.wal_abort_failed"
	MessageCypherTransactionsWALCommitFailed MessageID = "cyphertransactions.wal_commit_failed"
)

func CypherTransactionsWALAbortFailed(txID string, cause error) Message {
	return cypherTransactionsMessage(MessageCypherTransactionsWALAbortFailed,
		"failed to write WAL tx abort for "+txID+": "+cause.Error(),
		map[string]any{"TxID": txID, "Cause": cause.Error()})
}

func CypherTransactionsWALCommitFailed(txID string, cause error) Message {
	return cypherTransactionsMessage(MessageCypherTransactionsWALCommitFailed,
		"transaction "+txID+" committed, but failed to write WAL tx commit: "+cause.Error(),
		map[string]any{"TxID": txID, "Cause": cause.Error()})
}
