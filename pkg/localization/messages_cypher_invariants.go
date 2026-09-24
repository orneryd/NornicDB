package localization

const (
	MessageCypherInvariantsPipelineCreateFailed                 MessageID = "cypherinvariants.pipeline_create_failed"
	MessageCypherInvariantsDeleteByPrefixTransactionUnsupported MessageID = "cypherinvariants.delete_by_prefix_transaction_unsupported"
	MessageCypherInvariantsPipelineDeclinedAfterWrite           MessageID = "cypherinvariants.pipeline_declined_after_write"
)

// CypherInvariantsPipelineCreateFailed identifies a CREATE failure in the pipeline executor.
func CypherInvariantsPipelineCreateFailed(cause error) Message {
	return Message{
		ID:       MessageCypherInvariantsPipelineCreateFailed,
		Fallback: "pipeline CREATE failed: " + cause.Error(),
		Data:     map[string]any{"Cause": cause.Error()},
	}
}

// CypherInvariantsDeleteByPrefixTransactionUnsupported identifies an unsupported transactional bulk deletion.
func CypherInvariantsDeleteByPrefixTransactionUnsupported() Message {
	return Message{
		ID:       MessageCypherInvariantsDeleteByPrefixTransactionUnsupported,
		Fallback: "DeleteByPrefix not supported within transaction context",
	}
}

// CypherInvariantsPipelineDeclinedAfterWrite identifies a pipeline statement
// that could not be finished after one of its clauses had written; it is not
// handed to another route because that would repeat the writes.
func CypherInvariantsPipelineDeclinedAfterWrite(clause string) Message {
	return Message{
		ID:       MessageCypherInvariantsPipelineDeclinedAfterWrite,
		Fallback: "pipeline could not finish a statement after it had written (" + clause + "); the statement was not run again",
		Data:     map[string]any{"Clause": clause},
	}
}
