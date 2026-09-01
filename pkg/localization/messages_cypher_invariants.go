package localization

const (
	MessageCypherInvariantsPipelineCreateFailed                 MessageID = "cypherinvariants.pipeline_create_failed"
	MessageCypherInvariantsDeleteByPrefixTransactionUnsupported MessageID = "cypherinvariants.delete_by_prefix_transaction_unsupported"
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
