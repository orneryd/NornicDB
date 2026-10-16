package localization

// MessageCypherCustomFormattingUnsupported identifies a custom Go value whose
// formatter cannot participate in a statement's cancellation budget.
const MessageCypherCustomFormattingUnsupported MessageID = "cyphercore.custom_formatting_unsupported"

// CypherCustomFormattingUnsupported does not invoke the value's formatter.
func CypherCustomFormattingUnsupported(typeName string) Message {
	return Message{
		ID:       MessageCypherCustomFormattingUnsupported,
		Fallback: "custom formatting is unsupported for value type " + typeName + " in a cancellable query",
		Data:     map[string]any{"Type": typeName},
	}
}
