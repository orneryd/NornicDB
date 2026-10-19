package localization

import "fmt"

const (
	MessageCypherANTLRParseEmptyQuery     MessageID = "cypherantlr.parse_empty_query"
	MessageCypherANTLRParseSyntaxError    MessageID = "cypherantlr.parse_syntax_error"
	MessageCypherANTLRValidateEmptyQuery  MessageID = "cypherantlr.validate_empty_query"
	MessageCypherANTLRValidateSyntaxError MessageID = "cypherantlr.validate_syntax_error"
	MessageCypherANTLRUnexpectedInput     MessageID = "cypherantlr.unexpected_input"
)

// CypherANTLRParseEmptyQuery identifies an empty query passed to Parse.
func CypherANTLRParseEmptyQuery() Message {
	return Message{ID: MessageCypherANTLRParseEmptyQuery, Fallback: "empty query"}
}

// CypherANTLRUnexpectedInput identifies a syntax location without echoing query
// text. ANTLR's diagnostic may include password literals and lexer excerpts.
func CypherANTLRUnexpectedInput(line, column int) Message {
	return Message{ID: MessageCypherANTLRUnexpectedInput, Fallback: fmt.Sprintf("line %d:%d unexpected input", line, column), Data: map[string]any{"Line": line, "Column": column}}
}

// CypherANTLRParseSyntaxError identifies invalid syntax encountered by Parse.
func CypherANTLRParseSyntaxError(detail string) Message {
	return Message{ID: MessageCypherANTLRParseSyntaxError, Fallback: "syntax error: " + detail, Data: map[string]any{"Detail": detail}}
}

// CypherANTLRValidateEmptyQuery identifies an empty query passed to Validate.
func CypherANTLRValidateEmptyQuery() Message {
	return Message{ID: MessageCypherANTLRValidateEmptyQuery, Fallback: "empty query"}
}

// CypherANTLRValidateSyntaxError identifies invalid syntax encountered by Validate.
func CypherANTLRValidateSyntaxError(detail string) Message {
	return Message{ID: MessageCypherANTLRValidateSyntaxError, Fallback: "syntax error: " + detail, Data: map[string]any{"Detail": detail}}
}
