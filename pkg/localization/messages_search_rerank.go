package localization

import "strconv"

const (
	MessageSearchRerankRequestMarshalFailed  MessageID = "search.rerank_request_marshal_failed"
	MessageSearchRerankRequestCreationFailed MessageID = "search.rerank_request_creation_failed"
	MessageSearchRerankRequestFailed         MessageID = "search.rerank_request_failed"
	MessageSearchRerankAPIStatus             MessageID = "search.rerank_api_status"
	MessageSearchRerankResponseParseFailed   MessageID = "search.rerank_response_parse_failed"
	MessageSearchRerankResponseUnrecognized  MessageID = "search.rerank_response_unrecognized"
)

// SearchRerankRequestMarshalFailed identifies rerank request serialization failure.
func SearchRerankRequestMarshalFailed(cause error) Message {
	return Message{ID: MessageSearchRerankRequestMarshalFailed, Fallback: "failed to marshal request: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchRerankRequestCreationFailed identifies rerank HTTP request construction failure.
func SearchRerankRequestCreationFailed(cause error) Message {
	return Message{ID: MessageSearchRerankRequestCreationFailed, Fallback: "failed to create request: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchRerankRequestFailed identifies rerank HTTP request execution failure.
func SearchRerankRequestFailed(cause error) Message {
	return Message{ID: MessageSearchRerankRequestFailed, Fallback: "rerank request failed: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchRerankAPIStatus identifies a non-success rerank API response status.
func SearchRerankAPIStatus(status int) Message {
	return Message{ID: MessageSearchRerankAPIStatus, Fallback: "rerank API returned status " + strconv.Itoa(status), Data: map[string]any{"Status": status}}
}

// SearchRerankResponseParseFailed identifies rerank response decoding failure.
func SearchRerankResponseParseFailed(cause error) Message {
	return Message{ID: MessageSearchRerankResponseParseFailed, Fallback: "failed to parse response: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchRerankResponseUnrecognized identifies an unsupported rerank response shape.
func SearchRerankResponseUnrecognized() Message {
	return Message{ID: MessageSearchRerankResponseUnrecognized, Fallback: "unable to parse rerank response"}
}
