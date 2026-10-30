package localization

const (
	MessageInvalidHeader             MessageID = "security.invalid_header"
	MessageInvalidAuthorizationToken MessageID = "security.invalid_authorization_token"
	MessageInvalidTokenParameter     MessageID = "security.invalid_token_parameter"
	MessageInvalidURLParameter       MessageID = "security.invalid_url_parameter"
)

// InvalidHeader identifies a request header rejected by security validation.
func InvalidHeader(name string, cause error) Message {
	return Message{ID: MessageInvalidHeader, Fallback: "Invalid header " + name + ": " + cause.Error(), Data: map[string]any{"Name": name, "Cause": cause.Error()}}
}

// InvalidAuthorizationToken identifies an Authorization token rejected by validation.
func InvalidAuthorizationToken(cause error) Message {
	return Message{ID: MessageInvalidAuthorizationToken, Fallback: "Invalid authorization token: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// InvalidTokenParameter identifies a query token rejected by validation.
func InvalidTokenParameter(cause error) Message {
	return Message{ID: MessageInvalidTokenParameter, Fallback: "Invalid token parameter: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// InvalidURLParameter identifies a URL-valued query parameter rejected by validation.
func InvalidURLParameter(name string, cause error) Message {
	return Message{ID: MessageInvalidURLParameter, Fallback: "Invalid " + name + " parameter: " + cause.Error(), Data: map[string]any{"Name": name, "Cause": cause.Error()}}
}
