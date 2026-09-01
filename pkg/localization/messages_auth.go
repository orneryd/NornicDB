package localization

const (
	MessageUnsupportedGrantType  MessageID = "auth.unsupported_grant_type"
	MessageAPITokenAdminRequired MessageID = "auth.api_token_admin_required"
	MessageInvalidExpiresIn      MessageID = "auth.invalid_expires_in"
	MessageInvalidExpiresInHelp  MessageID = "auth.invalid_expires_in_help"
	MessageAPITokenFailed        MessageID = "auth.api_token_generation_failed"
	MessageOAuthCallbackError    MessageID = "auth.oauth_callback_error"
	MessageAuthorizationCode     MessageID = "auth.authorization_code_missing"
	MessageStateParameter        MessageID = "auth.state_parameter_missing"
	MessageNoUserContext         MessageID = "auth.user_context_missing"
	MessageOldPasswordIncorrect  MessageID = "auth.old_password_incorrect"
	MessageLogoutComplete        MessageID = "auth.logout_complete"
	MessagePasswordChanged       MessageID = "auth.password_changed"
	MessageProfileUpdated        MessageID = "auth.profile_updated"
	MessagePutRequired           MessageID = "server.method_put_required"
)

// UnsupportedGrantType identifies an unsupported OAuth grant_type value.
func UnsupportedGrantType() Message {
	return Message{ID: MessageUnsupportedGrantType, Fallback: "unsupported grant_type"}
}

// APITokenAdminRequired identifies a non-admin API token generation request.
func APITokenAdminRequired() Message {
	return Message{ID: MessageAPITokenAdminRequired, Fallback: "admin role required to generate API tokens"}
}

// InvalidExpiresIn identifies an invalid API token expires_in value.
func InvalidExpiresIn() Message {
	return Message{ID: MessageInvalidExpiresIn, Fallback: "invalid expires_in format"}
}

// InvalidExpiresInWithHelp identifies an invalid expires_in value with accepted examples.
func InvalidExpiresInWithHelp() Message {
	return Message{ID: MessageInvalidExpiresInHelp, Fallback: "invalid expires_in format (use: 1h, 24h, 7d, 365d, 0 for never)"}
}

// APITokenGenerationFailed identifies an internal API token generation failure.
func APITokenGenerationFailed() Message {
	return Message{ID: MessageAPITokenFailed, Fallback: "failed to generate token"}
}

// OAuthCallbackFailed identifies an OAuth provider callback error.
func OAuthCallbackFailed(providerError, description string) Message {
	return Message{
		ID:       MessageOAuthCallbackError,
		Fallback: "OAuth error: " + providerError + " - " + description,
		Data:     map[string]any{"Error": providerError, "Description": description},
	}
}

// MissingAuthorizationCode identifies an OAuth callback without a code.
func MissingAuthorizationCode() Message {
	return Message{ID: MessageAuthorizationCode, Fallback: "missing authorization code"}
}

// MissingStateParameter identifies an OAuth callback without state.
func MissingStateParameter() Message {
	return Message{ID: MessageStateParameter, Fallback: "missing state parameter"}
}

// NoUserContext identifies an authenticated endpoint without principal context.
func NoUserContext() Message {
	return Message{ID: MessageNoUserContext, Fallback: "no user context"}
}

// OldPasswordIncorrect identifies a rejected current password.
func OldPasswordIncorrect() Message {
	return Message{ID: MessageOldPasswordIncorrect, Fallback: "old password incorrect"}
}

// LogoutComplete identifies a completed HTTP logout operation.
func LogoutComplete() Message {
	return Message{ID: MessageLogoutComplete, Fallback: "logged out"}
}

// PasswordChanged identifies a completed password change.
func PasswordChanged() Message {
	return Message{ID: MessagePasswordChanged, Fallback: "password changed"}
}

// ProfileUpdated identifies a completed profile update.
func ProfileUpdated() Message {
	return Message{ID: MessageProfileUpdated, Fallback: "profile updated"}
}

// PutRequired identifies an endpoint that only accepts PUT requests.
func PutRequired() Message {
	return Message{ID: MessagePutRequired, Fallback: "PUT required"}
}
