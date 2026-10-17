package localization

// Bolt user impersonation (imp_user on BEGIN, auto-commit RUN and ROUTE).
const (
	MessageBoltImpersonationForbidden    MessageID = "bolt.impersonation_forbidden"
	MessageBoltImpersonationAuthDisabled MessageID = "bolt.impersonation_auth_disabled"
	MessageBoltImpersonationUnsupported  MessageID = "bolt.impersonation_unsupported"
	MessageBoltImpersonatedUserInvalid   MessageID = "bolt.impersonated_user_invalid"
)

// BoltImpersonationForbidden identifies a request to impersonate a user the
// authenticated user may not impersonate (or one that doesn't exist, which
// is refused the same way).
func BoltImpersonationForbidden(user string) Message {
	return Message{ID: MessageBoltImpersonationForbidden, Fallback: "Permission denied: impersonating user '" + user + "' is not allowed.", Data: map[string]any{"User": user}}
}

// BoltImpersonationAuthDisabled identifies a request to impersonate a user
// on a server whose authentication is disabled, in Neo4j's words.
func BoltImpersonationAuthDisabled() Message {
	return Message{ID: MessageBoltImpersonationAuthDisabled, Fallback: "Impersonation is not supported with auth disabled."}
}

// BoltImpersonationUnsupported identifies a request to impersonate a user
// on a Bolt server whose authenticator can't resolve users.
func BoltImpersonationUnsupported() Message {
	return Message{ID: MessageBoltImpersonationUnsupported, Fallback: "Impersonation is not supported by this server's authenticator."}
}

// BoltImpersonatedUserInvalid identifies an imp_user that is neither a
// string nor null.
func BoltImpersonatedUserInvalid() Message {
	return Message{ID: MessageBoltImpersonatedUserInvalid, Fallback: "imp_user must be a string or null."}
}
