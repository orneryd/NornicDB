package localization

import "log/slog"

const (
	// EventServerMCPDisabled identifies startup with MCP disabled by configuration.
	EventServerMCPDisabled EventID = "server.mcp.disabled"
	// EventServerRemoteCredentialKeyFallback identifies credential key reuse.
	EventServerRemoteCredentialKeyFallback EventID = "server.remote_credentials.key_fallback"
	// EventServerUIHeadless identifies UI disablement in headless mode.
	EventServerUIHeadless EventID = "server.ui.headless"
	// EventServerAuthenticationDisabled identifies startup without authentication.
	EventServerAuthenticationDisabled           EventID   = "server.auth.disabled"
	MessageServerLogMCPDisabled                 MessageID = "server.log.mcp_disabled"
	MessageServerLogRemoteCredentialKeyFallback MessageID = "server.log.remote_credentials_key_fallback"
	MessageServerLogUIHeadless                  MessageID = "server.log.ui_headless"
	MessageServerLogAuthenticationDisabled      MessageID = "server.log.authentication_disabled"
)

// ServerLogMCPDisabled describes an MCP-disabled startup log message.
func ServerLogMCPDisabled() Message {
	return Message{ID: MessageServerLogMCPDisabled, Fallback: "mcp server disabled via configuration"}
}

// ServerLogRemoteCredentialKeyFallback describes a credential key reuse warning.
func ServerLogRemoteCredentialKeyFallback() Message {
	return Message{ID: MessageServerLogRemoteCredentialKeyFallback, Fallback: "remote credential encryption key fallback in use"}
}

// ServerLogUIHeadless describes UI disablement in headless mode.
func ServerLogUIHeadless() Message {
	return Message{ID: MessageServerLogUIHeadless, Fallback: "headless mode: UI disabled"}
}

// ServerLogAuthenticationDisabled describes startup without authentication.
func ServerLogAuthenticationDisabled() Message {
	return Message{ID: MessageServerLogAuthenticationDisabled, Fallback: "authentication disabled"}
}

// ServerMCPDisabledEvent describes startup with MCP disabled by configuration.
func ServerMCPDisabledEvent() LogEvent {
	return LogEvent{
		ID:      EventServerMCPDisabled,
		Message: ServerLogMCPDisabled(),
	}
}

// ServerRemoteCredentialKeyFallbackEvent describes credential key reuse.
func ServerRemoteCredentialKeyFallbackEvent(fallback, remediation string) LogEvent {
	return LogEvent{
		ID:      EventServerRemoteCredentialKeyFallback,
		Message: ServerLogRemoteCredentialKeyFallback(),
		Attrs: []slog.Attr{
			slog.String("fallback", fallback),
			slog.String("remediation", remediation),
		},
	}
}

// ServerUIHeadlessEvent describes UI disablement in headless mode.
func ServerUIHeadlessEvent() LogEvent {
	return LogEvent{ID: EventServerUIHeadless, Message: ServerLogUIHeadless()}
}

// ServerAuthenticationDisabledEvent describes startup without authentication.
func ServerAuthenticationDisabledEvent() LogEvent {
	return LogEvent{ID: EventServerAuthenticationDisabled, Message: ServerLogAuthenticationDisabled()}
}
