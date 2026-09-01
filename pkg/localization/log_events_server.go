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
	EventServerAuthenticationDisabled EventID = "server.auth.disabled"
	// EventServerUIInitializationFailed identifies unavailable browser UI assets.
	EventServerUIInitializationFailed EventID = "server.ui.initialization_failed"
	// EventServerUIEnabled identifies successful browser UI registration.
	EventServerUIEnabled EventID = "server.ui.enabled"
	// EventServerRateLimitEnabled identifies enabled request rate limiting.
	EventServerRateLimitEnabled EventID = "server.rate_limit.enabled"
	// EventServerGraphQLEnabled identifies successful GraphQL route registration.
	EventServerGraphQLEnabled EventID = "server.graphql.enabled"
	// EventServerHeimdallDisabled identifies startup with Heimdall disabled.
	EventServerHeimdallDisabled                 EventID   = "server.heimdall.disabled"
	MessageServerLogMCPDisabled                 MessageID = "server.log.mcp_disabled"
	MessageServerLogRemoteCredentialKeyFallback MessageID = "server.log.remote_credentials_key_fallback"
	MessageServerLogUIHeadless                  MessageID = "server.log.ui_headless"
	MessageServerLogAuthenticationDisabled      MessageID = "server.log.authentication_disabled"
	MessageServerLogUIInitializationFailed      MessageID = "server.log.ui_initialization_failed"
	MessageServerLogUIEnabled                   MessageID = "server.log.ui_enabled"
	MessageServerLogRateLimitEnabled            MessageID = "server.log.rate_limit_enabled"
	MessageServerLogGraphQLEnabled              MessageID = "server.log.graphql_enabled"
	MessageServerLogHeimdallDisabled            MessageID = "server.log.heimdall_disabled"
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

// ServerLogUIInitializationFailed describes unavailable browser UI assets.
func ServerLogUIInitializationFailed() Message {
	return Message{ID: MessageServerLogUIInitializationFailed, Fallback: "UI initialization failed"}
}

// ServerLogUIEnabled describes successful browser UI registration.
func ServerLogUIEnabled() Message {
	return Message{ID: MessageServerLogUIEnabled, Fallback: "UI browser enabled"}
}

// ServerLogRateLimitEnabled describes enabled request rate limiting.
func ServerLogRateLimitEnabled() Message {
	return Message{ID: MessageServerLogRateLimitEnabled, Fallback: "rate limiting enabled"}
}

// ServerLogGraphQLEnabled describes successful GraphQL route registration.
func ServerLogGraphQLEnabled() Message {
	return Message{ID: MessageServerLogGraphQLEnabled, Fallback: "graphql API enabled"}
}

// ServerLogHeimdallDisabled describes startup with Heimdall disabled.
func ServerLogHeimdallDisabled() Message {
	return Message{ID: MessageServerLogHeimdallDisabled, Fallback: "heimdall AI assistant disabled"}
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

// ServerUIInitializationFailedEvent describes unavailable browser UI assets.
func ServerUIInitializationFailedEvent(err error) LogEvent {
	return LogEvent{
		ID:      EventServerUIInitializationFailed,
		Message: ServerLogUIInitializationFailed(),
		Attrs:   []slog.Attr{slog.Any("error", err)},
	}
}

// ServerUIEnabledEvent describes successful browser UI registration.
func ServerUIEnabledEvent(route string) LogEvent {
	return LogEvent{
		ID:      EventServerUIEnabled,
		Message: ServerLogUIEnabled(),
		Attrs:   []slog.Attr{slog.String("route", route)},
	}
}

// ServerRateLimitEnabledEvent describes enabled request rate limiting.
func ServerRateLimitEnabledEvent(perMinute, perHour int, scope string) LogEvent {
	return LogEvent{
		ID:      EventServerRateLimitEnabled,
		Message: ServerLogRateLimitEnabled(),
		Attrs: []slog.Attr{
			slog.Int("per_minute", perMinute),
			slog.Int("per_hour", perHour),
			slog.String("scope", scope),
		},
	}
}

// ServerGraphQLEnabledEvent describes successful GraphQL route registration.
func ServerGraphQLEnabledEvent(route string) LogEvent {
	return LogEvent{
		ID:      EventServerGraphQLEnabled,
		Message: ServerLogGraphQLEnabled(),
		Attrs:   []slog.Attr{slog.String("route", route)},
	}
}

// ServerHeimdallDisabledEvent describes startup with Heimdall disabled.
func ServerHeimdallDisabledEvent(subsystem, overrideEnv string) LogEvent {
	return LogEvent{
		ID:      EventServerHeimdallDisabled,
		Message: ServerLogHeimdallDisabled(),
		Attrs: []slog.Attr{
			slog.String("subsystem", subsystem),
			slog.String("override_env", overrideEnv),
		},
	}
}
