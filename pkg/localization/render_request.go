package localization

// RenderPurpose selects the existing message or error boundary contract.
type RenderPurpose uint8

const (
	// MessageText renders a message through the catalog. A nil manager uses its
	// English fallback, or reports an error when that fallback is absent.
	MessageText RenderPurpose = iota
	// ErrorText renders an error's message. A nil message renders empty text; a
	// nil manager renders its English fallback or message ID without an error.
	ErrorText
)

// RenderRequest identifies a message and the boundary contract for rendering it.
// MessageText with nil Message denotes an empty message. ErrorText with nil
// Message denotes a nil error. These states retain their different nil-manager
// policies. A zero request has the MessageText contract.
//
// Message must remain immutable while rendering. Custom renderers can inspect
// both fields. A caller that already owns an immutable message can reference it
// directly; MessageRequest makes a value copy for callers needing that lifetime.
//
// text, tag, err := manager.Render(ctx, localization.MessageRequest(message))
// text, tag, err = manager.Render(ctx, localization.ErrorRequest(localizedError))
type RenderRequest struct {
	Purpose RenderPurpose
	Message *Message
}

// MessageRequest captures a message value for a MessageText rendering boundary.
// Template-data references retain their existing shallow-copy semantics.
func MessageRequest(message Message) RenderRequest {
	return RenderRequest{Purpose: MessageText, Message: &message}
}

// ErrorRequest selects an error's message without changing its code or cause.
// A nil error retains the empty-text contract even when a manager is present.
func ErrorRequest(carrier *LocalizedError) RenderRequest {
	if carrier == nil {
		return RenderRequest{Purpose: ErrorText}
	}
	return RenderRequest{Purpose: ErrorText, Message: &carrier.Message}
}
