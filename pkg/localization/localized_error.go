package localization

// LocalizedError carries stable error identity and a localizable message separately.
type LocalizedError struct {
	Code    string
	Message Message
	Cause   error
}

// NewLocalizedError constructs an error that preserves message identity and wrapping.
func NewLocalizedError(code string, message Message, cause error) *LocalizedError {
	return &LocalizedError{Code: code, Message: message, Cause: cause}
}

// Error returns the source English fallback for backward compatibility.
func (e *LocalizedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message.Fallback != "" {
		return e.Message.Fallback
	}
	return string(e.Message.ID)
}

// Unwrap preserves errors.Is and errors.As behavior for the underlying cause.
func (e *LocalizedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
