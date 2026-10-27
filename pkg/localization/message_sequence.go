package localization

import "strings"

// MessageSequence keeps joined diagnostics localizable until the rendering
// boundary chooses a language. String supplies the source-language fallback.
type MessageSequence struct {
	Messages  []Message
	Separator string
}

func (s MessageSequence) String() string {
	parts := make([]string, len(s.Messages))
	for i, message := range s.Messages {
		parts[i] = message.Fallback
	}
	return strings.Join(parts, s.Separator)
}
