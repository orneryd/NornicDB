package localization

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCustomFormattingMessage(t *testing.T) {
	message := CypherCustomFormattingUnsupported("custom.Type")
	require.Equal(t, MessageCypherCustomFormattingUnsupported, message.ID)
	require.Equal(t, "custom.Type", message.Data["Type"])
	require.Contains(t, message.Fallback, "custom.Type")
}
