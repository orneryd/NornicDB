package json

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericWireFormattingPreservesFloat(t *testing.T) {
	for _, format := range []func(string) string{Pretty, Compact} {
		text := format(`{"v":2.0,"e":2e0,"big":9007199254740993,"s":"9007199254740993"}`)
		got := Parse(text).(map[string]interface{})
		t.Logf("formatted=%s parsed=%#v", text, got)
		require.IsType(t, float64(0), got["v"])
		require.IsType(t, float64(0), got["e"])
	}
}
