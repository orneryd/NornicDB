package fn

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestIdentityFunctionNamesPreserveCodeUnits(t *testing.T) {
	units := []string{"\xed\xa0\x80", "\xed\xa0\x81", "\xed\xb0\x80", "\ufffd", `\ud800`, "😀"}
	mu.Lock()
	previous := map[string]Func{}
	present := map[string]bool{}
	for _, unit := range units {
		for _, key := range []string{"identity_units." + unit, strings.ToLower("identity_units." + unit)} {
			if _, seen := present[key]; !seen {
				previous[key], present[key] = registry[key]
			}
		}
	}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for key, value := range previous {
			if present[key] {
				registry[key] = value
			} else {
				delete(registry, key)
			}
		}
	})
	for index, unit := range units {
		want := int64(index + 1)
		Register("identity_units."+unit, func(Context, []string) (interface{}, error) { return want, nil })
	}
	ctx := Context{Eval: func(string) (interface{}, error) { return nil, nil }, Now: time.Now}
	for index, unit := range units {
		for _, prefix := range []string{"identity_units.", "IDENTITY_UNITS."} {
			value, found, err := EvaluateFunction(prefix+unit, nil, ctx)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(index+1), value, "exact registered name %q", prefix+unit)
		}
	}
}
