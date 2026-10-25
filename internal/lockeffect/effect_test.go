package lockeffect

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifySyncLockBoundaries(t *testing.T) {
	for _, tc := range []struct {
		method    string
		deferred  bool
		action    string
		exclusive bool
	}{
		{"Lock", false, "lock", true}, {"RLock", false, "lock", false},
		{"Unlock", false, "unlock", false}, {"RUnlock", false, "unlock", false},
		{"Unlock", true, "defer", false}, {"RUnlock", true, "defer", false},
		{"Lock", true, "", false}, {"RLock", true, "", false}, {"TryLock", false, "", false},
	} {
		t.Run(tc.method+map[bool]string{false: "/direct", true: "/deferred"}[tc.deferred], func(t *testing.T) {
			action, exclusive := Classify(tc.method, tc.deferred)
			require.Equal(t, tc.action, action)
			require.Equal(t, tc.exclusive, exclusive)
		})
	}
}
