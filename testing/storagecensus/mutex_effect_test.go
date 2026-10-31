package storagecensus

import (
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMutexEffectsBindReceiverAndConditionalAcquisition(t *testing.T) {
	for _, test := range []struct {
		path, receiver, method, action string
		exclusive, fallible            bool
	}{
		{"sync", "Mutex", "Lock", "lock", true, false},
		{"sync", "RWMutex", "RLock", "lock", false, false},
		{"sync", "RWMutex", "Unlock", "unlock", false, false},
		{"sync", "RWMutex", "LockContext", "", false, false},
		{"github.com/orneryd/nornicdb/pkg/storage", "trackedRWMutex", "Lock", "lock", true, false},
		{"github.com/orneryd/nornicdb/pkg/storage", "trackedRWMutex", "LockContext", "lock", true, true},
		{"github.com/orneryd/nornicdb/pkg/storage", "trackedRWMutex", "RLockContext", "lock", false, true},
		{"github.com/orneryd/nornicdb/pkg/storage", "trackedRWMutex", "RUnlock", "unlock", false, false},
		{"github.com/orneryd/nornicdb/pkg/storage", "trackedRWMutex", "TryLock", "", false, false},
		{"example.org/other", "trackedRWMutex", "LockContext", "", false, false},
		{"github.com/orneryd/nornicdb/pkg/storage", "otherMutex", "LockContext", "", false, false},
	} {
		t.Run(test.path+"/"+test.receiver+"/"+test.method, func(t *testing.T) {
			pkg := types.NewPackage(test.path, "fixture")
			named := types.NewNamed(types.NewTypeName(token.NoPos, pkg, test.receiver, nil), types.NewStruct(nil, nil), nil)
			receiver := types.NewVar(token.NoPos, pkg, "receiver", types.NewPointer(named))
			sig := types.NewSignatureType(receiver, nil, nil, nil, nil, false)
			method := types.NewFunc(token.NoPos, pkg, test.method, sig)
			action, exclusive, fallible := mutexMethodEffect(method)
			require.Equal(t, test.action, action)
			require.Equal(t, test.exclusive, exclusive)
			require.Equal(t, test.fallible, fallible)
		})
	}
	action, exclusive, fallible := mutexMethodEffect(nil)
	require.Empty(t, action)
	require.False(t, exclusive)
	require.False(t, fallible)
}
