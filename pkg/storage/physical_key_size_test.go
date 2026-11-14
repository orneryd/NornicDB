package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreKeyAdmissionHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tx := &BadgerTransaction{}
	require.ErrorIs(t, tx.storeKeyLimitErrorLocked(ctx), context.Canceled)
}

func TestPhysicalKeySizingMatchesEncoding(t *testing.T) {
	for _, namespaceSize := range []int{0, 4, 127, 128} {
		for _, nameSize := range []int{0, 1, 127, 128, 16383, 16384, 65000} {
			t.Run(fmt.Sprintf("namespace=%d/name=%d", namespaceSize, nameSize), func(t *testing.T) {
				namespace := strings.Repeat("n", namespaceSize)
				name := strings.Repeat("x", nameSize)
				require.Equal(t, len(propKeyForwardKey(namespace, name)), propKeyForwardKeySize(namespace, name))
				require.Equal(t, len(countDeltaKey(countDeltaKindLabel, namespace, name, ^uint64(0))), countDeltaKeySize(namespace, name))
				require.Equal(t, len(edgeTypeLabelCountKey(1, namespace, "人物", name)), edgeTypeLabelCountKeySize(namespace, "人物", name))
			})
		}
	}
	// Capacity changes must not change the persisted key representation.
	require.Equal(t, append([]byte{prefixPropKeyForward, 4}, []byte{'t', 'e', 's', 't', 1, 'p'}...), propKeyForwardKey("test", "p"))
}

func TestStoreLimitErrorsKeepTheirCauseAndClientClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"value", storeValueFits(10, 11, "body"), ErrStoreValueTooLarge},
		{"key", storeKeyFits(storeKeyLimit+1, "key"), ErrStoreKeyTooLong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.err, tc.want)
			coded, ok := tc.err.(interface{ BoltErrorCode() string })
			require.True(t, ok)
			require.Equal(t, "Neo.ClientError.Statement.ArgumentError", coded.BoltErrorCode())
		})
	}
}
