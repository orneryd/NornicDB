package bolt

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLegacyDateTimeWireSignatureFailurePreservesState(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	for _, target := range []struct {
		name, address string
		reference     bool
	}{
		{"nornic", fmt.Sprintf("127.0.0.1:%d", port), false}, {"neo4j", "", true},
	} {
		t.Run(target.name, func(t *testing.T) {
			address := target.address
			if target.reference {
				address = dateTimeReferenceAddress(t)
			}
			conn := dateTimeWireConnect(t, address, 5, false, target.reference)
			defer conn.Close()
			message := encodePackStreamStringInto([]byte{0xB3, MsgRun}, "RETURN $v")
			message = append(message, 0xA1, 0x81, 'v', 0xB3, 0x46, 0, 0, 0, 0xA0)
			require.NoError(t, SendMessage(conn, message))
			code, _, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Equal(t, "Neo.ClientError.Request.Invalid", code)
			got := dateTimeWireQuery(t, conn, "RETURN 1", []byte{0xA0})
			require.Equal(t, encodePackStreamValue([]any{int64(1)}), got)

		})
	}
}
