package storage

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWALCaptureDecisionRoundTrip(t *testing.T) {
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{
		Namespace: "test", LogicalID: "capture-roundtrip", Mode: CommitModeDirect,
		Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:n"}}},
	})
	require.NoError(t, err)
	for _, decision := range []uint8{0, 1, 2, 3, 4, 255} {
		t.Run("encoded_"+strconv.Itoa(int(decision)), func(t *testing.T) {
			encoded, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: payload, Capture: decision})
			require.NoError(t, err)
			var record walEntityCommitData
			require.NoError(t, decodeRecoveryPayload(encoded, &record))
			envelope, operations, err := record.decode()
			if decision > uint8(ChangeCaptureFull)+1 {
				require.ErrorIs(t, err, ErrInvalidData)
				return
			}
			require.NoError(t, err)
			require.Len(t, operations, 1)
			require.Equal(t, decision != 0, envelope.capture.decided)
			if decision != 0 {
				require.Equal(t, ChangeCaptureMode(decision-1), envelope.capture.mode)
			}
		})
	}
	_, _, err = (walEntityCommitData{Payload: []byte("invalid"), Capture: 3}).decode()
	require.Error(t, err)
}

func TestWALNativeWritesRetainCaptureDecision(t *testing.T) {
	for _, mode := range []ChangeCaptureMode{ChangeCaptureOff, ChangeCaptureDiff, ChangeCaptureFull} {
		t.Run(mode.String(), func(t *testing.T) {
			native, wal, wrapped := newWALClientFixture(t, nil)
			native.SetChangeCapture("test", mode)
			_, err := wrapped.CreateNode(t.Context(), &Node{ID: "test:capture", Properties: map[string]any{"v": 1}})
			require.NoError(t, err)
			entries := walClientEntries(t, wal)
			require.Len(t, entries, 1)
			var record walEntityCommitData
			require.NoError(t, decodeRecoveryPayload(entries[0].Data, &record))
			envelope, _, err := record.decode()
			require.NoError(t, err)
			require.True(t, envelope.capture.decided)
			require.Equal(t, mode, envelope.capture.mode)
		})
	}
}

// Compare the historical metadata-free record with the admitted-mode record;
// payload and encoder ownership are identical, so allocation growth is visible.
func BenchmarkWALCaptureDecisionEncoding(b *testing.B) {
	for _, size := range []int{256, 65536} {
		for _, decision := range []uint8{0, 3} {
			b.Run(strconv.Itoa(size)+"/capture_"+strconv.Itoa(int(decision)), func(b *testing.B) {
				record := walEntityCommitData{Payload: make([]byte, size), Capture: decision}
				encoded, err := marshalWALRecoveryPayload(nil, record)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := marshalWALRecoveryPayload(nil, record); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(encoded)), "encoded-B")
			})
		}
	}
}
