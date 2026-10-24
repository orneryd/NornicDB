package bolt

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Incoming DateTime signatures must belong to the negotiated family, even
// inside nested collections. The oracle uses the same independently built bytes.
func TestLegacyDateTimeWireSignatureNegotiation(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	for _, target := range []struct {
		name, address string
		reference     bool
	}{
		{"nornic", fmt.Sprintf("127.0.0.1:%d", port), false},
		{"neo4j", "", true},
	} {
		t.Run(target.name, func(t *testing.T) {
			for _, protocol := range []struct {
				name    string
				version byte
				patch   bool
			}{
				{"legacy44", 4, false}, {"utc44", 4, true}, {"bolt50", 5, false},
			} {
				for _, signature := range []byte{0x46, 0x66, 0x49, 0x69} {
					for _, shape := range []string{"scalar", "list", "map"} {
						t.Run(fmt.Sprintf("%s/%x/%s", protocol.name, signature, shape), func(t *testing.T) {
							address := target.address
							if target.reference {
								address = dateTimeReferenceAddress(t)
							}
							conn := dateTimeWireConnect(t, address, protocol.version, protocol.patch, target.reference)
							defer conn.Close()
							value := encodePackStreamIntInto([]byte{0xB3, signature}, 4500)
							value = encodePackStreamIntInto(value, 42)
							if signature == 0x66 || signature == 0x69 {
								value = encodePackStreamStringInto(value, "Europe/Paris")
							} else {
								value = encodePackStreamIntInto(value, 3600)
							}
							access := "$v"
							switch shape {
							case "list":
								value = append([]byte{0x91}, value...)
								access = "$v[0]"
							case "map":
								value = append([]byte{0xA1, 0x81, 't'}, value...)
								access = "$v.t"
							}
							params := append([]byte{0xA1, 0x81, 'v'}, value...)
							utc := protocol.patch || protocol.version == 5
							allowed := utc == (signature == 0x49 || signature == 0x69)
							if allowed {
								got := dateTimeWireQuery(t, conn, "RETURN "+access+".nanosecond", params)
								require.Equal(t, encodePackStreamValue([]any{int64(42)}), got)
							} else {
								message := encodePackStreamStringInto([]byte{0xB3, MsgRun}, "RETURN "+access)
								message = append(message, params...)
								require.NoError(t, SendMessage(conn, append(message, 0xA0)))
								code, _, err := AssertFailure(t, conn)
								require.NoError(t, err)
								require.Equal(t, "Neo.ClientError.Request.Invalid", code)
								recovered := dateTimeWireQuery(t, conn, "RETURN 1", []byte{0xA0})
								require.Equal(t, encodePackStreamValue([]any{int64(1)}), recovered)

							}
						})
					}
				}
			}
		})
	}
}

// RUN/BEGIN metadata and PULL/DISCARD options have separate consumers; each
// must carry the same negotiated policy as the parameters consumer.
func TestLegacyDateTimeWireMetadataSignatureNegotiation(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	for _, target := range []struct {
		name, address string
		reference     bool
	}{
		{"nornic", fmt.Sprintf("127.0.0.1:%d", port), false},
		{"neo4j", "", true},
	} {
		for _, protocol := range []struct {
			name    string
			version byte
			patch   bool
		}{
			{"legacy44", 4, false}, {"utc44", 4, true}, {"bolt50", 5, false},
		} {
			for _, signature := range []byte{0x46, 0x66, 0x49, 0x69} {
				for _, messageType := range []byte{MsgRun, MsgBegin, MsgPull, MsgDiscard} {
					t.Run(fmt.Sprintf("%s/%s/%x/%x", target.name, protocol.name, signature, messageType), func(t *testing.T) {
						address := target.address
						if target.reference {
							address = dateTimeReferenceAddress(t)
						}
						conn := dateTimeWireConnect(t, address, protocol.version, protocol.patch, target.reference)
						defer conn.Close()
						value := encodePackStreamIntInto([]byte{0xB3, signature}, 4500)
						value = encodePackStreamIntInto(value, 42)
						if signature == 0x66 || signature == 0x69 {
							value = encodePackStreamStringInto(value, "Europe/Paris")
						} else {
							value = encodePackStreamIntInto(value, 3600)
						}
						// tx_metadata: {nested: [datetime]}
						metadata := encodePackStreamStringInto([]byte{0xA1}, "tx_metadata")
						metadata = encodePackStreamStringInto(append(metadata, 0xA1), "nested")
						metadata = append(append(metadata, 0x91), value...)
						if messageType == MsgPull || messageType == MsgDiscard {
							metadata[0] = 0xA2
							metadata = encodePackStreamStringInto(metadata, "n")
							metadata = encodePackStreamIntInto(metadata, -1)
							run := encodePackStreamStringInto([]byte{0xB3, MsgRun}, "RETURN 1")
							require.NoError(t, SendMessage(conn, append(run, 0xA0, 0xA0)))
							require.NoError(t, ReadSuccess(t, conn))
						}
						message := []byte{0xB1, messageType}
						if messageType == MsgRun {
							message = encodePackStreamStringInto([]byte{0xB3, MsgRun}, "RETURN 1")
							message = append(message, 0xA0)
						}
						require.NoError(t, SendMessage(conn, append(message, metadata...)))
						allowed := (protocol.patch || protocol.version == 5) == (signature == 0x49 || signature == 0x69)
						if !allowed {
							code, _, err := AssertFailure(t, conn)
							require.NoError(t, err)
							require.Equal(t, "Neo.ClientError.Request.Invalid", code)
							if messageType == MsgPull || messageType == MsgDiscard {
								require.NoError(t, SendMessage(conn, []byte{0xB1, messageType, 0xA1, 0x81, 'n', 0xFF}))
								if messageType == MsgPull {
									sig, record, err := ReadMessage(conn)
									require.NoError(t, err)
									require.Equal(t, byte(MsgRecord), sig)
									require.Equal(t, encodePackStreamValue([]any{int64(1)}), record)
								}
								require.NoError(t, ReadSuccess(t, conn))
							} else {
								recovered := dateTimeWireQuery(t, conn, "RETURN 1", []byte{0xA0})
								require.Equal(t, encodePackStreamValue([]any{int64(1)}), recovered)
							}

						} else {
							if messageType == MsgPull {
								signature, record, err := ReadMessage(conn)
								require.NoError(t, err)
								require.Equal(t, byte(MsgRecord), signature)
								require.Equal(t, encodePackStreamValue([]any{int64(1)}), record)
							}
							require.NoError(t, ReadSuccess(t, conn))
							if messageType == MsgBegin {
								require.NoError(t, SendMessage(conn, []byte{0xB0, MsgRollback}))
								require.NoError(t, ReadSuccess(t, conn))
							}
						}
					})
				}
			}
		}
	}
}

// All PackStream structure widths use the same policy owner, and bytes or
// strings containing signature-looking data are not interpreted as structures.
func TestLegacyDateTimeNegotiationStructureWidths(t *testing.T) {
	for _, utc := range []bool{false, true} {
		for _, sig := range []byte{0x46, 0x66, 0x49, 0x69} {
			fields := encodePackStreamIntInto(nil, 4500)
			fields = encodePackStreamIntInto(fields, 42)
			if sig == 0x66 || sig == 0x69 {
				fields = encodePackStreamStringInto(fields, "Europe/Paris")
			} else {
				fields = encodePackStreamIntInto(fields, 3600)
			}
			for _, header := range [][]byte{{0xB3, sig}, {0xDC, 3, sig}, {0xDD, 0, 3, sig}} {
				wire := append(header, fields...)
				_, used, err := decodePackStreamValue(wire, 0, utc)
				if utc == (sig == 0x49 || sig == 0x69) {
					require.NoError(t, err)
					require.Equal(t, len(wire), used)
				} else {
					require.ErrorIs(t, err, errDateTimeSignature)
				}
			}
		}
		for _, wire := range [][]byte{encodePackStreamString(string([]byte{0xB3, 0x46, 0xB3, 0x49})), {0xCC, 4, 0xB3, 0x46, 0xB3, 0x49}} {
			_, used, err := decodePackStreamValue(wire, 0, utc)
			require.NoError(t, err)
			require.Equal(t, len(wire), used)
		}
	}
}
