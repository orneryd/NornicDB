package storage

// Hunt ground H10 (decoders and parsers), targets 6 and 8 of 8:
//
//	FuzzHuntH10WALPayload       journal record payload decoding
//	                            (processWALEntryPayload, current frame version):
//	                            no panic, no hang; an accepted record encodes again
//	                            and decodes to the same record
//	FuzzSnapshotValueIdentity   (existing repository target, snapshot value
//	                            codec: byte-for-byte round trip) is target 8
//
// Run: go test ./pkg/storage -run '^$' -fuzz '^FuzzHuntH10WALPayload$' -fuzztime 30m -parallel 1

import (
	"bytes"
	"testing"
	"time"
)

func FuzzHuntH10WALPayload(f *testing.F) {
	for _, entry := range []WALEntry{
		{Sequence: 1, Timestamp: time.Unix(1700000000, 0).UTC(), Operation: OpBufferedCommit, Data: []byte{1, 2, 3}, Database: "nornic"},
		{Sequence: 2, Timestamp: time.Unix(1700000001, 0).UTC(), Operation: OpBufferedCommit, Data: nil, Database: ""},
		{Sequence: 1 << 40, Timestamp: time.Unix(0, 0).UTC(), Operation: OpBufferedCommit, Data: bytes.Repeat([]byte{0xff}, 300), Database: "x"},
	} {
		entry := entry
		entry.Checksum = crc32Checksum(entry.Data)
		var buf bytes.Buffer
		payload, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(append([]byte(nil), payload...))
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		var decoded WALEntry
		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err = processWALEntryPayload(walPayloadDecode, nil, &decoded, payload, walFormatVersionCurrent)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("hang decoding %x", payload)
		}
		if err != nil {
			return
		}
		var buf bytes.Buffer
		again, err := processWALEntryPayload(walPayloadEncode, &buf, &decoded, nil, 0)
		if err != nil {
			return // a decoded record the writer would refuse (size, fields) is not a round-trip claim
		}
		var second WALEntry
		if _, err := processWALEntryPayload(walPayloadDecode, nil, &second, again, walFormatVersionCurrent); err != nil {
			t.Fatalf("record accepted from %x does not decode after encoding: %v", payload, err)
		}
		if second.Sequence != decoded.Sequence || second.Operation != decoded.Operation || second.Database != decoded.Database ||
			second.Checksum != decoded.Checksum || !bytes.Equal(second.Data, decoded.Data) || !second.Timestamp.Equal(decoded.Timestamp) {
			t.Fatalf("round trip differs for input %x:\nfirst  %+v\nsecond %+v", payload, decoded, second)
		}
	})
}
