package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBuildAtomicRecordV2_FormatAndAlignment(t *testing.T) {
	payload := []byte(`{"seq":1,"op":"create_node","data":{"id":"n1"}}`)

	record, alignedLen := buildAtomicRecordV2(payload)
	if int64(len(record)) != alignedLen {
		t.Fatalf("record length mismatch: len=%d alignedLen=%d", len(record), alignedLen)
	}
	if alignedLen%walAlignment != 0 {
		t.Fatalf("record is not %d-byte aligned: len=%d", walAlignment, alignedLen)
	}
	if len(record) < 9 {
		t.Fatalf("record too short: %d", len(record))
	}

	magic := binary.LittleEndian.Uint32(record[0:4])
	if magic != walMagic {
		t.Fatalf("magic mismatch: got=0x%x want=0x%x", magic, walMagic)
	}
	if record[4] != walFormatVersion {
		t.Fatalf("version mismatch: got=%d want=%d", record[4], walFormatVersion)
	}

	payloadLen := binary.LittleEndian.Uint32(record[5:9])
	if payloadLen != uint32(len(payload)) {
		t.Fatalf("payload length mismatch: got=%d want=%d", payloadLen, len(payload))
	}

	payloadStart := 9
	payloadEnd := payloadStart + int(payloadLen)
	if payloadEnd+4+8 > len(record) {
		t.Fatalf("record too short for payload+crc+trailer: end=%d len=%d", payloadEnd+4+8, len(record))
	}

	gotPayload := record[payloadStart:payloadEnd]
	if string(gotPayload) != string(payload) {
		t.Fatalf("payload mismatch: got=%q want=%q", string(gotPayload), string(payload))
	}

	storedCRC := binary.LittleEndian.Uint32(record[payloadEnd : payloadEnd+4])
	computedCRC := crc32Checksum(payload)
	if storedCRC != computedCRC {
		t.Fatalf("crc mismatch: got=0x%x want=0x%x", storedCRC, computedCRC)
	}

	storedTrailer := binary.LittleEndian.Uint64(record[payloadEnd+4 : payloadEnd+4+8])
	if storedTrailer != walTrailer {
		t.Fatalf("trailer mismatch: got=0x%x want=0x%x", storedTrailer, walTrailer)
	}
}

func TestWriteAtomicRecordV2_MatchesBuild(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 1024+3) // include non-aligned size

	want, wantLen := buildAtomicRecordV2(payload)

	var buf bytes.Buffer
	gotLen, err := writeAtomicRecordV2(&buf, payload)
	if err != nil {
		t.Fatalf("writeAtomicRecordV2 failed: %v", err)
	}
	got := buf.Bytes()

	if gotLen != wantLen {
		t.Fatalf("aligned len mismatch: got=%d want=%d", gotLen, wantLen)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("record bytes mismatch")
	}
}
