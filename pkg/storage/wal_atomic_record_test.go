package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestWriteAtomicRecordV2_MatchesBuild(t *testing.T) {
	t.Run("format and alignment", func(t *testing.T) {
		payload := []byte(`{"seq":1,"op":"create_node","data":{"id":"n1"}}`)

		record, alignedLen := buildAtomicRecord(payload, walFormatVersionCurrent)
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
		if record[4] != walFormatVersionCurrent {
			t.Fatalf("version mismatch: got=%d want=%d", record[4], walFormatVersionCurrent)
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

	})
	t.Run("writer matches builder", func(t *testing.T) {
		payload := bytes.Repeat([]byte{0xAB}, 1024+3) // include non-aligned size

		want, wantLen := buildAtomicRecord(payload, walFormatVersionCurrent)

		var buf bytes.Buffer
		gotLen, err := writeAtomicRecord(&buf, payload, walFormatVersionCurrent)
		if err != nil {
			t.Fatalf("writeAtomicRecord failed: %v", err)
		}
		got := buf.Bytes()

		if gotLen != wantLen {
			t.Fatalf("aligned len mismatch: got=%d want=%d", gotLen, wantLen)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("record bytes mismatch")
		}

	})
}

type shortWriter struct {
	limit int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.limit <= 0 {
		return 0, nil
	}
	if len(p) > w.limit {
		return w.limit, nil
	}
	return len(p), nil
}

type errWriter struct {
	err error
}

func (w *errWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

func TestWriteAtomicRecordV2Bufio_ErrorBranches(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64)

	t.Run("flush failure is returned", func(t *testing.T) {
		bw := bufio.NewWriterSize(&errWriter{err: errors.New("flush fail")}, 16)
		_, err := writeAtomicRecordBufio(bw, payload, walFormatVersionCurrent)
		if err == nil {
			t.Fatalf("expected flush error")
		}
	})

	t.Run("short write returns io.ErrShortWrite", func(t *testing.T) {
		_, err := writeAtomicRecordBufio(&shortWriter{limit: 4}, payload, walFormatVersionCurrent)
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("expected short write error, got %v", err)
		}
	})

	t.Run("writer error is returned", func(t *testing.T) {
		_, err := writeAtomicRecordBufio(&errWriter{err: errors.New("write fail")}, payload, walFormatVersionCurrent)
		if err == nil {
			t.Fatalf("expected write error")
		}
	})
}
