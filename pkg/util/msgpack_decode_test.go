package util

import (
	"os"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestDecodeMsgpackBytesDecodesDatabaseStateWithoutArbitrarySizePolicy(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]string{"content": "database state"})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var out map[string]string
	if err := DecodeMsgpackBytes(payload, &out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out["content"] != "database state" {
		t.Fatalf("unexpected decode result: %#v", out)
	}
}

func TestDecodeMsgpackFileDecodesDatabaseStateWithoutArbitrarySizePolicy(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "msgpack-*.bin")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	defer tmp.Close()

	payload, err := msgpack.Marshal(map[string]string{"content": "persisted search index state"})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		t.Fatalf("seek failed: %v", err)
	}

	var out map[string]string
	if err := DecodeMsgpackFile(tmp, &out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out["content"] != "persisted search index state" {
		t.Fatalf("unexpected decode result: %#v", out)
	}
}

func TestDecodeMsgpackFileRejectsNilFile(t *testing.T) {
	var out map[string]string
	if err := DecodeMsgpackFile(nil, &out); err == nil {
		t.Fatal("expected nil file to fail")
	}
}
