package util

import (
	"bytes"
	"fmt"
	"os"

	"github.com/vmihailenco/msgpack/v5"
)

// DecodeMsgpackBytes decodes database-owned MessagePack state from memory.
func DecodeMsgpackBytes(data []byte, dst any) error {
	return msgpack.NewDecoder(bytes.NewReader(data)).Decode(dst)
}

// DecodeMsgpackFile decodes database-owned MessagePack state from a file.
func DecodeMsgpackFile(file *os.File, dst any) error {
	if file == nil {
		return fmt.Errorf("cannot decode msgpack from a nil file")
	}
	return msgpack.NewDecoder(file).Decode(dst)
}
