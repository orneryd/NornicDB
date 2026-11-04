package util

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// This is a cache threshold, not an input limit. The decoder keeps scratch
// buffers after Reset, so large payloads use the existing single-use path.
const maxPooledMsgpackPayload = 64 << 10

type msgpackDecodeState struct {
	reader  bytes.Reader
	decoder msgpack.Decoder
}

// Own the state instead of borrowing the process-wide library pool: failed
// RawMessage/custom decoding can leave its raw-copy recorder set, which Reset
// does not clear. Only our successful, bounded reads can return to this pool.
var msgpackDecodeStates = sync.Pool{New: func() any { return new(msgpackDecodeState) }}

// DecodeMsgpackBytes decodes database-owned MessagePack state from memory.
// Small successful reads reuse a private cursor and decoder. Failed decodes
// discard their state; large payloads still decode without a size restriction.
func DecodeMsgpackBytes(data []byte, dst any) error {
	if len(data) > maxPooledMsgpackPayload {
		return msgpack.NewDecoder(bytes.NewReader(data)).Decode(dst)
	}
	state := msgpackDecodeStates.Get().(*msgpackDecodeState)
	state.reader.Reset(data)
	state.decoder.Reset(&state.reader)
	err := state.decoder.Decode(dst)
	state.reader.Reset(nil)
	state.decoder.Reset(nil)
	if err == nil {
		msgpackDecodeStates.Put(state)
	}
	return err
}

// DecodeMsgpackFile decodes database-owned MessagePack state from a file.
func DecodeMsgpackFile(file *os.File, dst any) error {
	if file == nil {
		return fmt.Errorf("cannot decode msgpack from a nil file")
	}
	return msgpack.NewDecoder(file).Decode(dst)
}
