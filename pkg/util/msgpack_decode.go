package util

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/vmihailenco/msgpack/v5"
)

// DecodeMsgpackBytes decodes database-owned MessagePack state from memory.
func DecodeMsgpackBytes(data []byte, dst any) error {
	return DecodeMsgpackBytesWithContext(nil, data, dst)
}

// DecodeMsgpackBytesWithContext uses the same database decoder while carrying
// cancellation through actual scalar, element and byte reads.
func DecodeMsgpackBytesWithContext(ctx context.Context, data []byte, dst any) error {
	return msgpack.NewDecoder(ReaderWithContext(ctx, bytes.NewReader(data))).Decode(dst)
}

// ReaderWithContext preserves a byte scanner when present, so decoding an
// in-memory value checks cancellation without buffering an excluded payload.
// The reader shape is bound once; reads perform no capability discovery.
func ReaderWithContext(ctx context.Context, reader io.Reader) io.Reader {
	if ctx == nil {
		return reader
	}
	base := contextReader{ctx: ctx, reader: reader}
	if scanner, ok := reader.(io.ByteScanner); ok {
		return &contextByteScanner{contextReader: base, scanner: scanner}
	}
	return &base
}

// WriterWithContext carries cancellation through actual encoder writes. A nil
// context retains the ordinary writer, just as ReaderWithContext does.
func WriterWithContext(ctx context.Context, writer io.Writer) io.Writer {
	if ctx == nil {
		return writer
	}
	return &contextWriter{ctx: ctx, writer: writer}
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w *contextWriter) Write(value []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(value)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(value []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(value)
}

type contextByteScanner struct {
	contextReader
	scanner io.ByteScanner
}

func (r *contextByteScanner) ReadByte() (byte, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.scanner.ReadByte()
}
func (r *contextByteScanner) UnreadByte() error { return r.scanner.UnreadByte() }

// DecodeMsgpackFile decodes database-owned MessagePack state from a file.
func DecodeMsgpackFile(file *os.File, dst any) error {
	if file == nil {
		return fmt.Errorf("cannot decode msgpack from a nil file")
	}
	return msgpack.NewDecoder(file).Decode(dst)
}
