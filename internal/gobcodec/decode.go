// Package gobcodec reads historical gob data for storage migration and recovery.
package gobcodec

import (
	"bufio"
	"encoding/gob"
	"errors"
	"io"
)

// TrailingPolicy selects whether bytes after the first gob value are allowed.
type TrailingPolicy uint8

const (
	// AllowTrailing preserves the historical stored-body decoding contract.
	AllowTrailing TrailingPolicy = iota
	// RejectTrailing requires the input stream to end after the decoded value.
	RejectTrailing
)

// ErrTrailing reports bytes after a decoded historical gob value.
var ErrTrailing = errors.New("trailing historical gob payload")

// Decode reads a historical gob value, enforcing the selected framing policy.
func Decode(source io.Reader, value any, policy TrailingPolicy) error {
	if policy != AllowTrailing && policy != RejectTrailing {
		return errors.New("invalid historical gob framing policy")
	}
	if policy == AllowTrailing {
		return gob.NewDecoder(source).Decode(value)
	}
	// Use the input's byte reader directly when possible. Besides avoiding a
	// buffer allocation, this keeps the decoder from reading past its frame.
	reader, ok := source.(interface {
		io.Reader
		io.ByteReader
	})
	if !ok {
		reader = bufio.NewReader(source)
	}
	if err := gob.NewDecoder(reader).Decode(value); err != nil {
		return err
	}
	if _, err := reader.ReadByte(); err == nil {
		return ErrTrailing
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
