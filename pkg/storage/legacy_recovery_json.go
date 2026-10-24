package storage

import (
	"fmt"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"io"
)

// Legacy records do not describe the original Go numeric type. Preserve their
// numeric lexemes rather than passing integer properties through float64 first.
// An integer lexeme uses int64 (or uint64 for larger positive values); fractional/exponent
// lexemes remain floating point. New typed recovery records retain exact types.
func decodeLegacyRecoveryJSON(reader io.Reader, target any) error {
	decoder := valueconvert.PropertyJSON.NewDecoder(reader)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected extra legacy recovery JSON value")
		}
		return err
	}
	return nil
}
