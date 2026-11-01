package storage

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"io"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// Legacy records do not describe the original Go numeric type. Their property
// numbers take the one property-number decision (convert.PropertyJSON): an
// integer lexeme uses int64 (or uint64 for larger positive values), fractional
// or exponent lexemes remain floating point. New typed recovery records retain
// exact types.
func decodeLegacyRecoveryJSON(reader io.Reader, target any) error {
	decoder := jsontext.NewDecoder(reader)
	var raw jsontext.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected extra legacy recovery JSON value")
		}
		return err
	}
	if err := decodeLegacyRecoveryRecord(raw, target); err != nil {
		return err
	}
	var firstErr error
	normalize := func(properties map[string]any) {
		for key, value := range properties {
			normalized, err := valueconvert.PropertyJSON.Numbers(value)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			properties[key] = normalized
		}
	}
	transformRecoveryEntities(target, func(node *Node) *Node {
		if node != nil {
			normalize(node.Properties)
			normalize(node.EmbedMeta)
		}
		return node
	}, func(edge *Edge) *Edge {
		if edge != nil {
			normalize(edge.Properties)
		}
		return edge
	})
	return firstErr
}
