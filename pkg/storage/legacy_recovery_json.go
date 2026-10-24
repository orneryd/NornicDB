package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Legacy records do not describe the original Go numeric type. Preserve their
// numeric lexemes rather than passing integer properties through float64 first.
// An integer lexeme uses int64 (or uint64 for larger positive values); fractional/exponent
// lexemes remain floating point. New typed recovery records retain exact types.
func decodeLegacyRecoveryJSON(reader io.Reader, target any) error {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
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
	var firstErr error
	normalize := func(properties map[string]any) {
		for key, value := range properties {
			normalized, err := legacyRecoveryNumber(value)
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
	if firstErr == nil {
		canonicalizeDecodedPropertyContainers(target)
	}
	return firstErr
}

func legacyRecoveryNumber(value any) (any, error) {
	switch v := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(v), ".eE") {
			integer, err := v.Int64()
			if err == nil {
				return integer, nil
			}
			unsigned, unsignedErr := strconv.ParseUint(string(v), 10, 64)
			if unsignedErr != nil {
				return nil, fmt.Errorf("legacy recovery integer %q is outside the 64-bit integer range: %w", v, err)
			}
			return unsigned, nil
		}
		return v.Float64()
	case []any:
		for i, item := range v {
			normalized, err := legacyRecoveryNumber(item)
			if err != nil {
				return nil, err
			}
			v[i] = normalized
		}
	case map[string]any:
		for key, item := range v {
			normalized, err := legacyRecoveryNumber(item)
			if err != nil {
				return nil, err
			}
			v[key] = normalized
		}
	}
	return value, nil
}
