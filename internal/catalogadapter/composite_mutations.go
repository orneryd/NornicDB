// Package catalogadapter owns the Cypher-facing conversion of catalog inputs
// shared by protocol adapters. Catalog validation and publication remain in multidb.
package catalogadapter

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

// Catalog supplies composite input conversion to protocol adapters.
// Embed a value initialized with the protocol's manager; the surrounding adapter
// retains its own protocol-specific lifecycle operations.
//
//	adapter := Catalog{Manager: manager}
//	err := adapter.CreateCompositeDatabase(ctx, "catalog", constituents)
type Catalog struct {
	Manager *multidb.DatabaseManager
}

// CreateCompositeDatabase converts all inputs before publishing the catalog.
// Invalid input retains its original indexed diagnostic and cannot partially
// create a composite.
func (a Catalog) CreateCompositeDatabase(ctx context.Context, name string, constituents []interface{}) error {
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, input := range constituents {
		ref, ok := ConstituentRef(input, NewConstituentInput)
		if !ok {
			return fmt.Errorf("invalid constituent type at index %d", i)
		}
		refs[i] = ref
	}
	return a.Manager.CreateCompositeDatabase(ctx, name, refs)
}

// AddConstituent converts one constituent and delegates its catalog transition
// with the caller's context unchanged.
func (a Catalog) AddConstituent(ctx context.Context, name string, input interface{}) error {
	ref, ok := ConstituentRef(input, NewConstituentInput)
	if !ok {
		return fmt.Errorf("invalid constituent type")
	}
	return a.Manager.AddConstituent(ctx, name, ref)
}

// ConstituentPurpose distinguishes external mutation maps from catalog records.
type ConstituentPurpose uint8

const (
	// NewConstituentInput excludes catalog-owned sealed state from input maps.
	NewConstituentInput ConstituentPurpose = iota
	// StoredConstituentRecord retains sealed state when adapting catalog views.
	StoredConstituentRecord
)

// ConstituentRef adapts either an already typed reference or a protocol map.
// A typed reference retains its exact existing meaning; map conversion exposes
// catalog-owned ConnectionV1 only for stored-record consumers.
func ConstituentRef(input interface{}, purpose ConstituentPurpose) (multidb.ConstituentRef, bool) {
	switch value := input.(type) {
	case multidb.ConstituentRef:
		return value, true
	case map[string]interface{}:
		ref := multidb.ConstituentRef{
			Alias:        stringField(value, "alias"),
			DatabaseName: stringField(value, "database_name"),
			Type:         stringField(value, "type"),
			AccessMode:   stringField(value, "access_mode"),
			URI:          stringField(value, "uri"),
			SecretRef:    stringField(value, "secret_ref"),
			AuthMode:     stringField(value, "auth_mode"),
			User:         stringField(value, "user"),
			Password:     stringField(value, "password"),
		}
		if purpose == StoredConstituentRecord {
			ref.ConnectionV1 = stringField(value, "connection_v1")
		}
		return ref, true
	default:
		return multidb.ConstituentRef{}, false
	}
}

func stringField(value map[string]interface{}, name string) string {
	text, _ := value[name].(string)
	return text
}
