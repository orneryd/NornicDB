// NOR-160 supported-value caller-copy contract. Restored verbatim in intent from
// the approved original head refs/codex/reference/pr-1688-20261104 (e1c1aa6f7)
// over de70c0e83; original author: NOR-160 lane (Codex), 1 November 2026. Ported
// only to the context-first API of release b098f7e708b4c701a743ffb0310a9b179a84c8f2.
//
// Red/green evidence: the current-release reproduction that this file replaces
// (pkg/storage/nor160_supported_value_copy_public_test.go at
// 5a88e3a816d116e9321c30b10524705682320e80286013f8f7f47e495c58980c) substituted
// storage.OwnedPropertyValue, the durable normalization hook, for this
// caller-copy protocol. Those are different contracts: durable normalization and
// generic caller-copy delivery must not share a hook. Its recorded result
// (sig/readiness-nor160/nor160-current-release.txt) is the fail-first baseline
// for the probes below: typed-map and pointer-record came back as
// map[string]interface {}, and the custom scalar's copy hook was never consulted.
//
// The probes assert only the approved caller-copy protocol: one dispatch per
// selected value, also inside typed lists, concrete type and pointer shape
// preserved, and every returned reference detached from the source.
package storage_test

import (
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor160TypedMap is a caller-declared map type, so a structural copy must keep
// the concrete type instead of routing it through a codec representation.
type nor160TypedMap map[string][]int64

// nor160ReferenceRecord is a caller-owned record reached through a pointer.
type nor160ReferenceRecord struct {
	Items []int64
	Attrs nor160TypedMap
}

// nor160ScalarCopy is a caller-declared scalar with its own copy protocol: the
// original probe's shape, a registered scalar built on int64. Neither its Go
// width nor a durable scalar hook may displace its CallerCopy dispatch.
type nor160ScalarCopy int64

var nor160ScalarCalls atomic.Int64

func (v nor160ScalarCopy) CallerCopy() any { nor160ScalarCalls.Add(1); return v }

func TestNOR160PublicEntityCopyUsesTheSharedValueOwner(t *testing.T) {
	for _, route := range []string{"node", "edge"} {
		for _, kind := range []string{"typed-map", "pointer-record", "custom-scalar"} {
			t.Run(route+"/"+kind, func(t *testing.T) {
				typed := nor160TypedMap{"values": {1, 2}}
				record := &nor160ReferenceRecord{Items: []int64{3}, Attrs: nor160TypedMap{"record": {4}}}
				properties := map[string]any{}
				switch kind {
				case "typed-map":
					properties["value"] = typed
				case "pointer-record":
					properties["value"] = record
				default:
					properties["value"] = []nor160ScalarCopy{5, 6}
				}
				var copied map[string]any
				nor160ScalarCalls.Store(0)
				if route == "node" {
					copied = storage.CopyNode(&storage.Node{ID: "test:owner", Properties: properties}).Properties
				} else {
					copied = storage.CopyEdge(&storage.Edge{ID: "test:owner", Properties: properties}).Properties
				}
				switch kind {
				case "typed-map":
					require.IsType(t, typed, copied["value"], "structural preservation must not replace a typed map with a codec representation")
					copied["value"].(nor160TypedMap)["values"][0] = 10
					require.Equal(t, int64(1), typed["values"][0])
				case "pointer-record":
					require.IsType(t, record, copied["value"], "a caller-owned record keeps its pointer and concrete type")
					copied["value"].(*nor160ReferenceRecord).Items[0] = 11
					copied["value"].(*nor160ReferenceRecord).Attrs["record"][0] = 12
					require.Equal(t, int64(3), record.Items[0])
					require.Equal(t, int64(4), record.Attrs["record"][0])
				default:
					require.Equal(t, int64(2), nor160ScalarCalls.Load(), "one custom-copy dispatch per selected scalar, also inside typed lists")
				}
			})
		}
	}
}
