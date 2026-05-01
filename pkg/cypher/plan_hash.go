// Package cypher: D-04b deterministic plan-tree hash for slow-query log + Phase 6 span attribution.
//
// PlanHash returns a 16-character lowercase hex digest of the bound execution
// plan computed via FNV-1a 64-bit (stdlib hash/fnv). The canonical form walks
// PlanOperator.OperatorType, .Description, .Identifiers, .Arguments, and
// .Children; argument values are restricted to a stable known type set
// (string|int64|float64|bool) — see W4 below — so the hash is deterministic
// across map iteration order, Go versions, and process restarts.
//
// Why fixed 16-char hex?
//   - 64-bit FNV-1a is non-cryptographic but stable across stdlib versions.
//   - 16 hex chars is enough collision resistance for plan-class fingerprinting
//     (~4e9 plans before 50% collision probability via the birthday bound).
//   - Phase 6 (TRC-04) will reuse this exact function for the
//     nornicdb.cypher.plan span attribute; operators correlate slow-query log
//     records with traces via plan_hash equality.
//
// W4 canonical-form pin (Pitfall 8): Arguments map values are restricted to
// string|int64|float64|bool. Other types contribute a single 0x00 nil byte
// to the canonical form — see the type switch default branch comment marked
// "TODO: PlanHash arg type expansion". Phase 2 ships these four types because
// they are what the executor actually emits; future expansion (e.g.,
// time.Duration) requires an explicit canonical-form change AND a new
// TestPlanHash_Stability golden value to flag the schema bump.
package cypher

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// Sentinel separator bytes prevent canonical-form collisions via concatenation.
// All four are byte values that cannot appear in valid UTF-8 string content
// (they are all >0x7F continuation bytes used out-of-context, which any sane
// Operator/Description/Identifier producer never emits).
const (
	planSepField = 0xff // between top-level fields (op/desc/idents/args/children)
	planSepID    = 0xfe // between identifier list elements
	planSepKV    = 0xfd // between argument key and value
	planSepChild = 0xfc // between sibling child operators
)

// Argument-type prefix bytes. Restricting Arguments map values to these four
// types is the W4 stability contract; new types require a canonical-form
// change AND golden-test update.
const (
	planArgNil    byte = 0x00 // unsupported type — see W4 / TODO below
	planArgString byte = 0x01
	planArgInt64  byte = 0x02
	planArgFloat  byte = 0x03
	planArgBool   byte = 0x04
)

// PlanHash returns the 16-char hex FNV-1a digest of the canonical form of plan.
//
// Nil safety: PlanHash(nil) and PlanHash(&ExecutionPlan{Root: nil}) both
// return "0000000000000000" — the zero placeholder operators expect when the
// executor emits a slow-query log without a populated plan tree
// (e.g., normal-mode queries that don't run EXPLAIN/PROFILE).
func PlanHash(plan *ExecutionPlan) string {
	if plan == nil || plan.Root == nil {
		return "0000000000000000"
	}
	h := fnv.New64a()
	canonicalizePlan(h, plan.Root)
	return fmt.Sprintf("%016x", h.Sum64())
}

// canonicalizePlan writes a deterministic byte representation of op into h.
// Field order is locked: OperatorType | Description | Identifiers | Arguments | Children.
// Argument map iteration is stabilized via sorted keys; values use a typed
// prefix byte to defeat type ambiguity.
func canonicalizePlan(h interface{ Write([]byte) (int, error) }, op *PlanOperator) {
	if op == nil {
		return
	}

	h.Write([]byte(op.OperatorType))
	h.Write([]byte{planSepField})

	h.Write([]byte(op.Description))
	h.Write([]byte{planSepField})

	for _, id := range op.Identifiers {
		h.Write([]byte(id))
		h.Write([]byte{planSepID})
	}
	h.Write([]byte{planSepField})

	if len(op.Arguments) > 0 {
		keys := make([]string, 0, len(op.Arguments))
		for k := range op.Arguments {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.Write([]byte(k))
			h.Write([]byte{planSepKV})
			writeArgValue(h, op.Arguments[k])
			h.Write([]byte{planSepID})
		}
	}
	h.Write([]byte{planSepField})

	for _, child := range op.Children {
		canonicalizePlan(h, child)
		h.Write([]byte{planSepChild})
	}
}

// writeArgValue serializes a single argument value to h using a typed prefix
// byte. W4: only string, int64, float64, bool are supported — other types
// contribute a single planArgNil byte (no value bytes) so unsupported-type
// arguments are equivalent for hash purposes regardless of their concrete
// type. This is the intentional stability contract; expanding the type set
// requires a canonical-form bump.
func writeArgValue(h interface{ Write([]byte) (int, error) }, v interface{}) {
	switch x := v.(type) {
	case string:
		h.Write([]byte{planArgString})
		h.Write([]byte(x))
	case int64:
		h.Write([]byte{planArgInt64})
		var buf [8]byte
		u := uint64(x)
		buf[0] = byte(u)
		buf[1] = byte(u >> 8)
		buf[2] = byte(u >> 16)
		buf[3] = byte(u >> 24)
		buf[4] = byte(u >> 32)
		buf[5] = byte(u >> 40)
		buf[6] = byte(u >> 48)
		buf[7] = byte(u >> 56)
		h.Write(buf[:])
	case float64:
		h.Write([]byte{planArgFloat})
		// fmt-based formatting keeps stable across Go versions; %x of bits would
		// also work but %v is consistent with the stable arg-type contract.
		fmt.Fprintf(writeAdapter{h}, "%v", x)
	case bool:
		h.Write([]byte{planArgBool})
		if x {
			h.Write([]byte{0x01})
		} else {
			h.Write([]byte{0x00})
		}
	default:
		// TODO: PlanHash arg type expansion — see W4 in 02-03-PLAN.
		// Unsupported types collapse to a nil contribution so the hash remains
		// stable when the executor adds a new plan-arg type before Phase 2 has
		// a chance to formalize it. A canonical-form bump is required to
		// distinguish among unsupported types.
		h.Write([]byte{planArgNil})
	}
}

// writeAdapter satisfies io.Writer for fmt.Fprintf without importing io
// (avoids interface bloat in the hot serialization loop).
type writeAdapter struct {
	h interface{ Write([]byte) (int, error) }
}

func (w writeAdapter) Write(p []byte) (int, error) { return w.h.Write(p) }
