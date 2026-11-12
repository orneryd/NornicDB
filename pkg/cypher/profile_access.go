package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/queryprofile"
)

// A physical reader reports the logical entities/index entries it actually
// read, separately from the rows its consuming clause emits. These are not
// disk I/O or cache-miss counts. Neither a planner estimate nor final result
// cardinality can supply an access observation.
type profileAccessKey struct {
	parent  int64
	kind    string
	detail  string
	unknown bool
}

// profileAccessRecord is one request-owned access record: the identity the
// statement reported that read under, and the one counter every reader of it
// adds to through queryprofile.Access.
type profileAccessRecord struct {
	key     profileAccessKey
	counter queryprofile.ReadCounter
}

type profileAccessObservation struct {
	Parent  int64
	Kind    string
	Detail  string
	Rows    int64
	Hits    int64
	Unknown bool
}

// Access returns the counter this statement records a physical read of kind
// and detail on. It is this collector's answer to queryprofile.Access: the
// frame in ctx decides the parent, so a reader reached from a callback reports
// to the clause actually executing, and the key answers every reader of one
// read with the same counter. A statement nobody profiles has no collector and
// reaches no record.
func (c *profileRuntimeCollector) Access(ctx context.Context, kind, detail string) *queryprofile.ReadCounter {
	return &c.accessRecord(profileAccessKey{parent: profileAccessParent(ctx), kind: kind, detail: detail}).counter
}

func profileAccessParent(ctx context.Context) int64 {
	if frame, ok := profileRuntimeFrameFromContext(ctx); ok {
		return frame.id
	}
	return 0
}

// profileReadContext retains only the counter owner and immutable attribution
// ID. Storage sees its service lifetime, never the originating request context.
type profileReadContext struct {
	context.Context
	collector *profileRuntimeCollector
	parent    int64
}

func (c *profileRuntimeCollector) WithLifetime(lifetime context.Context, scope context.Context) context.Context {
	return &profileReadContext{Context: lifetime, collector: c, parent: profileAccessParent(scope)}
}

func (scope *profileReadContext) WithLifetime(lifetime context.Context, _ context.Context) context.Context {
	return &profileReadContext{Context: lifetime, collector: scope.collector, parent: scope.parent}
}

func (scope *profileReadContext) Value(key interface{}) interface{} {
	if _, own := key.(queryprofile.ObserverKey); own {
		return scope
	}
	return scope.Context.Value(key)
}

func (scope *profileReadContext) Access(_ context.Context, kind, detail string) *queryprofile.ReadCounter {
	return &scope.collector.accessRecord(profileAccessKey{parent: scope.parent, kind: kind, detail: detail}).counter
}

func (scope *profileReadContext) UnobservedRead(_ context.Context, category, name string) {
	scope.collector.unobservedRead(scope.parent, category, name)
}

func (c *profileRuntimeCollector) accessRecord(key profileAccessKey) *profileAccessRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	if record := c.accesses[key]; record != nil {
		return record
	}
	if c.accesses == nil {
		c.accesses = make(map[profileAccessKey]*profileAccessRecord)
	}
	record := &profileAccessRecord{key: key}
	c.accesses[key] = record
	c.accessOrder = append(c.accessOrder, record)
	return record
}

// UnobservedRead records a real opaque invocation in the same request-owned
// ledger as observed reads, preserving the executing parent and provenance.
func (c *profileRuntimeCollector) UnobservedRead(ctx context.Context, category, name string) {
	c.unobservedRead(profileAccessParent(ctx), category, name)
}

func (c *profileRuntimeCollector) unobservedRead(parent int64, category, name string) {
	detail := category
	if name != "" {
		detail += ": " + name
	}
	c.accessRecord(profileAccessKey{parent: parent, unknown: true, detail: detail})
}

func (c *profileRuntimeCollector) accessObservation() []profileAccessObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]profileAccessObservation, 0, len(c.accessOrder))
	for _, record := range c.accessOrder {
		rows, hits := record.counter.Values()
		out = append(out, profileAccessObservation{
			Parent: record.key.parent, Kind: record.key.kind, Detail: record.key.detail,
			Rows: rows, Hits: hits,
			Unknown: record.key.unknown,
		})
	}
	return out
}

// profileKindAllRelationshipsScan names the whole relationship population a
// PROFILE statement actually read: a token-listing procedure reads every
// relationship, since no pattern selects a type or direction.
const profileKindAllRelationshipsScan = queryprofile.KindAllRelationshipsScan
