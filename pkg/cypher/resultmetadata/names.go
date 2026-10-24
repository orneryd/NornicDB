// Package resultmetadata carries read-only names across query metadata consumers.
// Unknown names have no payload; a known empty scope remains distinct.
package resultmetadata

import "context"

// Provider supplies previously established result columns without analysis.
// Implementations must preserve unknown on a cache miss.
type Provider interface {
	CachedResultColumns(context.Context, string, []string) Names
}

// Names is an opaque known-or-unknown sequence of projected names. Its zero
// value means unknown. Visit requires an explicit policy for both outcomes.
type Names struct {
	values []string
	known  bool
}

// Known returns immutable, authoritative names in the supplied order.
func Known(names []string) Names {
	return Names{values: append([]string{}, names...), known: true}
}

// Unknown returns names whose projection has not been established.
func Unknown() Names { return Names{} }

// Visit exposes a fresh copy only when names are known. Unknown has no readable
// names and cannot be used as a known empty scope or a UNION comparison operand.
func (n Names) Visit(known func([]string), unknown func()) {
	if n.known {
		known(append([]string{}, n.values...))
	} else {
		unknown()
	}
}

// Projection carries execution's parsed expressions and their authoritative
// names together. Names remains opaque when the projection cannot be read.
type Projection struct {
	Expressions []string
	Names       Names
	Standalone  bool
	Distinct    bool
	Modifiers   string
	// PassThrough means every expression is a variable projected under its
	// own name. Literal words such as true are distinguished by syntax.
	PassThrough bool
}

// ProjectionProvider supplies the canonical single-clause projection reader.
// This compiles projection syntax, never a statement scope or semantic walk.
type ProjectionProvider interface {
	ResultProjection(string, []string) Projection
}
