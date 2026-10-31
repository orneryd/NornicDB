// Author: Codex, NOR-160 caller-owned retained-image results, 29 October 2026.
package storage

import "github.com/orneryd/nornicdb/pkg/callerowned"

// copyPropertyValue detaches a caller-visible property or metadata value
// through the single structural ownership implementation (pkg/callerowned).
// Durable representation/number normalization is a separate owner.
func copyPropertyValue(value any) any {
	return callerowned.Value(value)
}

// CallerCopy lets rows, results and pages that carry nodes detach them through
// the same traversal as their other values.
func (n *Node) CallerCopy() any { return copyNode(n) }

// CallerCopy lets rows, results and pages that carry edges detach them through
// the same traversal as their other values.
func (e *Edge) CallerCopy() any { return copyEdge(e) }
