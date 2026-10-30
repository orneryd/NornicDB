// Author: Codex R17 implementation agent, 29 October 2026.
package antlr

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// inspection is the compatibility text boundary. It resolves just this request
// in the retained tree; admitted readers carry their node instead. Repeated
// spellings at different extents remain ambiguous, even if another source root
// would have a unique match. No map of the statement's other windows is built.
func (root *retainedSyntaxRoot) inspection(key syntaxInspectionKey, call bool) (syntaxInspection, bool) {
	if key.source == "" {
		return syntaxInspection{}, false
	}
	source := root.data.view.Original
	var selected syntaxInspection
	found := false
	for from := 0; from <= len(source)-len(key.source); {
		at := strings.Index(source[from:], key.source)
		if at < 0 {
			break
		}
		start := from + at
		view, exact := root.inspectionAt(root.data.root, start, start+len(key.source), key.entry, call)
		if exact {
			if found {
				selected.ambiguous = true
				return selected, true
			}
			selected, found = view, true
		}
		from = start + 1
	}
	return selected, found
}

// inspectionAt follows only the subtree containing the requested start. CST
// preorder preserves the former outermost-rule choice at a shared extent.
func (root *retainedSyntaxRoot) inspectionAt(id, start, end int, entry SyntaxEntry, call bool) (syntaxInspection, bool) {
	data := root.data
	node := data.nodes[id]
	if node.rule < 0 || node.start < 0 || node.end > len(data.source) || node.end <= node.start {
		return syntaxInspection{}, false
	}
	span, _ := data.view.OriginalSpan(cyphertext.Span{Start: node.start, End: node.end}, cyphertext.SourceExtent)
	if start < span.Start || start >= span.End {
		return syntaxInspection{}, false
	}
	if exactSpan, exact := data.view.OriginalSpan(cyphertext.Span{Start: node.start, End: node.end}, cyphertext.SourceExactBoundaries); exact && exactSpan.Start == start {
		view := syntaxInspection{data: data, node: id, start: start, end: end, codeStart: node.start, codeEnd: node.end}
		if call {
			if entry == fragmentEntry && root.callWindowEnd(id, end, &view) {
				return view, true
			}
		} else if exactSpan.End == end {
			if entry == fragmentEntry {
				return view, true
			}
			if ownEntry, owned := inspectionEntry(node.rule); owned && ownEntry == entry {
				return view, true
			}
		}
	}
	for _, child := range node.children {
		if view, found := root.inspectionAt(child, start, end, entry, call); found {
			return view, true
		}
	}
	return syntaxInspection{}, false
}

// CALL compatibility inputs include their known continuation. Only the same
// grammar-owned containers that supplied that continuation may set its end.
func (root *retainedSyntaxRoot) callWindowEnd(id, originalEnd int, view *syntaxInspection) bool {
	data := root.data
	node := data.nodes[id]
	if node.rule != CypherParserRULE_callSubquery && node.rule != CypherParserRULE_queryCallSt && node.rule != CypherParserRULE_standaloneCall {
		return false
	}
	for parent := id; parent >= 0; parent = data.nodes[parent].parent {
		container := data.nodes[parent]
		switch container.rule {
		case CypherParserRULE_callSubquery:
			if parent != id {
				continue
			}
		case CypherParserRULE_script, CypherParserRULE_query, CypherParserRULE_regularQuery, CypherParserRULE_singleQuery, CypherParserRULE_subqueryBody, CypherParserRULE_queryCallSt, CypherParserRULE_standaloneCall:
		default:
			continue
		}
		end := container.end
		if parent == data.root {
			end = len(data.source)
		}
		if end < node.end || end > len(data.source) {
			continue
		}
		end = node.start + len(strings.TrimSpace(data.source[node.start:end]))
		span, exact := data.view.OriginalSpan(cyphertext.Span{Start: node.start, End: end}, cyphertext.SourceExactBoundaries)
		if exact && span.Start == view.start && span.End == originalEnd {
			view.codeEnd = end
			return true
		}
	}
	return false
}
