// Author: R17 implementation lane. Views retain real grammar nodes, not a
// manufactured Script/Query hierarchy or a second grammar acquisition.
package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"sync"
)

type syntaxWindow struct {
	root, afterNode int
	start, end      int
	first, after    int
}

type syntaxWindowRef struct {
	data     *statementSyntaxData
	window   syntaxWindow
	once     sync.Once
	retained *statementSyntaxData
}

func (ref *syntaxWindowRef) materialize() *statementSyntaxData {
	ref.once.Do(func() {
		data := *ref.data
		data.window = &ref.window
		data.credentials = nil
		for _, credential := range ref.data.credentials {
			if credential.Start >= ref.window.start && credential.End <= ref.window.end {
				data.credentials = append(data.credentials, credential)
			}
		}
		data.effects = (StatementSyntax{&data}).collectEffects()
		ref.retained = &data
	})
	return ref.retained
}

func (data *statementSyntaxData) windowStart() int {
	if data.window != nil {
		return data.window.start
	}
	return 0
}

func (data *statementSyntaxData) originalWindow() string {
	if window := data.window; window != nil {
		return data.view.Original[window.start:window.end]
	}
	return data.view.Original
}

func (data *statementSyntaxData) includesNode(id int) bool {
	if data.window == nil {
		return true
	}
	window := data.window
	node := data.nodes[id]
	return id >= window.root && id < window.afterNode && node.end > window.start && node.start < window.end
}

// projectionItemView is a cache-only role, never a parser entry.
const projectionItemView SyntaxEntry = 255

// indexSyntaxWindows is called under the existing syntax cache's write lock.
// Only successfully parsed, coordinate-identical source is indexed. Raw-source
// normalization and failed grammar refinement keep their existing ingress.
// Equal source windows share the original immutable node and token arenas.
func indexSyntaxWindows(data *statementSyntaxData, cypher25 bool, stage SourceStage) {
	if data.window != nil || data.source != data.view.Original {
		return
	}
	for id, node := range data.nodes {
		var entries []SyntaxEntry
		switch node.rule {
		case CypherParserRULE_query, CypherParserRULE_regularQuery,
			CypherParserRULE_singleQuery, CypherParserRULE_subqueryBody:
			entries = []SyntaxEntry{FragmentEntry, StatementEntry, ClauseEntry}
		case CypherParserRULE_matchSt, CypherParserRULE_withSt,
			CypherParserRULE_returnSt, CypherParserRULE_unwindSt,
			CypherParserRULE_callSubquery, CypherParserRULE_queryCallSt,
			CypherParserRULE_standaloneCall, CypherParserRULE_createSt,
			CypherParserRULE_mergeSt, CypherParserRULE_deleteSt,
			CypherParserRULE_setSt, CypherParserRULE_removeSt,
			CypherParserRULE_foreachSt:
			entries = []SyntaxEntry{FragmentEntry, StatementEntry, ClauseEntry}
		case CypherParserRULE_pattern:
			entries = []SyntaxEntry{PatternEntry}
		case CypherParserRULE_projectionItem:
			entries = []SyntaxEntry{projectionItemView}
		case -1:
			if node.firstToken >= 0 && data.tokens[node.firstToken].kind == CypherLexerMULT && node.parent >= 0 && data.nodes[node.parent].rule == CypherParserRULE_projectionItems {
				entries = []SyntaxEntry{projectionItemView}
			} else {
				continue
			}
		default:
			continue
		}
		if node.end <= node.start || node.firstToken < 0 {
			continue
		}
		root := id
		if parent := node.parent; parent >= 0 && (data.nodes[parent].rule == CypherParserRULE_updatingStatement || data.nodes[parent].rule == CypherParserRULE_readingStatement) && data.nodes[parent].firstToken == node.firstToken && data.nodes[parent].afterToken == node.afterToken {
			root = parent
		}
		afterNode := root + 1
		for afterNode < len(data.nodes) && data.nodes[afterNode].parent >= root {
			afterNode++
		}
		ref := &syntaxWindowRef{data: data, window: syntaxWindow{
			root: root, afterNode: afterNode, start: node.start, end: node.end,
			first: node.firstToken, after: node.afterToken,
		}}
		if node.rule == CypherParserRULE_callSubquery {
			call := SyntaxNode{syntaxView[syntaxNode]{data, id}}
			if open, found := call.DirectToken(CypherLexerLBRACE); found {
				_, end := open.Span()
				header := data.source[node.start:end]
				key := statementSyntaxKey{source: header, cypher25: cypher25, stage: stage}
				if statementSyntaxCache.callHeaders[key] == nil {
					// A scope reader consumes only the admitted header's roles. Its body
					// and irrelevant supplied tail cannot widen this grammar acquisition.
					statementSyntaxCache.callHeaders[key] = &syntaxWindowRef{data: data, window: syntaxWindow{root: id, afterNode: afterNode, start: node.start, end: node.end, first: node.firstToken, after: node.afterToken}}
				}
			}
		}
		text := data.source[node.start:node.end]
		for _, entry := range entries {
			key := statementSyntaxKey{source: text, entry: entry, cypher25: cypher25, stage: stage}
			if statementSyntaxCache.views[key] == nil {
				statementSyntaxCache.views[key] = ref
			}
		}
		// Execution and CALL readers also receive a clause suffix. Retain its
		// actual enclosing query node and project only the written clause interval.
		// No grammar node or role is manufactured for that source window.
		if len(entries) > 0 && entries[0] == FragmentEntry {
			for parent := node.parent; parent >= 0; parent = data.nodes[parent].parent {
				container := data.nodes[parent]
				if container.rule != CypherParserRULE_singleQuery && container.rule != CypherParserRULE_subqueryBody {
					continue
				}
				if container.end <= node.end {
					continue
				}
				tail := strings.TrimRightFunc(data.source[node.start:container.end], func(r rune) bool { return r == ' ' || r == '\n' || r == '\r' || r == '\t' })
				last := node.start + len(tail)
				after := container.afterToken
				for after > node.firstToken && data.tokens[after-1].start >= last {
					after--
				}
				afterNode := parent + 1
				for afterNode < len(data.nodes) && data.nodes[afterNode].parent >= parent {
					afterNode++
				}
				suffix := &syntaxWindowRef{data: data, window: syntaxWindow{root: parent, afterNode: afterNode, start: node.start, end: last, first: node.firstToken, after: after}}
				for _, entry := range []SyntaxEntry{FragmentEntry, StatementEntry, ClauseEntry} {
					key := statementSyntaxKey{source: tail, entry: entry, cypher25: cypher25, stage: stage}
					if statementSyntaxCache.views[key] == nil {
						statementSyntaxCache.views[key] = suffix
					}
				}
				break
			}
		}
	}
}

// RetainedProjectionItem returns an already parsed projection role. It never
// constructs a RETURN statement or acquires a grammar on a cache miss. A
// standalone projection adapter retains its existing distinct ingress.
func RetainedProjectionItem(ctx context.Context, text string, cypher25 bool) (SyntaxNode, bool) {
	if ctx.Err() != nil {
		return SyntaxNode{}, false
	}
	key := statementSyntaxKey{source: text, entry: projectionItemView, cypher25: cypher25, stage: AcceptedSource}
	statementSyntaxCache.RLock()
	ref := statementSyntaxCache.views[key]
	statementSyntaxCache.RUnlock()
	if ref == nil {
		return SyntaxNode{}, false
	}
	return (StatementSyntax{ref.materialize()}).Root(), true
}

// SyntaxOrigin identifies both the shared arena and the selected extent.
// Node-ID products can only be reused for the same origin.
type SyntaxOrigin struct {
	arena            *syntaxNodeData
	root, start, end int
}

// Origin returns the immutable grammar arena and source extent of this view.
func (syntax StatementSyntax) Origin() SyntaxOrigin {
	if !syntax.Valid() || len(syntax.data.nodes) == 0 {
		return SyntaxOrigin{}
	}
	return SyntaxOrigin{&syntax.data.nodes[0], syntax.Root().id, syntax.data.windowStart(), syntax.data.windowStart() + len(syntax.Source())}
}

// Contains reports whether this view owns the node's arena and full extent.
func (syntax StatementSyntax) Contains(node SyntaxNode) bool {
	if !syntax.Valid() || !node.Valid() || len(syntax.data.nodes) == 0 || len(node.data.nodes) == 0 || &syntax.data.nodes[0] != &node.data.nodes[0] {
		return false
	}
	start, end := node.Span()
	start += node.data.windowStart()
	end += node.data.windowStart()
	return syntax.data.includesNode(node.id) && start >= syntax.data.windowStart() && end <= syntax.data.windowStart()+len(syntax.Source())
}

// RetainedCallScope selects an already admitted CALL header and its real scope
// node. It does not validate the supplied body/tail and never acquires grammar.
// Leading trivia contributes only an offset; names and roles come from the CST.
func RetainedCallScope(ctx context.Context, source string, cypher25 bool) (SyntaxNode, int, bool) {
	if ctx.Err() != nil {
		return SyntaxNode{}, 0, false
	}
	text := strings.TrimLeftFunc(source, cyphertext.IsWhitespace)
	leading := len(source) - len(text)
	statementSyntaxCache.RLock()
	var found *syntaxWindowRef
	for key, ref := range statementSyntaxCache.callHeaders {
		if key.stage == AcceptedSource && key.cypher25 == cypher25 && strings.HasPrefix(text, key.source) {
			found = ref
			break
		}
	}
	statementSyntaxCache.RUnlock()
	if found == nil {
		return SyntaxNode{}, 0, false
	}
	return (StatementSyntax{found.materialize()}).Root(), leading, true
}
