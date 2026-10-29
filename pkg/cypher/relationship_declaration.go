package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// The shared syntax owner seals the decoded type and its quoted/dynamic facts
// below both parser adapters and storage. These nominal wrappers preserve the
// executor's semantic API without copying or reconstructing type vectors.
type relationshipTypeFacts cyphertext.RelationshipType
type relationshipTypes cyphertext.RelationshipTypes

func (types relationshipTypes) len() int { return cyphertext.RelationshipTypes(types).Len() }
func (types relationshipTypes) at(index int) relationshipTypeFacts {
	return relationshipTypeFacts(cyphertext.RelationshipTypes(types).At(index))
}
func (value relationshipTypeFacts) storageName() string {
	return cyphertext.RelationshipType(value).StorageName()
}
func (value relationshipTypeFacts) isDynamic() bool {
	return cyphertext.RelationshipType(value).IsDynamic()
}
func (value relationshipTypeFacts) isQuoted() bool {
	return cyphertext.RelationshipType(value).IsQuoted()
}
func (value relationshipTypeFacts) matchesStorageName(name string) bool {
	return cyphertext.RelationshipType(value).MatchesStorageName(name)
}
func (types relationshipTypes) hasDynamic() bool {
	for index := 0; index < types.len(); index++ {
		if types.at(index).isDynamic() {
			return true
		}
	}
	return false
}
func (types relationshipTypes) contains(name string) bool {
	for index := 0; index < types.len(); index++ {
		if types.at(index).matchesStorageName(name) {
			return true
		}
	}
	return false
}
func (types relationshipTypes) matches(name string) bool {
	return types.len() == 0 || types.contains(name)
}
func (types relationshipTypes) firstOccurrence(index int) bool {
	value := types.at(index)
	for previous := 0; previous < index; previous++ {
		other := types.at(previous)
		if other.storageName() == value.storageName() && other.isDynamic() == value.isDynamic() {
			return false
		}
	}
	return true
}
func (types relationshipTypes) soleUpperNameIs(name string) bool {
	return types.len() == 1 && !types.at(0).isDynamic() && upperASCII(types.at(0).storageName()) == name
}
func (types relationshipTypes) same(other relationshipTypes) bool {
	if types.len() != other.len() {
		return false
	}
	for index := 0; index < types.len(); index++ {
		left, right := types.at(index), other.at(index)
		if left.storageName() != right.storageName() || left.isDynamic() != right.isDynamic() {
			return false
		}
	}
	return true
}
func (types relationshipTypes) bytes() int { return cyphertext.RelationshipTypes(types).Bytes() }
func (types relationshipTypes) storageNames() []string {
	if types.len() == 0 {
		return nil
	}
	names := make([]string, types.len())
	for index := range names {
		names[index] = types.at(index).storageName()
	}
	return names
}

// relationshipHop is the common admission shape for degree and lookup-index
// shortcuts. Only the declaration owner opens a relationship's brackets.
type relationshipHop struct {
	from, to            string
	declaration         relationshipDeclaration
	incoming, outgoing  bool
	bracketed, complete bool
	toStart, end        int
}

func readRelationshipHop(text string) (hop relationshipHop, ok bool) {
	hop, ok = readRelationshipHopPrefix(text)
	return hop, ok && !(hop.incoming && hop.outgoing) && queryGapEnd(text, hop.end) == len(text)
}

// relationshipFrame is an opaque connection boundary, before interpreting its
// declaration. Syntax-only consumers never need to resolve a dynamic token.
type relationshipFrame struct {
	detailOpen, detailClose, end, outgoingAt int
	incoming, outgoing, bracketed, complete  bool
}

type relationshipHopFrame struct {
	fromOpen, fromClose, toOpen, toClose int
	connection                           relationshipFrame
}

// containsRelationshipPattern classifies graph frames through the owner and
// the canonical quote/comment reader, including inside nested expressions.
func containsRelationshipPattern(expression string) bool {
	for index := 0; index < len(expression); index++ {
		switch c := expression[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, c) - 1
		case '/':
			if end := queryCommentEnd(expression, index); end >= 0 {
				index = end - 1
			}
		case '(':
			if _, present := relationshipChainEnd(expression, index, len(expression)); present {
				return true
			}
		}
	}
	return false
}

type relationshipExpressionShape struct{ pattern, brace bool }

// Expression programs retain the structural facts they need. A graph
// expression needs no materialized copy of its masked declaration text.
func relationshipExpressionReaderText(expression string) relationshipExpressionShape {
	if containsRelationshipPattern(expression) {
		_, facts := readRelationshipNodeText(expression, false)
		return relationshipExpressionShape{pattern: true, brace: facts.brace}
	}
	return relationshipExpressionShape{brace: strings.Contains(expression, "{")}
}

func relationshipDelimiterEnd(text string, open int, opener, closer rune, bounds map[int]int) int {
	if bounds != nil {
		if end, ok := bounds[open]; ok {
			return end
		}
		return -1
	}
	return cyphertext.FindMatchingDelimiter(text, open, opener, closer, cyphertext.OpeningDelimiter)
}

func readRelationshipFrame(text string, start int, bounds map[int]int) (relationshipFrame, bool) {
	owned, ok := cyphertext.ReadConnectionFrame(text, start, bounds)
	open, close := owned.DetailBounds()
	return relationshipFrame{detailOpen: open, detailClose: close, end: owned.End(), outgoingAt: owned.OutgoingAt(), incoming: owned.Incoming(), outgoing: owned.Outgoing(), bracketed: owned.Bracketed(), complete: owned.Complete()}, ok
}

func relationshipConnectionOf(text string, frame relationshipFrame) relationshipHop {
	hop := relationshipHop{incoming: frame.incoming, outgoing: frame.outgoing, bracketed: frame.bracketed, complete: frame.complete, end: frame.end}
	if frame.bracketed {
		hop.declaration = relationshipDeclarationOf(text[frame.detailOpen+1 : frame.detailClose])
	} else {
		hop.declaration = relationshipDeclarationHead("")
	}
	return hop
}

// Bare declarations remain valid for the legacy declaration adapter. Complete
// hop/chain callers additionally require both connecting dashes and endpoints.
func readRelationshipConnection(text string, start int) (relationshipHop, bool) {
	frame, ok := readRelationshipFrame(text, start, nil)
	if !ok {
		return relationshipHop{}, false
	}
	return relationshipConnectionOf(text, frame), true
}

// A quantified group is a path endpoint too. Leave invalid attachments to the
// path validator, rather than changing their error into a scalar arrow error.
func relationshipConnectionStartAfterNode(text string, open, close int) int {
	at := queryGapEnd(text, close+1)
	inner := queryGapEnd(text, open+1)
	if inner < close && text[inner] == '(' {
		if quantifier, ok := relationshipQuantifierAt(text, at, len(text)); ok {
			at = queryGapEnd(text, quantifier.end)
		}
	}
	if at >= len(text) || text[at] != '-' && text[at] != '<' && text[at] != '[' {
		return -1
	}
	return at
}
func relationshipFrameAfterNode(text string, open, close int, bounds map[int]int) (relationshipFrame, bool) {
	at := relationshipConnectionStartAfterNode(text, open, close)
	if at < 0 {
		return relationshipFrame{}, false
	}
	return readRelationshipFrame(text, at, bounds)
}

func relationshipPrefixAfterNode(text string, open, close int) (cyphertext.ConnectionPrefix, bool) {
	at := relationshipConnectionStartAfterNode(text, open, close)
	if at < 0 {
		var prefix cyphertext.ConnectionPrefix
		return prefix, false
	}
	return cyphertext.ReadConnectionPrefix(text, at)
}

func readRelationshipHopFrame(text string, start int, bounds map[int]int) (shape relationshipHopFrame, ok bool) {
	shape.fromOpen = queryGapEnd(text, start)
	if shape.fromOpen >= len(text) || text[shape.fromOpen] != '(' {
		return shape, false
	}
	shape.fromClose = relationshipDelimiterEnd(text, shape.fromOpen, '(', ')', bounds)
	if shape.fromClose < 0 {
		return shape, false
	}
	connection, valid := relationshipFrameAfterNode(text, shape.fromOpen, shape.fromClose, bounds)
	if !valid || !connection.complete {
		return shape, false
	}
	shape.connection, shape.toOpen = connection, connection.end
	if shape.toOpen >= len(text) || text[shape.toOpen] != '(' {
		return shape, false
	}
	shape.toClose = relationshipDelimiterEnd(text, shape.toOpen, '(', ')', bounds)
	return shape, shape.toClose >= 0
}

func readRelationshipHopPrefix(text string) (relationshipHop, bool) {
	shape, ok := readRelationshipHopFrame(text, 0, nil)
	if !ok {
		return relationshipHop{}, false
	}
	hop := relationshipConnectionOf(text, shape.connection)
	hop.from = strings.TrimSpace(text[shape.fromOpen+1 : shape.fromClose])
	hop.to = strings.TrimSpace(text[shape.toOpen+1 : shape.toClose])
	hop.toStart, hop.end = shape.toOpen, shape.toClose+1
	return hop, true
}

// invalidRelationshipArrow validates frame membership using the same connection
// reader. Cached balanced boundaries keep nested expression scopes linear.
func invalidRelationshipArrow(query string) int {
	live := false
	for index := 0; index+1 < len(query); index++ {
		switch c := query[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(query, index, c) - 1
		case '/':
			if end := queryCommentEnd(query, index); end >= 0 {
				index = end - 1
			}
		case '-':
			if query[index+1] == '>' {
				live = true
			}
		}
		if live {
			break
		}
	}
	if !live {
		return -1
	}
	context := newMapPatternContext(query)
	if context.unbalanced {
		// An incomplete frame cannot classify arrows. The selected validator
		// owns the delimiter error and must report it before membership.
		return -1
	}
	bounds := context.closes
	var inline [16]int
	arrows := inline[:0]
	for index := 0; index < len(query); index++ {
		switch c := query[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(query, index, c) - 1
		case '/':
			if end := queryCommentEnd(query, index); end >= 0 {
				index = end - 1
			}
		case '(':
			if close, closed := bounds[index]; closed {
				frame, ok := relationshipFrameAfterNode(query, index, close, bounds)
				if !ok || !frame.complete {
					continue
				}
				arrow := frame.outgoingAt
				if arrow >= 0 && arrow+1 < len(query) && query[arrow+1] == '>' {
					arrows = append(arrows, arrow)
				}
			}
		case '-':
			if index+1 >= len(query) || query[index+1] != '>' {
				continue
			}
			if len(arrows) == 0 || arrows[len(arrows)-1] != index {
				return index
			}
			arrows = arrows[:len(arrows)-1]
		}
	}
	return -1
}

func relationshipChainEnd(q string, open, end int) (int, bool) {
	close := cyphertext.FindMatchingDelimiter(q[:end], open, '(', ')', cyphertext.OpeningDelimiter)
	if close < 0 {
		return 0, false
	}
	inner := skipASCIISpaces(q, open+1, close)
	if inner < close && q[inner] == '(' {
		return 0, false
	}
	chainEnd, relationships := close+1, 0
	for from := open; from < end; {
		shape, ok := readRelationshipHopFrame(q[from:end], 0, nil)
		if !ok {
			break
		}
		chainEnd = from + shape.toClose + 1
		relationships++
		from += shape.toOpen
	}
	return chainEnd, relationships > 0
}

func relationshipLookupPattern(text string) bool {
	end, ok := keywordSpanAt(text, 0, "FOR")
	if !ok {
		return false
	}
	text = strings.TrimSpace(text[end:])
	on := topLevelKeywordIndex(text, "ON")
	if on < 0 {
		return false
	}
	hop, ok := readRelationshipHop(text[:on])
	d := hop.declaration
	if !ok || hop.from != "" || hop.to != "" || !isPatternVariableName(d.variable) || d.hasColon || d.hasLength || d.lengthInvalid || d.properties != "" || d.predicate != "" {
		return false
	}
	end, ok = keywordSpanAt(text, on, "ON EACH")
	if !ok {
		return false
	}
	call := strings.TrimSpace(text[end:])
	name, rest, ok := parseIdentifierToken(call)
	if !ok || !strings.EqualFold(name, "type") {
		return false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "(") || findMatchingParen(rest, 0) != len(rest)-1 {
		return false
	}
	variable := strings.TrimSpace(rest[1 : len(rest)-1])
	return isPatternVariableName(variable) && symbolicNameValue(variable) == symbolicNameValue(d.variable)
}

// ignoresDynamicType is decided before resolution/validation of a token value.
// Neo4j 5.26.30 ignores that value on legacy * ranges, including empty strings,
// nulls and odd parameter values. Single-hop and quantified forms still resolve.
func (d relationshipDeclaration) ignoresDynamicType(chain labelChain, mode labelPatternMode) bool {
	return d.hasLength && mode == labelPatternMatch && chain.dynamic && chain.expr.kind == labelExpressionDynamic
}

// ignoredDynamicTypeStaticError is Neo4j's check of a value a legacy range
// ignores: it is never evaluated, but its type, a parameter's from its value,
// must still name a type.
func (d relationshipDeclaration) ignoredDynamicTypeStaticError(chain labelChain, params map[string]interface{}) error {
	expression := strings.TrimSpace(chain.expr.expression)
	return dynamicLabelTypeError(typedOperand((staticTypeScope{params: params}).staticExpressionType(expression)))
}

type relationshipRewriteFacts struct{ present, predicate, properties, mapReads, labels, repeated, brace bool }

// Only this owner constructs the text accepted by node/list prefilters.
// Relationship interiors have already been consumed and masked in this type.
type relationshipNodeText string

// patternElement never exposes a relationship's source text. Structural
// iteration consumes its declaration before handing the element to a reader.
type patternElement struct {
	node           string
	relationship   relationshipDeclaration
	isRelationship bool
	start, end     int
}

func eachPatternElement(pattern string, visit func(patternElement), cache ...*relationshipDeclarationCache) {
	for index := 0; index < len(pattern); index++ {
		switch cypherLexicalByte(pattern[index]) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(pattern, index, pattern[index]) - 1
		case '(', '[':
			closer := ')'
			if pattern[index] == '[' {
				closer = ']'
			}
			closing := cyphertext.FindMatchingDelimiter(pattern, index, rune(pattern[index]), closer, cyphertext.OpeningDelimiter)
			if closing < 0 {
				return
			}
			if pattern[index] == '[' {
				var declaration relationshipDeclaration
				if len(cache) > 0 {
					declaration = cache[0].readDeclaration(pattern[index+1 : closing])
				} else {
					declaration = relationshipDeclarationOf(pattern[index+1 : closing])
				}
				visit(patternElement{relationship: declaration, isRelationship: true, start: index, end: closing})
			} else {
				visit(patternElement{node: pattern[index : closing+1], start: index, end: closing})
			}
			index = closing
		}
	}
}

// relationshipNodeReaderText removes relationship interiors before a scanner
// for node/list/expression syntax runs. Its relationship facts come exclusively
// from the declaration; no prefilter gets a second interpretation of that text.
func relationshipNodeReaderText(query string, cache ...*relationshipDeclarationCache) (relationshipNodeText, relationshipRewriteFacts) {
	text, facts := readRelationshipNodeText(query, true, cache...)
	return relationshipNodeText(text), facts
}

// Both materialized prefilters and fact-only callers use this one traversal.
// A brace contributes only outside an owner-consumed relationship interior.
func readRelationshipNodeText(query string, materialize bool, cache ...*relationshipDeclarationCache) (string, relationshipRewriteFacts) {
	var facts relationshipRewriteFacts
	var masked strings.Builder
	last := 0
	var names [8]string
	count := 0
	for open, close := nextRelationshipBracketInExpressions(query, 0); open >= 0; open, close = nextRelationshipBracketInExpressions(query, close+1) {
		facts.brace = facts.brace || strings.Contains(query[last:open+1], "{")
		var d relationshipDeclaration
		if len(cache) > 0 {
			d = cache[0].readDeclaration(query[open+1 : close])
		} else {
			d = relationshipDeclarationHead(query[open+1 : close])
		}
		facts.present = true
		facts.predicate = facts.predicate || d.predicate != ""
		facts.properties = facts.properties || d.properties != ""
		facts.mapReads = facts.mapReads || indexOutsideQuotes(d.properties, '.') >= 0 || indexOutsideQuotes(d.properties, '(') >= 0
		facts.labels = facts.labels || d.needsRewrite
		name := symbolicNameValue(d.variable)
		if isPatternVariableName(d.variable) {
			if count == len(names) {
				facts.repeated = true
			} else {
				for _, earlier := range names[:count] {
					facts.repeated = facts.repeated || earlier == name
				}
				names[count] = name
				count++
			}
		}
		if materialize {
			if last == 0 {
				masked.Grow(len(query))
			}
			masked.WriteString(query[last : open+1])
			for i := open + 1; i < close; i++ {
				masked.WriteByte(' ')
			}
		}
		last = close
	}
	facts.brace = facts.brace || strings.Contains(query[last:], "{")
	if last == 0 {
		return query, facts
	}
	if !materialize {
		return "", facts
	}
	masked.WriteString(query[last:])
	return masked.String(), facts
}

// A relationship pattern's inside ([r:A|B *1..2 {k: v}], [r:A WHERE p]) is
// read by one scanner wherever a statement uses it: CREATE, MERGE, MATCH,
// the clause rewrites, the fast-path plans, the write-stretch analysis and
// schema statements (relationshipDeclarationOf, and
// relationshipDeclarationHead for readers that need no type list): a
// backtick-quoted type may hold any character, ':', '|', '*', '{' and
// spaces included, and a doubled backtick inside it is one backtick, as
// labels are read (eachChainItem, #907). A dynamic type is an expression:
// a '{', '*' or '..' inside $(...) ($({k: t}.k), $(['A', 'B'][i * 1]),
// $(ts[0..1][0])) is neither the property map nor a length.

// relationshipDeclaration is a relationship pattern's inside, read outside
// backticks.
type relationshipDeclaration struct {
	// Source offsets belong to this owner too; rewriters never scan the inside.
	typeOffset, chainOffset, variableEnd, whereOffset int
	dynamic, alternatives, conservativeType           bool
	needsRewrite                                      bool
	viaIS                                             bool
	// variable is the relationship variable as written, trimmed ("" when
	// there is none).
	variable string
	// hasColon is set when the declaration names types (r:T, :T).
	hasColon bool
	// typeText is the text after the colon and before any length, as
	// written: a dynamic type $(e) is resolved from it per row.
	typeText string
	// types are the type names in order, backtick-quoted ones unquoted
	// (`a``b` is a`b); a dynamic $(e) item is kept as written.
	types relationshipTypes
	// invalidType is the first type as written that names no type: an
	// unquoted one that isn't an identifier (a name Neo4j can't parse
	// unquoted) or an empty quoted one (``); "" when there is none.
	invalidType string
	emptyType   bool
	// hasLength is set for a variable length (*, *2, *1..3, before or after
	// the types); length is its digits and dots after the *.
	hasLength bool
	rangeText string
	// lengthInvalid is set when the length is malformed: '..' without a
	// '*', or anything but digits and at most one '..' after the '*'
	// (MATCH rejects it). Quoted text and expressions don't count (#879).
	lengthInvalid bool
	// properties is the property map, from its { to the end, trimmed, or a
	// trailing parameter ($props); "" when there is none.
	properties string
	// predicate is the element's own WHERE predicate ([r:T WHERE r.k = 1]),
	// the text after WHERE, trimmed; "" when there is none.
	predicate string
}

// relationshipDeclarationCache belongs to one statement rewrite. Facts are
// reused across admission, binding, and rewriting within one execution.
type relationshipDeclarationCache struct {
	declarationFirst                    cachedRelationshipDeclaration
	declarationPresent                  bool
	declarationRest                     []cachedRelationshipDeclaration
	declarationOverflow                 map[string]relationshipDeclaration
	repetitionKnown, repetitionPossible bool
}

func (cache *relationshipDeclarationCache) rememberRewriteFacts(facts relationshipRewriteFacts) {
	cache.repetitionKnown, cache.repetitionPossible = true, facts.repeated
}

func (cache *relationshipDeclarationCache) mayRepeatVariables() bool {
	return !cache.repetitionKnown || cache.repetitionPossible
}

type cachedRelationshipDeclaration struct {
	sourceInterior string
	declaration    relationshipDeclaration
}

func (cache *relationshipDeclarationCache) readDeclaration(text string) relationshipDeclaration {
	if cache.declarationPresent && cache.declarationFirst.sourceInterior == text {
		return cache.declarationFirst.declaration
	}
	for _, entry := range cache.declarationRest {
		if entry.sourceInterior == text {
			return entry.declaration
		}
	}
	if declaration, ok := cache.declarationOverflow[text]; ok {
		return declaration
	}
	declaration := relationshipDeclarationHead(text)
	if !cache.declarationPresent {
		cache.declarationFirst = cachedRelationshipDeclaration{sourceInterior: text, declaration: declaration}
		cache.declarationPresent = true
	} else if len(cache.declarationRest) < 7 {
		cache.declarationRest = append(cache.declarationRest, cachedRelationshipDeclaration{sourceInterior: text, declaration: declaration})
	} else {
		// Keep lookup work bounded for statements with many declarations.
		if cache.declarationOverflow == nil {
			cache.declarationOverflow = make(map[string]relationshipDeclaration)
		}
		cache.declarationOverflow[text] = declaration
	}
	return declaration
}

// relationshipDeclarationOf reads a relationship pattern's inside (the text
// between [ and ]). Types are separated by | (or by a second colon, the
// deprecated [:A|:B] form); empty items are skipped.
func relationshipDeclarationOf(inner string) relationshipDeclaration {
	entry, cached := relationshipDeclarationEntryOf(inner)
	if entry.typed {
		return entry.full
	}
	entry.full, entry.typed = entry.head, true
	if entry.head.hasColon {
		types, invalid, empty, alternatives := cyphertext.ReadRelationshipTypes(entry.head.typeText)
		entry.full.types = relationshipTypes(types)
		entry.full.invalidType, entry.full.emptyType, entry.full.alternatives = invalid, empty, alternatives
	}
	if cached {
		relationshipDeclarationFacts.put(strings.Clone(inner), entry)
	}
	return entry.full
}

// relationshipDeclarationHead reads a relationship pattern's inside as
// relationshipDeclarationOf does, without the type list (types and
// invalidType stay empty): the variable, the type text, the length and the
// property map. MATCH's range check and variable-length variables read it.
func relationshipDeclarationHead(inner string) relationshipDeclaration {
	entry, _ := relationshipDeclarationEntryOf(inner)
	return entry.head
}

// Declaration facts are independent of rows and parameters, and every field
// is an immutable string, scalar or owner-built type list, so consumers share
// one entry per inside by copy: the head, and the typed declaration once a
// consumer has read the type list. Row traversal reads both for every row.
type relationshipDeclarationEntry struct {
	head, full relationshipDeclaration
	typed      bool
}

var relationshipDeclarationFacts = newBoundedCache[string, relationshipDeclarationEntry](4096, boundedCacheStringShardIndex)

// relationshipDeclarationEntryOf returns the retained entry for inner, or a
// new one holding its head; cacheable reports whether inner may be retained.
func relationshipDeclarationEntryOf(inner string) (entry relationshipDeclarationEntry, cacheable bool) {
	if len(inner) > 1024 {
		return relationshipDeclarationEntry{head: readRelationshipDeclarationHead(inner)}, false
	}
	if entry, ok := relationshipDeclarationFacts.get(inner); ok {
		return entry, true
	}
	// The retained facts are slices of their own copy of inner, never of
	// the caller's statement.
	owned := strings.Clone(inner)
	entry = relationshipDeclarationEntry{head: readRelationshipDeclarationHead(owned)}
	relationshipDeclarationFacts.put(owned, entry)
	return entry, true
}

func readRelationshipDeclarationHead(inner string) (declaration relationshipDeclaration) {
	declaration = relationshipDeclaration{whereOffset: -1, typeOffset: -1}
	leading := queryGapEnd(inner, 0)
	head := strings.TrimSpace(inner[leading:])
	// A complete symbolic name, including IS and literal-like keywords, is
	// already the variable. It has no type/range/map/predicate to scan again.
	if written, end, ok := scanSymbolicName(head, 0); ok && queryGapEnd(head, end) == len(head) {
		declaration.variable = written
		declaration.variableEnd = leading + end
		return declaration
	}
	// The WHERE predicate ends the inside; it is an expression, so nothing in
	// it is the type, the length or the property map.
	if strings.IndexAny(head, "Ww") >= 0 {
		if where := elementWhereIndex(head, -1, len(head)); where >= 0 {
			declaration.whereOffset = leading + where
			declaration.predicate = strings.TrimSpace(head[where+len("WHERE"):])
			head = strings.TrimSpace(head[:where])
		}
	}
	masked := blankRelationshipExpressions(head)
	if brace := strings.IndexByte(masked, '{'); brace >= 0 {
		declaration.properties = strings.TrimSpace(head[brace:])
		head, masked = head[:brace], masked[:brace]
	}
	declaration.lengthInvalid = relationshipLengthInvalid(masked)
	if declaration.properties == "" {
		if rest, parameter, ok := splitPatternParameterMap(head); ok {
			declaration.properties = parameter
			head, masked = rest, masked[:len(rest)]
		}
	}
	if star := strings.IndexByte(masked, '*'); star >= 0 {
		end := star + 1
		for end < len(head) && (isDigitByte(head[end]) || head[end] == '.') {
			end++
		}
		declaration.hasLength = true
		declaration.rangeText = head[star+1 : end]
		if strings.TrimSpace(head[end:]) == "" {
			head = head[:star]
		} else {
			head = head[:star] + head[end:]
		}
		masked = blankRelationshipExpressions(head)
	}
	// The declaration marker belongs to the masked head. A colon inside a
	// dynamic map/list/parenthesized expression is part of the value, including
	// when IS spells the outer marker. Preserve source offsets for rewriters.
	colon := strings.IndexByte(masked, ':')
	if colon < 0 {
		// IS is another spelling of the type marker; its variable still
		// comes from the canonical symbolic-name reader.
		name, end, ok := scanSymbolicName(head, 0)
		marker := 0
		if ok {
			marker = queryGapEnd(head, end)
			// IS is a symbolic name until another token makes it a type
			// marker. In [is IS T], the second IS is that marker; in
			// [IS T], the first is. A lone [is] always binds a variable.
			if strings.EqualFold(name, "IS") && marker < len(head) {
				if stop, secondMarker := keywordSpanAt(head, marker, "IS"); !secondMarker || queryGapEnd(head, stop) == len(head) {
					marker = 0
				}
			}
		}
		if stop, is := keywordSpanAt(head, marker, "IS"); is && queryGapEnd(head, stop) < len(head) {
			if marker > 0 {
				declaration.variable = name
			}
			declaration.hasColon, declaration.viaIS = true, true
			declaration.needsRewrite = true
			declaration.typeText = strings.TrimSpace(head[stop:])
			declaration.dynamic = hasDynamicToken(declaration.typeText)
			declaration.conservativeType = strings.ContainsAny(declaration.typeText, "`$&!%()")
			declaration.typeOffset = leading + queryGapEnd(head, stop)
			declaration.variableEnd = leading + len(declaration.variable)
			declaration.chainOffset = declaration.variableEnd
			return declaration
		}
		declaration.variable = strings.TrimSpace(head)
		declaration.variableEnd = leading + len(declaration.variable)
		return declaration
	}
	declaration.hasColon = true
	declaration.variable = strings.TrimSpace(head[:colon])
	if written, end, ok := scanSymbolicName(head, 0); ok && queryGapEnd(head, end) == colon {
		declaration.variable = written
	}
	declaration.typeText = strings.TrimSpace(head[colon+1:])
	declaration.dynamic = hasDynamicToken(declaration.typeText)
	declaration.conservativeType = strings.ContainsAny(declaration.typeText, "`$&!%()")
	// The declaration already masked quoted names and expressions. Static
	// plain names need no label tree merely to decide whether to rewrite.
	declaration.needsRewrite = declaration.dynamic || strings.ContainsAny(masked[colon+1:], "|&!%(:")
	declaration.variableEnd = leading + len(declaration.variable)
	declaration.chainOffset = leading + strings.IndexByte(blankRelationshipExpressions(inner[leading:]), ':')
	declaration.typeOffset = queryGapEnd(inner, declaration.chainOffset+1)
	return declaration
}

type relationshipTypeChainPlan struct {
	chain labelChain
	ok    bool
}

// Share only immutable grammar facts. Dynamic values are still resolved on
// each execution, and resolveDynamic builds a new expression without changing
// this plan. The owner keeps the key and its expression text together.
var relationshipTypeChainPlans = newBoundedCache[string, relationshipTypeChainPlan](4096, boundedCacheStringShardIndex)

func (d relationshipDeclaration) typeChain() (labelChain, bool) {
	text := d.typeText
	if len(text) > 1024 {
		return scanLabelChain(text, true)
	}
	if plan, ok := relationshipTypeChainPlans.get(text); ok {
		return plan.chain, plan.ok
	}
	text = strings.Clone(text)
	chain, ok := scanLabelChain(text, true)
	if ok {
		chain.expr.freezeSchemaDependencies()
	}
	relationshipTypeChainPlans.put(text, relationshipTypeChainPlan{chain: chain, ok: ok})
	return chain, ok
}

func (d relationshipDeclaration) staticTypeName() string {
	if d.types.len() == 1 && !d.types.at(0).isDynamic() {
		return d.types.at(0).storageName()
	}
	return symbolicNameValue(d.typeText)
}

// fulltextTypes supplies decoded names; a quoted name such as A|B remains
// one type rather than being reinterpreted as union syntax by schema DDL.
func (d relationshipDeclaration) fulltextTypes() ([]string, bool) {
	if d.types.len() == 0 || d.invalidType != "" || d.emptyType || d.dynamic || d.hasLength || d.lengthInvalid || d.properties != "" || d.predicate != "" {
		return nil, false
	}
	return d.types.storageNames(), true
}

func (d relationshipDeclaration) hopBounds(unbounded int) (min, max int) {
	spec := d.rangeText
	if spec == "" {
		return 1, unbounded
	}
	if left, right, ranged := strings.Cut(spec, ".."); ranged {
		min, max = 1, unbounded
		if left != "" {
			min, _ = strconv.Atoi(left)
		}
		if right != "" {
			max, _ = strconv.Atoi(right)
		}
		return min, max
	}
	min, _ = strconv.Atoi(spec)
	return min, min
}

func (d relationshipDeclaration) unboundedLength() bool {
	return d.hasLength && (d.rangeText == "" || strings.HasSuffix(d.rangeText, ".."))
}

// relationshipLengthInvalid reports a malformed length in masked, a
// declaration's text before its property map with quoted text and
// expressions blanked (blankRelationshipExpressions): '..' without a '*',
// or anything but digits and at most one '..' after the '*'.
func relationshipLengthInvalid(masked string) bool {
	masked = strings.TrimSpace(masked)
	star := strings.IndexByte(masked, '*')
	if star < 0 {
		return strings.Contains(masked, "..")
	}
	spec := strings.TrimSpace(masked[star+1:])
	for index := 0; index < len(spec); index++ {
		if !isDigitByte(spec[index]) && spec[index] != '.' {
			return true
		}
	}
	return strings.Count(spec, "..") > 1 || strings.Contains(spec, ".") && !strings.Contains(spec, "..")
}

// blankRelationshipExpressions is text with quoted text and the inside of
// parenthesized expressions blanked, at the same offsets, so a '{', '*' or
// '..' left in it belongs to the declaration: one inside a dynamic type
// ($({k: t}.k), $(t * 2), $(ts[0..1][0])) is part of the type expression.
func blankRelationshipExpressions(text string) string {
	text = blankQuotedText(text)
	var out []byte
	for index := 0; index < len(text); index++ {
		if text[index] != '(' {
			continue
		}
		end := cyphertext.FindMatchingDelimiter(text, index, '(', ')', cyphertext.OpeningDelimiter)
		if end < 0 {
			break
		}
		if out == nil {
			out = []byte(text)
		}
		for inside := index + 1; inside < end; inside++ {
			out[inside] = ' '
		}
		index = end
	}
	if out == nil {
		return text
	}
	return string(out)
}

// singleType is the declaration's one type for CREATE and MERGE: its name,
// or the type text as written when it is dynamic or names more or fewer
// than one type (the shape validators reject those first).
func (declaration relationshipDeclaration) singleType() string {
	if declaration.types.len() == 1 && !hasDynamicToken(declaration.typeText) {
		return declaration.types.at(0).storageName()
	}
	return declaration.typeText
}

// boundOutgoingRelationship reads (a)-[inside]->(b), the shape the UNWIND
// batch plans take, into its two node variables (simple identifiers) and its
// relationship's declaration; ok is false for any other shape.
func boundOutgoingRelationship(body string) (start string, declaration relationshipDeclaration, end string, ok bool) {
	hop, valid := readRelationshipHop(body)
	if !valid || !hop.bracketed || hop.incoming || !hop.outgoing || !isSimpleIdentifier(hop.from) || !isSimpleIdentifier(hop.to) {
		return "", declaration, "", false
	}
	return hop.from, hop.declaration, hop.to, true
}

// singleStaticType is the declaration's one type when it names exactly one
// type by a plain name, with no length and no predicate: what the fast-path
// plans accept. ok is false for anything else.
func (declaration relationshipDeclaration) singleStaticType() (string, bool) {
	value, ok := declaration.singleLiteralType()
	if !ok || !isSimpleIdentifier(value.storageName()) {
		return "", false
	}
	return value.storageName(), true
}

// singleLiteralType keeps the classification for fast paths that also accept
// quoted literal names. Properties are a separate admission fact for callers.
func (declaration relationshipDeclaration) singleLiteralType() (relationshipTypeFacts, bool) {
	if declaration.types.len() != 1 || declaration.invalidType != "" || declaration.emptyType || declaration.hasLength ||
		declaration.lengthInvalid || declaration.predicate != "" || declaration.types.at(0).isDynamic() {
		return relationshipTypeFacts{}, false
	}
	return declaration.types.at(0), true
}

// propertyMapBody is the text inside the declaration's property map ({…}),
// trimmed; ok is false when it has none or a parameter map.
func (declaration relationshipDeclaration) propertyMapBody() (string, bool) {
	properties := declaration.properties
	if len(properties) < 2 || properties[0] != '{' || cyphertext.FindMatchingDelimiter(properties, 0, '{', '}', cyphertext.OpeningDelimiter) != len(properties)-1 {
		return "", false
	}
	return strings.TrimSpace(properties[1 : len(properties)-1]), true
}

// traversalPatternParts consumes the same opaque connection frames as degree,
// lookup, syntax admission and chain classification. In particular, a gap after
// ']' cannot discard the connection's direction.
type traversalRelationship struct {
	variable, properties               string
	types                              relationshipTypes
	minHops, maxHops                   int
	incoming, outgoing, variableLength bool
}

func relationshipTraversalOf(connection relationshipHop) traversalRelationship {
	d := connection.declaration
	result := traversalRelationship{variable: d.variable, properties: d.properties, types: d.types, minHops: 1, maxHops: 1, incoming: connection.incoming, outgoing: connection.outgoing, variableLength: d.hasLength}
	if d.hasLength {
		result.minHops, result.maxHops = d.hopBounds(VarLengthUnboundedMaxHops)
	}
	return result
}

func traversalPatternParts(pattern string) []traversalPart {
	var parts []traversalPart
	start := strings.IndexByte(pattern, '(')
	for start >= 0 && start < len(pattern) {
		end := findMatchingParen(pattern, start)
		if end < 0 {
			return parts
		}
		if parts == nil {
			parts = make([]traversalPart, 0, 2)
		}
		part := traversalPart{node: pattern[start+1 : end]}
		frame, ok := relationshipFrameAfterNode(pattern, start, end, nil)
		if !ok || !frame.complete || frame.end >= len(pattern) || pattern[frame.end] != '(' {
			return append(parts, part)
		}
		part.relationship = relationshipTraversalOf(relationshipConnectionOf(pattern, frame))
		part.hasRelationship = true
		parts = append(parts, part)
		start = frame.end
	}
	return parts
}

// relationshipVariableTokens is the binding inventory for identifier lowering.
// The quoted-variable pass receives declaration facts rather than inferring a
// relationship variable from neighboring punctuation. Plain declarations count
// too: their quoted references must name the same binding, even for literals.
func relationshipVariableTokens(query string) []quotedToken {
	var bindings []quotedToken
	for _, token := range scanQuotedTokens(context.Background(), query) {
		if token.relationshipBinding {
			bindings = append(bindings, token)
		}
	}
	return bindings
}

// expressionListText is a list's interior, never a relationship declaration.
// The owner classifies brackets through its connection frames. Nested lists
// in relationship properties, predicates and dynamic expressions remain lists.
type expressionListText string

func eachExpressionList(query string, visit func(expressionListText) error) error {
	var inline [16]int
	stack := inline[:0]
	relationshipOpen := -1
	for index := 0; index < len(query); index++ {
		switch c := query[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(query, index, c) - 1
		case '/':
			if end := queryCommentEnd(query, index); end >= 0 {
				index = end - 1
			}
		case '[':
			if index != relationshipOpen {
				close := findMatchingBracket(query, index)
				if close < 0 {
					return nil
				}
				if err := visit(expressionListText(query[index+1 : close])); err != nil {
					return err
				}
			}
			stack = append(stack, index)
		case '(', '{':
			stack = append(stack, index)
		case ')', ']', '}':
			if len(stack) == 0 {
				continue
			}
			open := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if c == ')' && query[open] == '(' {
				frame, ok := relationshipFrameAfterNode(query, open, index, nil)
				if ok && frame.complete && frame.bracketed {
					d := relationshipDeclarationHead(query[frame.detailOpen+1 : frame.detailClose])
					if d.variable == "" || isPatternVariableName(d.variable) {
						relationshipOpen = frame.detailOpen
					}
				}
			}
		}
	}
	return nil
}
