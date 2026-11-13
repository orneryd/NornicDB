package antlr

import (
	"context"
	"sort"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// readContractBlocks reads the entries of the statement's constraint contract
// blocks with the grammar and returns their syntax diagnostics, at their
// positions in the statement.
//
// The statement's own parse reads a block as balanced tokens
// (constraintBlock): as a list of predicates it would put an expression after
// every expression of the grammar. Its entries are read here, so
// CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.age > } is a syntax error wherever
// the statement is parsed. A block nested in an entry is read with that entry.
//
// The entries are read from the statement's own tokens by the statement's own
// parser (contractEntryParser), so reading an entry costs what the entry
// holds: no text is lexed again and nothing after the entry is read.
//
// The entries a block reads as become part of tree, the statement's one parse
// tree, before anything is taken from it: each entry's requirement or
// expression is a child of its block where the entry's tokens stood, as the
// statement's parse of the entries would have made it. Everything that reads
// the statement reads that tree or the syntax built from it, so each finds
// the names, parameters and calls of an entry where it finds every other.
func (data *statementSyntaxData) readContractBlocks(ctx context.Context, tree antlr4.Tree, parser *CypherParser, tokens []antlr4.Token) ([]localization.Message, error) {
	// A contract block is what a constraint may REQUIRE (constraintRequirement):
	// a statement without the word has none, and its tree is not searched.
	required := false
	for _, token := range data.tokens {
		if token.kind == CypherLexerREQUIRE {
			required = true
			break
		}
	}
	if !required {
		return nil, nil
	}
	blocks := contractBlocksOf(tree, nil)
	if len(blocks) == 0 {
		return nil, nil
	}
	entries := &contractEntryParser{data: data, parser: parser, tokens: tokens, unready: true}
	entries.view.TokenStream = parser.GetTokenStream()
	parser.RemoveErrorListeners()
	parser.AddErrorListener(contractEntryListener{antlr4.NewDefaultErrorListener()})
	parser.SetErrorHandler(&contractEntryStrategy{DefaultErrorStrategy: antlr4.NewDefaultErrorStrategy(), entries: entries})
	// An entry is read in the full prediction mode. The fast mode reads a
	// requirement such as n.id IS UNIQUE as an expression first and then
	// reports an error of its own, so most entries would be read twice.
	parser.Interpreter.SetPredictionMode(antlr4.PredictionModeLL)
	var diagnostics []localization.Message
	for _, block := range blocks {
		failed, _, err := entries.block(ctx, block)
		if err != nil {
			return nil, err
		}
		if failed != nil {
			line, column := failed.line, failed.column
			if originalLine, originalColumn, ok := data.view.OriginalPosition(line, column); ok {
				line, column = originalLine, originalColumn
			}
			diagnostics = append(diagnostics, localization.CypherANTLRUnexpectedInput(line, column))
		}
	}
	return diagnostics, nil
}

// contractBlocksOf appends the contract blocks under node that are in no
// other block: a block in a block is read with the entry that holds it.
func contractBlocksOf(node antlr4.Tree, blocks []*ConstraintBlockContext) []*ConstraintBlockContext {
	if block, ok := node.(*ConstraintBlockContext); ok {
		return append(blocks, block)
	}
	for _, child := range node.GetChildren() {
		blocks = contractBlocksOf(child, blocks)
	}
	return blocks
}

// contractEntryParser is the statement's parser as the entries of the
// statement's contract blocks are read with it, after the statement: over the
// statement's tokens, through a view that ends where an entry is to end.
type contractEntryParser struct {
	data   *statementSyntaxData
	parser *CypherParser
	tokens []antlr4.Token
	view   contractTokenView
	// ended is the statement's token the entry being read ended before, when
	// that is before the end of its input (contractEntryStrategy), or -1.
	ended int
	// unready says that the parser has to be reset before it reads an entry:
	// it has read none yet, or its last reading ended at a syntax error.
	unready bool
}

// contractTokenView is the statement's token stream as an entry's parse reads
// it: the statement's tokens, with the end of input at the token the entry is
// to end before.
type contractTokenView struct {
	antlr4.TokenStream
	limit int
	end   *contractEntryEnd
}

func (view *contractTokenView) LT(k int) antlr4.Token {
	token := view.TokenStream.LT(k)
	if k > 0 && token != nil && token.GetTokenIndex() >= view.limit {
		return view.end
	}
	return token
}

func (view *contractTokenView) LA(k int) int {
	token := view.LT(k)
	if token == nil {
		return antlr4.TokenInvalidType
	}
	return token.GetTokenType()
}

// contractEntryEnd is the end of input an entry's parse reads at the
// statement's token the entry is to end before: it has that token's place.
type contractEntryEnd struct{ antlr4.Token }

func (contractEntryEnd) GetTokenType() int { return antlr4.TokenEOF }
func (contractEntryEnd) GetChannel() int   { return antlr4.TokenDefaultChannel }
func (end contractEntryEnd) GetStop() int  { return end.Token.GetStart() - 1 }
func (contractEntryEnd) GetText() string   { return "<EOF>" }

// contractEntryError is the first syntax error of an entry's parse: where in
// the statement's text the parser found it, and at which of the statement's
// tokens.
type contractEntryError struct{ line, column, token int }

func contractEntryErrorAt(token antlr4.Token) contractEntryError {
	return contractEntryError{line: token.GetLine(), column: token.GetColumn(), token: token.GetTokenIndex()}
}

// contractEntryStrategy ends an entry's parse where the entry ends.
//
// An entry is read with the tokens that follow it in view, because nothing
// marks its end. Its rule asks for the end of input after the requirement or
// expression it read. When another token stands there, the entry is complete
// and that token is not part of it: the parse ends there without an error,
// and the reader takes the entry to end before that token.
//
// Any other mismatched token is the entry's first syntax error, and ends the
// parse, as every error the parser reports does (contractEntryListener): the
// parser's recovery from an error reads on to the end of its input, which for
// an entry may be the rest of the block.
type contractEntryStrategy struct {
	*antlr4.DefaultErrorStrategy
	entries *contractEntryParser
}

func (strategy *contractEntryStrategy) RecoverInline(recognizer antlr4.Parser) antlr4.Token {
	current := recognizer.GetCurrentToken()
	entry := recognizer.GetParserRuleContext()
	if rule := entry.GetRuleIndex(); rule == CypherParserRULE_constraintBlockEntry || rule == CypherParserRULE_constraintBlockRequirementEntry {
		if count := entry.GetChildCount(); count > 0 {
			if body, ok := entry.GetChild(count - 1).(antlr4.ParserRuleContext); ok {
				if rule := body.GetRuleIndex(); rule == CypherParserRULE_constraintRequirement || rule == CypherParserRULE_expression {
					strategy.entries.ended = current.GetTokenIndex()
					return current
				}
			}
		}
	}
	panic(contractEntryErrorAt(current))
}

// contractEntryListener ends an entry's parse at the first syntax error the
// parser reports, with the token the parser reports it at.
type contractEntryListener struct{ *antlr4.DefaultErrorListener }

func (contractEntryListener) SyntaxError(_ antlr4.Recognizer, offending interface{}, line, column int, _ string, _ antlr4.RecognitionException) {
	failed := contractEntryError{line: line, column: column, token: -1}
	if token, ok := offending.(antlr4.Token); ok {
		failed.token = token.GetTokenIndex()
	}
	panic(failed)
}

// parse reads the statement's tokens from first as one entry that ends
// before limit at the latest: as any entry (constraintBlockEntry), or as a
// requirement alone (constraintBlockRequirementEntry). It returns the entry's
// tree and the token the entry ends before, or the entry's first syntax
// error.
func (entries *contractEntryParser) parse(first, limit int, requirement bool) (entry antlr4.ParserRuleContext, after int, failed *contractEntryError) {
	defer func() {
		if failure := recover(); failure != nil {
			syntax, ok := failure.(contractEntryError)
			if !ok {
				panic(failure)
			}
			entries.unready = true
			entry, failed = nil, &syntax
		}
	}()
	if entries.unready {
		// The parser keeps a syntax error until its rule has reported it, and
		// a reading that ended at its first error left it in that rule.
		entries.parser.SetError(nil)
		entries.parser.SetTokenStream(&entries.view)
		entries.unready = false
	}
	// Each reading has its own end of input: a tree may keep it as the first
	// token of a rule that read nothing.
	entries.view.limit, entries.view.end = limit, &contractEntryEnd{entries.tokens[limit]}
	entries.ended = -1
	entries.view.Seek(first)
	if requirement {
		entry = entries.parser.ConstraintBlockRequirementEntry()
	} else {
		entry = entries.parser.ConstraintBlockEntry()
	}
	if entries.ended >= 0 {
		return entry, entries.ended, nil
	}
	return entry, limit, nil
}

// entry reads the statement's tokens from first as one entry that ends before
// limit at the latest, and the blocks the entry holds. It returns the entry's
// tree and the token the entry ends before, or the first syntax error and the
// offset in the statement's source where the reading stopped.
func (entries *contractEntryParser) entry(ctx context.Context, first, limit int, requirement bool) (antlr4.ParserRuleContext, int, *contractEntryError, int, error) {
	entry, after, failed := entries.parse(first, limit, requirement)
	if err := ctx.Err(); err != nil {
		return nil, 0, nil, 0, err
	}
	if failed != nil {
		// An error with no token of the statement ends the reading where the
		// entry starts.
		stopped := entries.data.tokens[first].start
		if failed.token >= 0 && failed.token < len(entries.data.tokens) {
			stopped = entries.data.tokens[failed.token].start
		}
		return nil, 0, failed, stopped, nil
	}
	for _, block := range contractBlocksOf(entry, nil) {
		failed, stopped, err := entries.block(ctx, block)
		if err != nil {
			return nil, 0, nil, 0, err
		}
		if failed != nil {
			return nil, 0, failed, stopped, nil
		}
	}
	return entry, after, nil, 0, nil
}

// contractChildToken is the statement's first token a child of a block
// holds.
func contractChildToken(child antlr4.Tree) int {
	switch child := child.(type) {
	case antlr4.TerminalNode:
		return child.GetSymbol().GetTokenIndex()
	case antlr4.ParserRuleContext:
		if start := child.GetStart(); start != nil {
			return start.GetTokenIndex()
		}
	}
	return -1
}

// block reads the entries of one block of the statement's tree, and puts the
// entries it reads as into the tree in place of the block's tokens. When the
// block does not read, it returns the first syntax error of the reading that
// got farthest and the offset in the statement's source where that reading
// stopped.
func (entries *contractEntryParser) block(ctx context.Context, node *ConstraintBlockContext) (*contractEntryError, int, error) {
	data := entries.data
	children := node.GetChildren()
	if len(children) < 2 {
		return nil, 0, nil
	}
	open, opens := children[0].(antlr4.TerminalNode)
	closing, closes := children[len(children)-1].(antlr4.TerminalNode)
	if !opens || !closes || open.GetSymbol().GetTokenType() != CypherLexerLBRACE || closing.GetSymbol().GetTokenType() != CypherLexerRBRACE {
		return nil, 0, nil
	}
	first, last := open.GetSymbol().GetTokenIndex(), closing.GetSymbol().GetTokenIndex()
	if first < 0 || last >= len(data.tokens) || last <= first {
		return nil, 0, nil
	}
	block := contractBlockReading{entries: entries, end: data.tokens[last].start}
	// The block's own ';' tokens stand between its entries: the tokens of a
	// block in the block are that block's children, not this one's.
	for _, child := range children[1 : len(children)-1] {
		if token, ok := child.(antlr4.TerminalNode); ok && token.GetSymbol().GetTokenType() == CypherLexerSEMI {
			block.separators = append(block.separators, data.tokens[token.GetSymbol().GetTokenIndex()].start)
		}
	}
	read, err := block.read(ctx, data.tokens[first].end, false)
	if err != nil {
		return nil, 0, err
	}
	if !read {
		return block.failed, block.stopped, nil
	}
	if len(block.kept) == 0 {
		return nil, 0, nil
	}
	// The readings were kept last entry first. Each entry takes the place of
	// the block's own tokens it holds; the braces and the separators between
	// entries stay. An entry has no more nodes than it holds tokens, so the
	// block's children are rewritten where they are.
	count, written := len(children), 0
	entry := len(block.kept) - 1
	for index := 0; index < count; {
		child := children[index]
		token := contractChildToken(child)
		for entry >= 0 && block.kept[entry].after <= token {
			entry--
		}
		if entry < 0 || block.kept[entry].first > token {
			children[written] = child
			written++
			index++
			continue
		}
		reading := block.kept[entry]
		for index < count && contractChildToken(children[index]) < reading.after {
			index++
		}
		// The entry's wrapping rule and its end of input are not kept: the
		// block holds the requirement or expression itself.
		for _, held := range reading.tree.GetChildren() {
			if end, ok := held.(antlr4.TerminalNode); ok && end.GetSymbol().GetTokenType() == antlr4.TokenEOF {
				continue
			}
			held.SetParent(node)
			children[written] = held
			written++
		}
		entry--
	}
	for ; written < count; count-- {
		node.RemoveLastChild()
	}
	return nil, 0, nil
}

// contractEntryReading is one entry of a block that read: its parse tree, and
// the statement's tokens it holds, from first up to after.
type contractEntryReading struct {
	tree         antlr4.ParserRuleContext
	first, after int
}

// contractBlockReading is the reading of one contract block's body, which
// ends at end in the statement's source.
type contractBlockReading struct {
	entries *contractEntryParser
	end     int
	// separators holds where the block's own ';' tokens start.
	separators []int
	// unread holds the entry starts the body's rest does not read from.
	unread map[int]bool
	// required holds the entry starts a requirement before an alternative
	// was looked for at, with that alternative.
	required map[[2]int]bool
	// kept holds the entries of the division the body read as, last first.
	kept []contractEntryReading
	// failed is the first syntax error of the reading that got farthest, to
	// stopped.
	stopped int
	failed  *contractEntryError
}

// read reports whether the body's rest from at reads as entries, one after
// another with at most one ';' between two.
//
// Nothing marks where an entry ends: n.age IS :: INTEGER n.status IN ['a'] is
// two entries, though INTEGER n also names a type. An entry is therefore read
// as the longest run of tokens that reads as one constraintBlockEntry, and
// when the rest does not read after it, as the next shorter one. The body
// reads when one such division of it does, which is what the grammar's own
// list of adjacent predicates accepted.
//
// No entry holds a ';' of its block, so the longest run ends at the block's
// next one at the latest. An entry that is complete before that ends where
// the token that cannot be its next one stands, and a reading that fails
// stops at its first syntax error; no entry holds the token it stopped at.
// The work is therefore what the entries hold, however long the block's rest
// is.
//
// The grammar's alternatives come in order, and an earlier one wins over a
// later one that reads a longer run (contractEntryChoice): n.tags IS ::
// LIST<STRING> n.age > 0 is the requirement and then the comparison, though
// the whole also reads as one expression that compares with '<' and '>'. So
// where the longest run reads by a later alternative, an earlier one is
// looked for in it first (search).
func (block *contractBlockReading) read(ctx context.Context, at int, afterEntry bool) (bool, error) {
	data := block.entries.data
	source := data.source[:block.end]
	at, _ = cyphertext.GapEnd(source, at)
	if afterEntry && at < block.end && source[at] == ';' {
		at, _ = cyphertext.GapEnd(source, at+1)
	}
	if at >= block.end {
		return true, nil
	}
	if block.unread[at] {
		return false, nil
	}
	limit := block.end
	if next := sort.SearchInts(block.separators, at+1); next < len(block.separators) {
		limit = block.separators[next]
	}
	first := sort.Search(len(data.tokens), func(index int) bool { return data.tokens[index].start >= at })
	read, err := block.search(ctx, at, first, limit, contractAnyEntry)
	if err != nil || read {
		return read, err
	}
	if block.unread == nil {
		block.unread = map[int]bool{}
	}
	block.unread[at] = true
	return false, nil
}

// The alternatives an entry is read by, in the grammar's order: a contract of
// its own, then constraintRequirement's alternatives (the forms with IS, the
// form with IN, the others), then an expression. contractAnyEntry asks search
// for an entry by any of them.
const (
	contractAnyEntry = iota
	contractOwnContract
	contractRequirementIS
	contractRequirementIN
	contractRequirementOther
	contractExpression
)

// contractEntryChoice is the alternative entry was read by.
func contractEntryChoice(entry antlr4.ParserRuleContext) int {
	if entry.GetChildCount() == 0 {
		return contractOwnContract
	}
	switch body := entry.GetChild(0).(type) {
	case *ConstraintRequirementContext:
		if body.IS() != nil {
			return contractRequirementIS
		}
		if body.IN() != nil {
			return contractRequirementIN
		}
		return contractRequirementOther
	case *ExpressionContext:
		return contractExpression
	}
	return contractOwnContract
}

// search reports whether the body's rest reads with an entry that starts at
// at, the statement's token first, and ends at limit at the latest: an entry
// by any alternative (contractAnyEntry), or a requirement by an alternative
// that comes before the one given.
func (block *contractBlockReading) search(ctx context.Context, at, first, limit, before int) (bool, error) {
	data := block.entries.data
	requirement := before != contractAnyEntry
	for limit > at {
		after := sort.Search(len(data.tokens), func(index int) bool { return data.tokens[index].start >= limit })
		entry, ended, failed, stopped, err := block.entries.entry(ctx, first, after, requirement)
		if err != nil {
			return false, err
		}
		if failed == nil {
			// The entry ends before the limit when another token stood where
			// it was complete.
			after, limit = ended, data.tokens[ended].start
			choice := contractEntryChoice(entry)
			if (!requirement || choice < before) && !block.required[[2]int{at, choice}] && block.entries.earlierRequirementMayRead(choice, first, after) {
				// The run reads by a later alternative. A requirement by an
				// earlier one is in it and shorter: a longer run was read by
				// the alternatives in their order before this one.
				if block.required == nil {
					block.required = map[[2]int]bool{}
				}
				block.required[[2]int{at, choice}] = true
				read, err := block.search(ctx, at, first, limit, choice)
				if err != nil || read {
					return read, err
				}
			}
			if !requirement || choice < before {
				read, err := block.read(ctx, limit, true)
				if err != nil {
					return false, err
				}
				if read {
					block.kept = append(block.kept, contractEntryReading{tree: entry, first: first, after: after})
					return true, nil
				}
			}
		} else {
			// A run that is no requirement is no syntax error of the block.
			if !requirement && (block.failed == nil || stopped > block.stopped) {
				block.stopped, block.failed = stopped, failed
			}
			if stopped <= at {
				// No entry starts with this token.
				break
			}
			if stopped < limit {
				// No entry holds the token the reading stopped at.
				limit = stopped
				continue
			}
		}
		// The entry may end one token earlier.
		previous := after - 1
		for previous > first && data.tokens[previous].channel != antlr4.TokenDefaultChannel {
			previous--
		}
		if previous <= first {
			break
		}
		limit = data.tokens[previous].start
	}
	return false, nil
}

// earlierRequirementMayRead reports whether a requirement by an alternative
// before choice may read from the statement's tokens first up to after, the
// run an entry was read from by choice. Before an expression come all
// requirements. The forms that can extend an expression hold IS or IN, or
// start with MAX, ALLOWED, DISALLOWED or a block; before the requirement with
// IN come those with IS. An epoch invocation alone has the same expression
// span, while GROUP BY cannot extend an expression. It therefore needs no
// earlier-span search (TestContractEpochEntriesRetainTheirGroupingBoundaries).
// TestContractRequirementWordsAreTheGrammars pins these alternatives.
func (entries *contractEntryParser) earlierRequirementMayRead(choice, first, after int) bool {
	if choice != contractExpression && choice != contractRequirementIN {
		return false
	}
	if choice == contractExpression {
		switch entries.data.tokens[first].kind {
		case CypherLexerMAX, CypherLexerALLOWED, CypherLexerDISALLOWED, CypherLexerLBRACE:
			return true
		}
	}
	for _, token := range entries.data.tokens[first:after] {
		if token.kind == CypherLexerIS || choice == contractExpression && token.kind == CypherLexerIN {
			return true
		}
	}
	return false
}
