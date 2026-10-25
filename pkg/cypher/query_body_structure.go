package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Query structure is a read-only view of execution's retained grammar product.
// It remains available when names or procedure signatures cannot be resolved.
var queryBodyContainerKeywords = [...]string{"CALL", "EXISTS", "COUNT", "COLLECT"}

func queryBodyContainerKeyword(word string) bool {
	for _, keyword := range queryBodyContainerKeywords {
		if strings.EqualFold(word, keyword) {
			return true
		}
	}
	return false
}

func forEachQueryBody(ctx context.Context, source string, containers []string, visit func(string, int, int) error) error {
	if isNornicExtensionStatement(source) || isSchemaCommandStatement(source) {
		return nil
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err != nil {
		return syntaxAdmissionError(ctx, source, AcceptedSource, err)
	}
	var failure error
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if failure == nil {
			failure = ctx.Err()
		}
		if failure != nil {
			return nil
		}
		var container string
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_callSubquery:
			container = "CALL"
		case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist:
			container = "EXISTS"
		case cypherantlr.CypherParserRULE_countSubquery:
			container = "COUNT"
		case cypherantlr.CypherParserRULE_collectSubquery:
			container = "COLLECT"
		default:
			return nil
		}
		if !containsString(containers, container) {
			return nil
		}
		open, hasOpen := node.DirectToken(cypherantlr.CypherLexerLBRACE)
		close, hasClose := node.DirectToken(cypherantlr.CypherLexerRBRACE)
		if hasOpen && hasClose {
			_, start := open.Span()
			end, _ := close.Span()
			failure = visit(container, start, end)
		}
		return nil
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	return failure
}

type queryBodyStatus uint8

const (
	queryBodyRead queryBodyStatus = iota
	queryBodyDeclined
	queryBodyMixedUnion
)

type queryBody struct {
	branches []queryBodyBranch
	status   queryBodyStatus
	foreach  *queryBodyForeach
}

// queryBodyForeach retains the loop header and its update clauses from the
// statement's grammar product. Both admission and execution consume this view.
type queryBodyForeach struct {
	variable       string
	listExpression string
	updates        []pipelineClause
}

type queryBodyBranch struct {
	start, end             int
	unionAll               bool
	useStart, useEnd       int
	clauses                []queryBodyClause
	finished               bool
	finishStart, finishEnd int
	declined               bool
	useSyntax              cypherantlr.SyntaxNode
	uses                   []cypherantlr.SyntaxNode
	finishSyntax           cypherantlr.SyntaxNode
}

type queryBodyClause struct {
	pipelineClause
	start, end int
}

func declinedQueryBody(start, end int) queryBody {
	return queryBody{status: queryBodyDeclined, branches: []queryBodyBranch{{start: start, end: end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1, declined: true}}}
}

// readQueryBody is the compatibility entry for a source window. Statement
// compilation retains its product and calls queryBodyFromSyntax directly.
func readQueryBody(ctx context.Context, source string, start, end int) queryBody {
	if ctx.Err() != nil {
		return declinedQueryBody(start, end)
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil || ctx.Err() != nil {
		return declinedQueryBody(start, end)
	}
	node, ok := syntax.Window(start, end, cypherantlr.CypherParserRULE_query, cypherantlr.CypherParserRULE_regularQuery, cypherantlr.CypherParserRULE_subqueryBody, cypherantlr.CypherParserRULE_singleQuery, cypherantlr.CypherParserRULE_matchSt)
	if !ok {
		pattern, found := syntax.Window(start, end, cypherantlr.CypherParserRULE_patternWhere, cypherantlr.CypherParserRULE_pattern)
		if found {
			if container, parent := pattern.Parent(); parent {
				if body, _, complete := queryExpressionBody(container); complete {
					return body
				}
			}
		}
		return declinedQueryBody(start, end)
	}
	return queryBodyFromSyntax(node)
}

func queryBodyFromSyntax(node cypherantlr.SyntaxNode) queryBody {
	body := queryBody{status: queryBodyRead}
	if !node.Valid() {
		return queryBody{status: queryBodyDeclined}
	}
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_query:
		regular := node.ChildRule(cypherantlr.CypherParserRULE_regularQuery)
		if regular.Valid() {
			body = queryBodyFromSyntax(regular)
		} else {
			call := node.ChildRule(cypherantlr.CypherParserRULE_standaloneCall)
			if !call.Valid() {
				start, end := node.Span()
				return declinedQueryBody(start, end)
			}
			branch := newQueryBodyBranch(call)
			appendQuerySyntax(&branch, call)
			body.branches = []queryBodyBranch{branch}
		}
		use := node.ChildRule(cypherantlr.CypherParserRULE_useClause)
		if use.Valid() && len(body.branches) > 0 {
			setQueryBodyUse(&body.branches[0], use)
			body.branches[0].start, _ = use.Span()
		}
	case cypherantlr.CypherParserRULE_regularQuery:
		for i := 0; i < node.ChildCount(); i++ {
			child := node.Child(i)
			switch child.Rule() {
			case cypherantlr.CypherParserRULE_singleQuery:
				branch := newQueryBodyBranch(child)
				appendQuerySyntax(&branch, child)
				body.branches = append(body.branches, branch)
			case cypherantlr.CypherParserRULE_unionSt:
				query := child.ChildRule(cypherantlr.CypherParserRULE_singleQuery)
				branch := newQueryBodyBranch(query)
				_, branch.unionAll = child.DirectToken(cypherantlr.CypherLexerALL)
				appendQuerySyntax(&branch, query)
				body.branches = append(body.branches, branch)
			}
		}
	case cypherantlr.CypherParserRULE_subqueryBody:
		branch := newQueryBodyBranch(node)
		for i := 0; i < node.ChildCount(); i++ {
			child := node.Child(i)
			if child.Rule() == cypherantlr.CypherParserRULE_subqueryBody {
				tail := queryBodyFromSyntax(child)
				if len(tail.branches) > 0 {
					_, tail.branches[0].unionAll = node.DirectToken(cypherantlr.CypherLexerALL)
				}
				if branch.end > branch.start {
					body.branches = append(body.branches, branch)
				}
				body.branches = append(body.branches, tail.branches...)
				break
			}
			if child.IsToken(cypherantlr.CypherLexerUNION) {
				_, branch.end = lastQueryBranchSpan(branch)
				continue
			}
			appendQuerySyntax(&branch, child)
		}
		if len(body.branches) == 0 {
			body.branches = []queryBodyBranch{branch}
		}
	case cypherantlr.CypherParserRULE_foreachSt:
		variable := node.ChildRule(cypherantlr.CypherParserRULE_symbol)
		expression := node.ChildRule(cypherantlr.CypherParserRULE_expression)
		if !variable.Valid() || !expression.Valid() {
			start, end := node.Span()
			return declinedQueryBody(start, end)
		}
		branch := newQueryBodyBranch(node)
		for i := 0; i < node.ChildCount(); i++ {
			child := node.Child(i)
			if child.Rule() == cypherantlr.CypherParserRULE_updatingStatement {
				appendQuerySyntax(&branch, child)
			}
		}
		if len(branch.clauses) == 0 {
			return declinedQueryBody(branch.start, branch.end)
		}
		body.foreach = &queryBodyForeach{
			variable:       symbolicNameValue(variable.Text()),
			listExpression: expression.Text(),
			updates:        make([]pipelineClause, 0, len(branch.clauses)),
		}
		for _, source := range branch.clauses {
			clause := source.pipelineClause
			clause.syntax = source.syntax
			body.foreach.updates = append(body.foreach.updates, clause)
		}
		body.branches = []queryBodyBranch{branch}
	case cypherantlr.CypherParserRULE_singleQuery:
		branch := newQueryBodyBranch(node)
		appendQuerySyntax(&branch, node)
		body.branches = []queryBodyBranch{branch}
	case cypherantlr.CypherParserRULE_matchSt:
		branch := newQueryBodyBranch(node)
		appendQuerySyntax(&branch, node)
		body.branches = []queryBodyBranch{branch}
	default:
		start, end := node.Span()
		return declinedQueryBody(start, end)
	}
	var all *bool
	for i := range body.branches {
		branch := &body.branches[i]
		if branch.declined {
			body.status = queryBodyDeclined
		}
		if i > 0 {
			if all != nil && *all != branch.unionAll {
				body.status = queryBodyMixedUnion
			}
			value := branch.unionAll
			all = &value
		}
	}
	return body
}

// queryExpressionBody covers the grammar's statement and pattern forms through
// one structural view. A pattern-only body is an implicit MATCH, with its own
// declarations and predicate scope; it is never read as an outer expression.
func queryExpressionBody(container cypherantlr.SyntaxNode) (queryBody, cypherantlr.SyntaxNode, bool) {
	for _, rule := range []int{cypherantlr.CypherParserRULE_subqueryBody, cypherantlr.CypherParserRULE_regularQuery, cypherantlr.CypherParserRULE_matchSt} {
		if body := container.ChildRule(rule); body.Valid() {
			return queryBodyFromSyntax(body), body, true
		}
	}
	pattern := container.ChildRule(cypherantlr.CypherParserRULE_pattern)
	if !pattern.Valid() {
		if patternWhere := container.ChildRule(cypherantlr.CypherParserRULE_patternWhere); patternWhere.Valid() {
			pattern = patternWhere
		}
	}
	if !pattern.Valid() {
		return queryBody{}, cypherantlr.SyntaxNode{}, false
	}
	start, end := pattern.Span()
	if where := container.ChildRule(cypherantlr.CypherParserRULE_where); where.Valid() {
		_, end = where.Span()
	}
	branch := queryBodyBranch{start: start, end: end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1}
	branch.clauses = []queryBodyClause{{pipelineClause: pipelineClause{kind: pipelineClauseMatch, text: container.Source()[start:end], syntax: container}, start: start, end: end}}
	return queryBody{status: queryBodyRead, branches: []queryBodyBranch{branch}}, container, true
}

func newQueryBodyBranch(node cypherantlr.SyntaxNode) queryBodyBranch {
	start, end := node.Span()
	return queryBodyBranch{start: start, end: end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1}
}

func lastQueryBranchSpan(branch queryBodyBranch) (start, end int) {
	start, end = branch.start, branch.start
	for _, use := range branch.useNodes() {
		_, useEnd := use.Span()
		if useEnd > end {
			end = useEnd
		}
	}
	if len(branch.clauses) > 0 {
		end = max(end, branch.clauses[len(branch.clauses)-1].end)
	}
	if branch.finishEnd > end {
		end = branch.finishEnd
	}
	return
}

// useNodes retains every routing clause in this branch. useSyntax/useStart
// remain the first selection for names-only views and pipeline eligibility.
func (branch queryBodyBranch) useNodes() []cypherantlr.SyntaxNode {
	if len(branch.uses) > 0 {
		return branch.uses
	}
	if branch.useSyntax.Valid() {
		return []cypherantlr.SyntaxNode{branch.useSyntax}
	}
	return nil
}

func setQueryBodyUse(branch *queryBodyBranch, node cypherantlr.SyntaxNode) {
	branch.uses = append(branch.uses, node)
	start, end := node.Span()
	if branch.useStart < 0 || start < branch.useStart {
		branch.useStart, branch.useEnd, branch.useSyntax = start, end, node
	}
}

// appendQuerySyntax unwraps grammar containers only. It does not enter a clause
// expression or CALL body; those belong to their own retained statement views.
func appendQuerySyntax(branch *queryBodyBranch, node cypherantlr.SyntaxNode) {
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_singleQuery, cypherantlr.CypherParserRULE_singlePartQ, cypherantlr.CypherParserRULE_multiPartQ, cypherantlr.CypherParserRULE_readingStatement, cypherantlr.CypherParserRULE_updatingStatement:
		for i := 0; i < node.ChildCount(); i++ {
			appendQuerySyntax(branch, node.Child(i))
		}
		return
	case cypherantlr.CypherParserRULE_useClause:
		setQueryBodyUse(branch, node)
		return
	case cypherantlr.CypherParserRULE_embeddingSt:
		// WITH EMBEDDING is an execution option on the preceding updates. It
		// neither reads nor binds names, so it is not a scope operator.
		return
	}
	if node.IsToken(cypherantlr.CypherLexerFINISH) {
		branch.finished = true
		branch.finishSyntax = node
		branch.finishStart, branch.finishEnd = node.Span()
		return
	}
	kind, ok := querySyntaxClauseKind(node)
	if !ok {
		if node.Rule() >= 0 {
			branch.declined = true
		}
		return
	}
	start, end := node.Span()
	_, optional := node.DirectToken(cypherantlr.CypherLexerOPTIONAL)
	if optional && (kind == pipelineClauseCall || kind == pipelineClauseCallSubquery) {
		if call, found := node.DirectToken(cypherantlr.CypherLexerCALL); found {
			start, _ = call.Span()
		}
	}
	text := node.Text()
	nodeStart, _ := node.Span()
	if start > nodeStart {
		text = text[start-nodeStart:]
	}
	branch.clauses = append(branch.clauses, queryBodyClause{pipelineClause: pipelineClause{kind: kind, text: text, optional: optional, syntax: node}, start: start, end: end})
}

// embeddingCandidate reports the documented nonterminal WITH EMBEDDING form:
// one unaliased, unquoted EMBEDDING projection after an updating clause and
// immediately before RETURN. It reads the statement's single parse; the scope
// compiler, semantic validation and the pipeline frame decide its meaning.
func embeddingCandidate(clauses []queryBodyClause, index int) string {
	if index <= 0 || index+1 >= len(clauses) || clauses[index].kind != pipelineClauseWith || clauses[index+1].kind != pipelineClauseReturn {
		return ""
	}
	switch clauses[index-1].kind {
	case pipelineClauseCreate, pipelineClauseMerge, pipelineClauseSet, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseForeach:
	default:
		return ""
	}
	code := clauses[index].text
	if clauses[index].syntax.Valid() {
		code = clauses[index].syntax.Code()
	}
	code = strings.TrimSpace(code)
	if len(code) < len("WITH") || !strings.EqualFold(code[:len("WITH")], "WITH") {
		return ""
	}
	if name := strings.TrimSpace(code[len("WITH"):]); strings.EqualFold(name, "EMBEDDING") {
		return name
	}
	return ""
}

// statementHasEmbeddingCandidate reports a nonterminal WITH EMBEDDING
// projection in any branch of the statement's single parse.
func statementHasEmbeddingCandidate(ctx context.Context, source string) bool {
	if !containsFold(source, "EMBEDDING") {
		return false
	}
	for _, branch := range readQueryBody(ctx, source, 0, len(source)).branches {
		for index := range branch.clauses {
			if embeddingCandidate(branch.clauses, index) != "" {
				return true
			}
		}
	}
	return false
}

func querySyntaxClauseKind(node cypherantlr.SyntaxNode) (pipelineClauseKind, bool) {
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_matchSt:
		if _, optional := node.DirectToken(cypherantlr.CypherLexerOPTIONAL); optional {
			return pipelineClauseOptionalMatch, true
		}
		return pipelineClauseMatch, true
	case cypherantlr.CypherParserRULE_createSt:
		return pipelineClauseCreate, true
	case cypherantlr.CypherParserRULE_mergeSt:
		return pipelineClauseMerge, true
	case cypherantlr.CypherParserRULE_deleteSt:
		return pipelineClauseDelete, true
	case cypherantlr.CypherParserRULE_setSt:
		return pipelineClauseSet, true
	case cypherantlr.CypherParserRULE_removeSt:
		return pipelineClauseRemove, true
	case cypherantlr.CypherParserRULE_foreachSt:
		return pipelineClauseForeach, true
	case cypherantlr.CypherParserRULE_withSt:
		return pipelineClauseWith, true
	case cypherantlr.CypherParserRULE_unwindSt, cypherantlr.CypherParserRULE_forSt:
		return pipelineClauseUnwind, true
	case cypherantlr.CypherParserRULE_returnSt:
		return pipelineClauseReturn, true
	case cypherantlr.CypherParserRULE_callSubquery:
		return pipelineClauseCallSubquery, true
	case cypherantlr.CypherParserRULE_queryCallSt, cypherantlr.CypherParserRULE_standaloneCall:
		return pipelineClauseCall, true
	case cypherantlr.CypherParserRULE_letSt:
		return pipelineClauseLet, true
	case cypherantlr.CypherParserRULE_filterSt:
		return pipelineClauseFilter, true
	default:
		return 0, false
	}
}

func statementSyntaxQuery(syntax cypherantlr.StatementSyntax) cypherantlr.SyntaxNode {
	root := syntax.Root()
	if root.Rule() == cypherantlr.CypherParserRULE_query {
		return root
	}
	script := root.ChildRule(cypherantlr.CypherParserRULE_script)
	if script.Valid() {
		root = script
	}
	return root.ChildRule(cypherantlr.CypherParserRULE_query)
}

func queryStatementWindow(ctx context.Context, source string) (start, end int, mode ExecutionMode, ok bool) {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return -1, -1, ModeNormal, false
	}
	query := statementSyntaxQuery(syntax)
	if !query.Valid() {
		return -1, -1, ModeNormal, false
	}
	mode = ModeNormal
	start = -1
	for i := 0; i < query.ChildCount(); i++ {
		child := query.Child(i)
		switch child.Rule() {
		case cypherantlr.CypherParserRULE_queryPrefix:
			if _, found := child.DirectToken(cypherantlr.CypherLexerEXPLAIN); found {
				mode = ModeExplain
			}
			if _, found := child.DirectToken(cypherantlr.CypherLexerPROFILE); found {
				mode = ModeProfile
			}
		case cypherantlr.CypherParserRULE_cypherPreamble:
		default:
			if start < 0 {
				start, _ = child.Span()
			}
		}
	}
	_, end = query.Span()
	return start, end, mode, start >= 0
}

func callSubqueryBraces(ctx context.Context, source string) (open, end int, ok bool) {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return -1, -1, false
	}
	call, found := syntax.LeadingCall()
	if !found || call.Rule() != cypherantlr.CypherParserRULE_callSubquery {
		return -1, -1, false
	}
	return querySyntaxBraces(call)
}
func querySyntaxBraces(node cypherantlr.SyntaxNode) (open, end int, ok bool) {
	left, hasLeft := node.DirectToken(cypherantlr.CypherLexerLBRACE)
	right, hasRight := node.DirectToken(cypherantlr.CypherLexerRBRACE)
	if !hasLeft || !hasRight {
		return -1, -1, false
	}
	open, _ = left.Span()
	end, _ = right.Span()
	return open, end, true
}

func callClauseBody(ctx context.Context, clause queryBodyClause) (start, end int, ok bool) {
	if clause.kind != pipelineClauseCallSubquery {
		return -1, -1, false
	}
	if clause.syntax.Valid() {
		open, close, found := querySyntaxBraces(clause.syntax)
		return open + 1, close, found
	}
	open, close, found := callSubqueryBraces(ctx, clause.text)
	return clause.start + open + 1, clause.start + close, found
}
