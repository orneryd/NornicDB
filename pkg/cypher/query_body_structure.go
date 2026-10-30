// Author of retained syntax inspection: Codex R17 implementation agent.
package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
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

// forEachQueryBody visits retained body occurrences without rediscovering them
// from source fragments. The container walker below is the single role owner.
func forEachQueryBody(ctx context.Context, body queryBody, containers []string, visit func(string, queryBody) error) error {
	if !body.syntax.Valid() {
		return ctx.Err()
	}
	return forEachQueryContainer(ctx, body.syntax, containers, func(kind string, node cypherantlr.SyntaxNode) error {
		child, _, ok := queryExpressionBody(node)
		if !ok {
			return nil
		}
		return visit(kind, child)
	})
}

// forEachQueryBodyText preserves the independent source/admission boundary and
// its original brace-coordinate protocol. Retained readers use the typed entry.
func forEachQueryBodyText(ctx context.Context, source string, containers []string, visit func(string, int, int) error) error {
	if isNornicExtensionStatement(source) || isSchemaCommandStatement(source) {
		return nil
	}
	syntax, err := cypherantlr.InspectSyntax(ctx, source, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err != nil {
		return syntaxAdmissionError(ctx, source, AcceptedSource, err)
	}
	return forEachQueryContainer(ctx, syntax.Root(), containers, func(kind string, node cypherantlr.SyntaxNode) error {
		open, hasOpen := node.DirectToken(cypherantlr.CypherLexerLBRACE)
		close, hasClose := node.DirectToken(cypherantlr.CypherLexerRBRACE)
		if !hasOpen || !hasClose {
			return nil
		}
		_, start := open.Span()
		end, _ := close.Span()
		return visit(kind, start, end)
	})
}

func forEachQueryContainer(ctx context.Context, root cypherantlr.SyntaxNode, containers []string, visit func(string, cypherantlr.SyntaxNode) error) error {
	var failure error
	root.Walk(func(node cypherantlr.SyntaxNode) func() {
		if failure == nil {
			failure = ctx.Err()
		}
		if failure != nil {
			return nil
		}
		var kind string
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_callSubquery:
			kind = "CALL"
		case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist:
			kind = "EXISTS"
		case cypherantlr.CypherParserRULE_countSubquery:
			kind = "COUNT"
		case cypherantlr.CypherParserRULE_collectSubquery:
			kind = "COLLECT"
		default:
			return nil
		}
		if containsString(containers, kind) {
			failure = visit(kind, node)
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
	syntax cypherantlr.SyntaxNode
	source *cyphertext.Source
	// executionText is the existing FINISH/routing owner's lexical execution
	// view. It is never used to rediscover this body's retained grammar nodes.
	executionText string
	branches      []queryBodyBranch
	status        queryBodyStatus
	foreach       *queryBodyForeach
}

// queryBodyForeach retains the loop header and its update clauses from the
// statement's grammar product. Both admission and execution consume this view.
type queryBodyForeach struct {
	variable       string
	listExpression string
	updates        []pipelineClause
}

type queryBodyBranch struct {
	syntax                 cypherantlr.SyntaxNode
	source                 *cyphertext.Source
	executionText          string
	start, end             int
	unionAll               bool
	useStart, useEnd       int
	clauses                []queryBodyClause
	finished               bool
	finishStart, finishEnd int
	declined               bool
	useSyntax              cypherantlr.SyntaxNode
	finishSyntax           cypherantlr.SyntaxNode
}

type queryBodyClause struct {
	pipelineClause
}

func declinedQueryBody(start, end int) queryBody {
	return queryBody{status: queryBodyDeclined, branches: []queryBodyBranch{{start: start, end: end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1, declined: true}}}
}

// readQueryBodyText is the explicit source-window boundary. Statement
// compilation retains its product and calls readQueryBody directly.
func readQueryBodyText(ctx context.Context, source string, start, end int) queryBody {
	if ctx.Err() != nil {
		return declinedQueryBody(start, end)
	}
	syntax, err := cypherantlr.InspectSyntax(ctx, source, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil || ctx.Err() != nil {
		return declinedQueryBody(start, end)
	}
	node, ok := syntax.Window(start, end, cypherantlr.CypherParserRULE_query, cypherantlr.CypherParserRULE_regularQuery, cypherantlr.CypherParserRULE_subqueryBody, cypherantlr.CypherParserRULE_singleQuery, cypherantlr.CypherParserRULE_matchSt)
	if !ok {
		root := syntax.Root()
		pattern, found := syntax.Window(start, end, cypherantlr.CypherParserRULE_patternWhere, cypherantlr.CypherParserRULE_pattern)
		if found {
			if container, parent := pattern.Parent(); parent {
				if body, _, complete := queryExpressionBody(container); complete {
					return body
				}
			}
		}
		return readQueryBody(root)
	}
	return readQueryBody(node)
}

func readQueryBody(node cypherantlr.SyntaxNode) (body queryBody) {
	defer func() {
		body.syntax = node
		if node.Valid() {
			body.source = node.CodeSource(nil)
			body.executionText = body.source.Text
		}
	}()
	body = queryBody{status: queryBodyRead}
	if !node.Valid() {
		return queryBody{status: queryBodyDeclined}
	}
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_query:
		regular := node.ChildRule(cypherantlr.CypherParserRULE_regularQuery)
		if regular.Valid() {
			body = readQueryBody(regular)
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
			// The leading USE belongs to query, outside its singleQuery child.
			// Its branch window must retain that actual enclosing owner.
			body.branches[0].syntax = node
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
				tail := readQueryBody(child)
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
			body.foreach.updates = append(body.foreach.updates, clause)
		}
		body.branches = []queryBodyBranch{branch}
	case cypherantlr.CypherParserRULE_singleQuery, cypherantlr.CypherParserRULE_singlePartQ, cypherantlr.CypherParserRULE_multiPartQ, cypherantlr.CypherParserRULE_readingStatement, cypherantlr.CypherParserRULE_updatingStatement:
		branch := newQueryBodyBranch(node)
		appendQuerySyntax(&branch, node)
		body.branches = []queryBodyBranch{branch}
	case cypherantlr.CypherParserRULE_matchSt:
		branch := newQueryBodyBranch(node)
		appendQuerySyntax(&branch, node)
		body.branches = []queryBodyBranch{branch}
	default:
		if _, clause := querySyntaxClauseKind(node); clause {
			branch := newQueryBodyBranch(node)
			appendQuerySyntax(&branch, node)
			body.branches = []queryBodyBranch{branch}
		} else {
			start, end := node.Span()
			return declinedQueryBody(start, end)
		}
	}
	var all *bool
	for i := range body.branches {
		branch := &body.branches[i]
		branch.retainSourceWindow()
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
			return readQueryBody(body), body, true
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
	inputStart, inputEnd := pattern.InputSpan()
	if where := container.ChildRule(cypherantlr.CypherParserRULE_where); where.Valid() {
		_, end = where.Span()
		_, inputEnd = where.InputSpan()
	}
	containerStart, _ := container.InputSpan()
	prefix := container.InputBefore(container.Child(container.ChildCount() - 1))
	source := prefix.Window(inputStart-containerStart, inputEnd-containerStart)
	text := container.Source()[start:end]
	branch := queryBodyBranch{syntax: container, source: source, executionText: text, start: start, end: end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1}
	branch.clauses = []queryBodyClause{queryBodyClauseFromSyntax(container)}
	return queryBody{syntax: container, source: source, executionText: text, status: queryBodyRead, branches: []queryBodyBranch{branch}}, container, true
}

func newQueryBodyBranch(node cypherantlr.SyntaxNode) queryBodyBranch {
	start, end := node.Span()
	source := node.CodeSource(nil)
	return queryBodyBranch{syntax: node, source: source, executionText: source.Text, start: start, end: end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1}
}

// branch keeps an occurrence's node namespace and source association when a
// UNION consumer visits it independently. Equal branch text cannot replace it.
func (body queryBody) branch(index int) queryBody {
	branch := body.branches[index]
	status := queryBodyRead
	if branch.declined {
		status = queryBodyDeclined
	}
	return queryBody{syntax: branch.syntax, source: branch.source, executionText: branch.executionText, branches: body.branches[index : index+1], status: status}
}

// clauseWindow retains a contiguous physical execution stretch of one branch.
// The omitted clauses are still admitted by the enclosing Script; this view
// cannot admit a reconstructed source or substitute an equal-text occurrence.
func (body queryBody) clauseWindow(first, after int) queryBody {
	branch := body.branches[0]
	branch.clauses = branch.clauses[first:after]
	branch.useStart, branch.useEnd = -1, -1
	branch.useSyntax = cypherantlr.SyntaxNode{}
	branch.finished = false
	branch.finishStart, branch.finishEnd = -1, -1
	branch.finishSyntax = cypherantlr.SyntaxNode{}
	if len(branch.clauses) == 0 {
		branch.end = branch.start
		branch.executionText = ""
		branch.source = branch.source.Window(0, 0)
	} else {
		branch.start = branch.clauses[0].start
		branch.end = branch.clauses[len(branch.clauses)-1].end
		branch.retainSourceWindow()
	}
	body.branches = []queryBodyBranch{branch}
	body.source, body.executionText = branch.source, branch.executionText
	return body
}

// mutationReturnSources supplies native physical planners with the actual
// admitted final RETURN and its preceding clause stretch. It never joins
// fragments to manufacture another statement or searches for a keyword.
// mutationReturnSources splits a one-branch body into its mutation text and
// its final retained RETURN occurrence (hasReturn false when it has none).
func (body queryBody) mutationReturnSources() (mutation string, returned pipelineClause, hasReturn bool) {
	mutation = body.executionText
	if body.status != queryBodyRead || len(body.branches) != 1 {
		return mutation, returned, false
	}
	clauses := body.branches[0].clauses
	if len(clauses) == 0 || clauses[len(clauses)-1].kind != pipelineClauseReturn {
		return mutation, returned, false
	}
	returned, hasReturn = clauses[len(clauses)-1].pipelineClause, true
	mutation = ""
	if len(clauses) > 1 {
		mutation = body.syntax.CodeSourceWindow(clauses[0].syntax, clauses[len(clauses)-2].syntax).Text
	}
	return mutation, returned, hasReturn
}

// queryBodyClause's one constructor: the body clause of an admitted node whose
// grammar role the caller established (requiredPipelineClause).
// Author: Claude R17 implementation agent, 30 October 2026.
func queryBodyClauseFromSyntax(node cypherantlr.SyntaxNode) queryBodyClause {
	return queryBodyClause{pipelineClause: requiredPipelineClause(node)}
}

func lastQueryBranchSpan(branch queryBodyBranch) (start, end int) {
	start, end = branch.start, branch.start
	if branch.useEnd > end {
		end = branch.useEnd
	}
	if len(branch.clauses) > 0 {
		end = branch.clauses[len(branch.clauses)-1].end
	}
	if branch.finishEnd > end {
		end = branch.finishEnd
	}
	return
}

func setQueryBodyUse(branch *queryBodyBranch, node cypherantlr.SyntaxNode) {
	if branch.useStart >= 0 {
		branch.declined = true
		return
	}
	branch.useStart, branch.useEnd = node.Span()
	branch.useSyntax = node
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
	_, ok := querySyntaxClauseKind(node)
	if !ok {
		if node.Rule() >= 0 {
			branch.declined = true
		}
		return
	}
	branch.clauses = append(branch.clauses, queryBodyClauseFromSyntax(node))
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
	for _, branch := range readQueryBodyText(ctx, source, 0, len(source)).branches {
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
	switch root.Rule() {
	case cypherantlr.CypherParserRULE_query, cypherantlr.CypherParserRULE_regularQuery,
		cypherantlr.CypherParserRULE_subqueryBody, cypherantlr.CypherParserRULE_singleQuery,
		cypherantlr.CypherParserRULE_singlePartQ, cypherantlr.CypherParserRULE_multiPartQ,
		cypherantlr.CypherParserRULE_readingStatement, cypherantlr.CypherParserRULE_updatingStatement:
		return root
	}
	if _, clause := querySyntaxClauseKind(root); clause {
		return root
	}
	script := root.ChildRule(cypherantlr.CypherParserRULE_script)
	if script.Valid() {
		root = script
	}
	return root.ChildRule(cypherantlr.CypherParserRULE_query)
}

func queryStatementWindow(ctx context.Context, source string) (start, end int, mode ExecutionMode, ok bool) {
	syntax, err := cypherantlr.QuerySyntax(ctx, source, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
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
	call, err := cypherantlr.LeadingCallSyntax(ctx, source, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return -1, -1, false
	}
	if !call.Valid() || call.Rule() != cypherantlr.CypherParserRULE_callSubquery {
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

func callClauseBody(clause queryBodyClause) (queryBody, bool) {
	if clause.kind != pipelineClauseCallSubquery {
		return queryBody{}, false
	}
	body := clause.syntax.ChildRule(cypherantlr.CypherParserRULE_subqueryBody)
	if !body.Valid() {
		return queryBody{}, false
	}
	return readQueryBody(body), true
}

// retainSourceWindow projects the already read branch extent, including a proper
// leading clause stretch before UNION/FINISH, through its retained Source owner.
func (branch *queryBodyBranch) retainSourceWindow() {
	var first, last cypherantlr.SyntaxNode
	take := func(node cypherantlr.SyntaxNode) {
		if !node.Valid() {
			return
		}
		start, end := node.Span()
		if start < branch.start || end > branch.end {
			return
		}
		if !first.Valid() {
			first, last = node, node
			return
		}
		a, _ := first.Span()
		_, b := last.Span()
		if start < a {
			first = node
		}
		if end > b {
			last = node
		}
	}
	for _, clause := range branch.clauses {
		node := clause.syntax
		if clause.optional && (clause.kind == pipelineClauseCall || clause.kind == pipelineClauseCallSubquery) {
			for i := 0; i < node.ChildCount(); i++ {
				child := node.Child(i)
				if child.IsToken(cypherantlr.CypherLexerCALL) {
					take(child)
					break
				}
			} // last extent still belongs to the CALL node.
			if node.Valid() {
				_, end := node.Span()
				_, previous := last.Span()
				if end > previous {
					last = node
				}
			}
			continue
		}
		take(node)
	}
	take(branch.useSyntax)
	take(branch.finishSyntax)
	if first.Valid() && last.Valid() {
		branch.source = branch.syntax.CodeSourceWindow(first, last)
		branch.executionText = branch.source.Text
	}
}
