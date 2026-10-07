// Code generated from CypherParser.g4 by ANTLR 4.13.1. DO NOT EDIT.

package antlr // CypherParser
import "github.com/antlr4-go/antlr/v4"

// BaseCypherParserListener is a complete listener for a parse tree produced by CypherParser.
type BaseCypherParserListener struct{}

var _ CypherParserListener = &BaseCypherParserListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseCypherParserListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseCypherParserListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseCypherParserListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseCypherParserListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterScript is called when production script is entered.
func (s *BaseCypherParserListener) EnterScript(ctx *ScriptContext) {}

// ExitScript is called when production script is exited.
func (s *BaseCypherParserListener) ExitScript(ctx *ScriptContext) {}

// EnterShellCommand is called when production shellCommand is entered.
func (s *BaseCypherParserListener) EnterShellCommand(ctx *ShellCommandContext) {}

// ExitShellCommand is called when production shellCommand is exited.
func (s *BaseCypherParserListener) ExitShellCommand(ctx *ShellCommandContext) {}

// EnterShellCommandElement is called when production shellCommandElement is entered.
func (s *BaseCypherParserListener) EnterShellCommandElement(ctx *ShellCommandElementContext) {}

// ExitShellCommandElement is called when production shellCommandElement is exited.
func (s *BaseCypherParserListener) ExitShellCommandElement(ctx *ShellCommandElementContext) {}

// EnterTransactionStatement is called when production transactionStatement is entered.
func (s *BaseCypherParserListener) EnterTransactionStatement(ctx *TransactionStatementContext) {}

// ExitTransactionStatement is called when production transactionStatement is exited.
func (s *BaseCypherParserListener) ExitTransactionStatement(ctx *TransactionStatementContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseCypherParserListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseCypherParserListener) ExitQuery(ctx *QueryContext) {}

// EnterUseClause is called when production useClause is entered.
func (s *BaseCypherParserListener) EnterUseClause(ctx *UseClauseContext) {}

// ExitUseClause is called when production useClause is exited.
func (s *BaseCypherParserListener) ExitUseClause(ctx *UseClauseContext) {}

// EnterQueryPrefix is called when production queryPrefix is entered.
func (s *BaseCypherParserListener) EnterQueryPrefix(ctx *QueryPrefixContext) {}

// ExitQueryPrefix is called when production queryPrefix is exited.
func (s *BaseCypherParserListener) ExitQueryPrefix(ctx *QueryPrefixContext) {}

// EnterShowCommand is called when production showCommand is entered.
func (s *BaseCypherParserListener) EnterShowCommand(ctx *ShowCommandContext) {}

// ExitShowCommand is called when production showCommand is exited.
func (s *BaseCypherParserListener) ExitShowCommand(ctx *ShowCommandContext) {}

// EnterAdministrationCommand is called when production administrationCommand is entered.
func (s *BaseCypherParserListener) EnterAdministrationCommand(ctx *AdministrationCommandContext) {}

// ExitAdministrationCommand is called when production administrationCommand is exited.
func (s *BaseCypherParserListener) ExitAdministrationCommand(ctx *AdministrationCommandContext) {}

// EnterQualifiedName is called when production qualifiedName is entered.
func (s *BaseCypherParserListener) EnterQualifiedName(ctx *QualifiedNameContext) {}

// ExitQualifiedName is called when production qualifiedName is exited.
func (s *BaseCypherParserListener) ExitQualifiedName(ctx *QualifiedNameContext) {}

// EnterSchemaCommand is called when production schemaCommand is entered.
func (s *BaseCypherParserListener) EnterSchemaCommand(ctx *SchemaCommandContext) {}

// ExitSchemaCommand is called when production schemaCommand is exited.
func (s *BaseCypherParserListener) ExitSchemaCommand(ctx *SchemaCommandContext) {}

// EnterConstraintRequirement is called when production constraintRequirement is entered.
func (s *BaseCypherParserListener) EnterConstraintRequirement(ctx *ConstraintRequirementContext) {}

// ExitConstraintRequirement is called when production constraintRequirement is exited.
func (s *BaseCypherParserListener) ExitConstraintRequirement(ctx *ConstraintRequirementContext) {}

// EnterConstraintBlock is called when production constraintBlock is entered.
func (s *BaseCypherParserListener) EnterConstraintBlock(ctx *ConstraintBlockContext) {}

// ExitConstraintBlock is called when production constraintBlock is exited.
func (s *BaseCypherParserListener) ExitConstraintBlock(ctx *ConstraintBlockContext) {}

// EnterPropertyTypeName is called when production propertyTypeName is entered.
func (s *BaseCypherParserListener) EnterPropertyTypeName(ctx *PropertyTypeNameContext) {}

// ExitPropertyTypeName is called when production propertyTypeName is exited.
func (s *BaseCypherParserListener) ExitPropertyTypeName(ctx *PropertyTypeNameContext) {}

// EnterRegularQuery is called when production regularQuery is entered.
func (s *BaseCypherParserListener) EnterRegularQuery(ctx *RegularQueryContext) {}

// ExitRegularQuery is called when production regularQuery is exited.
func (s *BaseCypherParserListener) ExitRegularQuery(ctx *RegularQueryContext) {}

// EnterSingleQuery is called when production singleQuery is entered.
func (s *BaseCypherParserListener) EnterSingleQuery(ctx *SingleQueryContext) {}

// ExitSingleQuery is called when production singleQuery is exited.
func (s *BaseCypherParserListener) ExitSingleQuery(ctx *SingleQueryContext) {}

// EnterStandaloneCall is called when production standaloneCall is entered.
func (s *BaseCypherParserListener) EnterStandaloneCall(ctx *StandaloneCallContext) {}

// ExitStandaloneCall is called when production standaloneCall is exited.
func (s *BaseCypherParserListener) ExitStandaloneCall(ctx *StandaloneCallContext) {}

// EnterExistsSubquery is called when production existsSubquery is entered.
func (s *BaseCypherParserListener) EnterExistsSubquery(ctx *ExistsSubqueryContext) {}

// ExitExistsSubquery is called when production existsSubquery is exited.
func (s *BaseCypherParserListener) ExitExistsSubquery(ctx *ExistsSubqueryContext) {}

// EnterCountSubquery is called when production countSubquery is entered.
func (s *BaseCypherParserListener) EnterCountSubquery(ctx *CountSubqueryContext) {}

// ExitCountSubquery is called when production countSubquery is exited.
func (s *BaseCypherParserListener) ExitCountSubquery(ctx *CountSubqueryContext) {}

// EnterCollectSubquery is called when production collectSubquery is entered.
func (s *BaseCypherParserListener) EnterCollectSubquery(ctx *CollectSubqueryContext) {}

// ExitCollectSubquery is called when production collectSubquery is exited.
func (s *BaseCypherParserListener) ExitCollectSubquery(ctx *CollectSubqueryContext) {}

// EnterCallSubquery is called when production callSubquery is entered.
func (s *BaseCypherParserListener) EnterCallSubquery(ctx *CallSubqueryContext) {}

// ExitCallSubquery is called when production callSubquery is exited.
func (s *BaseCypherParserListener) ExitCallSubquery(ctx *CallSubqueryContext) {}

// EnterSubqueryBody is called when production subqueryBody is entered.
func (s *BaseCypherParserListener) EnterSubqueryBody(ctx *SubqueryBodyContext) {}

// ExitSubqueryBody is called when production subqueryBody is exited.
func (s *BaseCypherParserListener) ExitSubqueryBody(ctx *SubqueryBodyContext) {}

// EnterReturnSt is called when production returnSt is entered.
func (s *BaseCypherParserListener) EnterReturnSt(ctx *ReturnStContext) {}

// ExitReturnSt is called when production returnSt is exited.
func (s *BaseCypherParserListener) ExitReturnSt(ctx *ReturnStContext) {}

// EnterWithSt is called when production withSt is entered.
func (s *BaseCypherParserListener) EnterWithSt(ctx *WithStContext) {}

// ExitWithSt is called when production withSt is exited.
func (s *BaseCypherParserListener) ExitWithSt(ctx *WithStContext) {}

// EnterEmbeddingSt is called when production embeddingSt is entered.
func (s *BaseCypherParserListener) EnterEmbeddingSt(ctx *EmbeddingStContext) {}

// ExitEmbeddingSt is called when production embeddingSt is exited.
func (s *BaseCypherParserListener) ExitEmbeddingSt(ctx *EmbeddingStContext) {}

// EnterSkipSt is called when production skipSt is entered.
func (s *BaseCypherParserListener) EnterSkipSt(ctx *SkipStContext) {}

// ExitSkipSt is called when production skipSt is exited.
func (s *BaseCypherParserListener) ExitSkipSt(ctx *SkipStContext) {}

// EnterLimitSt is called when production limitSt is entered.
func (s *BaseCypherParserListener) EnterLimitSt(ctx *LimitStContext) {}

// ExitLimitSt is called when production limitSt is exited.
func (s *BaseCypherParserListener) ExitLimitSt(ctx *LimitStContext) {}

// EnterProjectionBody is called when production projectionBody is entered.
func (s *BaseCypherParserListener) EnterProjectionBody(ctx *ProjectionBodyContext) {}

// ExitProjectionBody is called when production projectionBody is exited.
func (s *BaseCypherParserListener) ExitProjectionBody(ctx *ProjectionBodyContext) {}

// EnterProjectionItems is called when production projectionItems is entered.
func (s *BaseCypherParserListener) EnterProjectionItems(ctx *ProjectionItemsContext) {}

// ExitProjectionItems is called when production projectionItems is exited.
func (s *BaseCypherParserListener) ExitProjectionItems(ctx *ProjectionItemsContext) {}

// EnterProjectionItem is called when production projectionItem is entered.
func (s *BaseCypherParserListener) EnterProjectionItem(ctx *ProjectionItemContext) {}

// ExitProjectionItem is called when production projectionItem is exited.
func (s *BaseCypherParserListener) ExitProjectionItem(ctx *ProjectionItemContext) {}

// EnterOrderItem is called when production orderItem is entered.
func (s *BaseCypherParserListener) EnterOrderItem(ctx *OrderItemContext) {}

// ExitOrderItem is called when production orderItem is exited.
func (s *BaseCypherParserListener) ExitOrderItem(ctx *OrderItemContext) {}

// EnterOrderSt is called when production orderSt is entered.
func (s *BaseCypherParserListener) EnterOrderSt(ctx *OrderStContext) {}

// ExitOrderSt is called when production orderSt is exited.
func (s *BaseCypherParserListener) ExitOrderSt(ctx *OrderStContext) {}

// EnterSinglePartQ is called when production singlePartQ is entered.
func (s *BaseCypherParserListener) EnterSinglePartQ(ctx *SinglePartQContext) {}

// ExitSinglePartQ is called when production singlePartQ is exited.
func (s *BaseCypherParserListener) ExitSinglePartQ(ctx *SinglePartQContext) {}

// EnterMultiPartQ is called when production multiPartQ is entered.
func (s *BaseCypherParserListener) EnterMultiPartQ(ctx *MultiPartQContext) {}

// ExitMultiPartQ is called when production multiPartQ is exited.
func (s *BaseCypherParserListener) ExitMultiPartQ(ctx *MultiPartQContext) {}

// EnterMatchSt is called when production matchSt is entered.
func (s *BaseCypherParserListener) EnterMatchSt(ctx *MatchStContext) {}

// ExitMatchSt is called when production matchSt is exited.
func (s *BaseCypherParserListener) ExitMatchSt(ctx *MatchStContext) {}

// EnterUnwindSt is called when production unwindSt is entered.
func (s *BaseCypherParserListener) EnterUnwindSt(ctx *UnwindStContext) {}

// ExitUnwindSt is called when production unwindSt is exited.
func (s *BaseCypherParserListener) ExitUnwindSt(ctx *UnwindStContext) {}

// EnterReadingStatement is called when production readingStatement is entered.
func (s *BaseCypherParserListener) EnterReadingStatement(ctx *ReadingStatementContext) {}

// ExitReadingStatement is called when production readingStatement is exited.
func (s *BaseCypherParserListener) ExitReadingStatement(ctx *ReadingStatementContext) {}

// EnterUpdatingStatement is called when production updatingStatement is entered.
func (s *BaseCypherParserListener) EnterUpdatingStatement(ctx *UpdatingStatementContext) {}

// ExitUpdatingStatement is called when production updatingStatement is exited.
func (s *BaseCypherParserListener) ExitUpdatingStatement(ctx *UpdatingStatementContext) {}

// EnterDeleteSt is called when production deleteSt is entered.
func (s *BaseCypherParserListener) EnterDeleteSt(ctx *DeleteStContext) {}

// ExitDeleteSt is called when production deleteSt is exited.
func (s *BaseCypherParserListener) ExitDeleteSt(ctx *DeleteStContext) {}

// EnterRemoveSt is called when production removeSt is entered.
func (s *BaseCypherParserListener) EnterRemoveSt(ctx *RemoveStContext) {}

// ExitRemoveSt is called when production removeSt is exited.
func (s *BaseCypherParserListener) ExitRemoveSt(ctx *RemoveStContext) {}

// EnterRemoveItem is called when production removeItem is entered.
func (s *BaseCypherParserListener) EnterRemoveItem(ctx *RemoveItemContext) {}

// ExitRemoveItem is called when production removeItem is exited.
func (s *BaseCypherParserListener) ExitRemoveItem(ctx *RemoveItemContext) {}

// EnterForeachSt is called when production foreachSt is entered.
func (s *BaseCypherParserListener) EnterForeachSt(ctx *ForeachStContext) {}

// ExitForeachSt is called when production foreachSt is exited.
func (s *BaseCypherParserListener) ExitForeachSt(ctx *ForeachStContext) {}

// EnterQueryCallSt is called when production queryCallSt is entered.
func (s *BaseCypherParserListener) EnterQueryCallSt(ctx *QueryCallStContext) {}

// ExitQueryCallSt is called when production queryCallSt is exited.
func (s *BaseCypherParserListener) ExitQueryCallSt(ctx *QueryCallStContext) {}

// EnterParenExpressionChain is called when production parenExpressionChain is entered.
func (s *BaseCypherParserListener) EnterParenExpressionChain(ctx *ParenExpressionChainContext) {}

// ExitParenExpressionChain is called when production parenExpressionChain is exited.
func (s *BaseCypherParserListener) ExitParenExpressionChain(ctx *ParenExpressionChainContext) {}

// EnterYieldItems is called when production yieldItems is entered.
func (s *BaseCypherParserListener) EnterYieldItems(ctx *YieldItemsContext) {}

// ExitYieldItems is called when production yieldItems is exited.
func (s *BaseCypherParserListener) ExitYieldItems(ctx *YieldItemsContext) {}

// EnterYieldItem is called when production yieldItem is entered.
func (s *BaseCypherParserListener) EnterYieldItem(ctx *YieldItemContext) {}

// ExitYieldItem is called when production yieldItem is exited.
func (s *BaseCypherParserListener) ExitYieldItem(ctx *YieldItemContext) {}

// EnterMergeSt is called when production mergeSt is entered.
func (s *BaseCypherParserListener) EnterMergeSt(ctx *MergeStContext) {}

// ExitMergeSt is called when production mergeSt is exited.
func (s *BaseCypherParserListener) ExitMergeSt(ctx *MergeStContext) {}

// EnterMergeAction is called when production mergeAction is entered.
func (s *BaseCypherParserListener) EnterMergeAction(ctx *MergeActionContext) {}

// ExitMergeAction is called when production mergeAction is exited.
func (s *BaseCypherParserListener) ExitMergeAction(ctx *MergeActionContext) {}

// EnterSetSt is called when production setSt is entered.
func (s *BaseCypherParserListener) EnterSetSt(ctx *SetStContext) {}

// ExitSetSt is called when production setSt is exited.
func (s *BaseCypherParserListener) ExitSetSt(ctx *SetStContext) {}

// EnterSetItem is called when production setItem is entered.
func (s *BaseCypherParserListener) EnterSetItem(ctx *SetItemContext) {}

// ExitSetItem is called when production setItem is exited.
func (s *BaseCypherParserListener) ExitSetItem(ctx *SetItemContext) {}

// EnterNodeLabels is called when production nodeLabels is entered.
func (s *BaseCypherParserListener) EnterNodeLabels(ctx *NodeLabelsContext) {}

// ExitNodeLabels is called when production nodeLabels is exited.
func (s *BaseCypherParserListener) ExitNodeLabels(ctx *NodeLabelsContext) {}

// EnterLabelExpression is called when production labelExpression is entered.
func (s *BaseCypherParserListener) EnterLabelExpression(ctx *LabelExpressionContext) {}

// ExitLabelExpression is called when production labelExpression is exited.
func (s *BaseCypherParserListener) ExitLabelExpression(ctx *LabelExpressionContext) {}

// EnterLabelConjunction is called when production labelConjunction is entered.
func (s *BaseCypherParserListener) EnterLabelConjunction(ctx *LabelConjunctionContext) {}

// ExitLabelConjunction is called when production labelConjunction is exited.
func (s *BaseCypherParserListener) ExitLabelConjunction(ctx *LabelConjunctionContext) {}

// EnterLabelNegation is called when production labelNegation is entered.
func (s *BaseCypherParserListener) EnterLabelNegation(ctx *LabelNegationContext) {}

// ExitLabelNegation is called when production labelNegation is exited.
func (s *BaseCypherParserListener) ExitLabelNegation(ctx *LabelNegationContext) {}

// EnterCreateSt is called when production createSt is entered.
func (s *BaseCypherParserListener) EnterCreateSt(ctx *CreateStContext) {}

// ExitCreateSt is called when production createSt is exited.
func (s *BaseCypherParserListener) ExitCreateSt(ctx *CreateStContext) {}

// EnterPatternWhere is called when production patternWhere is entered.
func (s *BaseCypherParserListener) EnterPatternWhere(ctx *PatternWhereContext) {}

// ExitPatternWhere is called when production patternWhere is exited.
func (s *BaseCypherParserListener) ExitPatternWhere(ctx *PatternWhereContext) {}

// EnterWhere is called when production where is entered.
func (s *BaseCypherParserListener) EnterWhere(ctx *WhereContext) {}

// ExitWhere is called when production where is exited.
func (s *BaseCypherParserListener) ExitWhere(ctx *WhereContext) {}

// EnterPattern is called when production pattern is entered.
func (s *BaseCypherParserListener) EnterPattern(ctx *PatternContext) {}

// ExitPattern is called when production pattern is exited.
func (s *BaseCypherParserListener) ExitPattern(ctx *PatternContext) {}

// EnterExpression is called when production expression is entered.
func (s *BaseCypherParserListener) EnterExpression(ctx *ExpressionContext) {}

// ExitExpression is called when production expression is exited.
func (s *BaseCypherParserListener) ExitExpression(ctx *ExpressionContext) {}

// EnterXorExpression is called when production xorExpression is entered.
func (s *BaseCypherParserListener) EnterXorExpression(ctx *XorExpressionContext) {}

// ExitXorExpression is called when production xorExpression is exited.
func (s *BaseCypherParserListener) ExitXorExpression(ctx *XorExpressionContext) {}

// EnterAndExpression is called when production andExpression is entered.
func (s *BaseCypherParserListener) EnterAndExpression(ctx *AndExpressionContext) {}

// ExitAndExpression is called when production andExpression is exited.
func (s *BaseCypherParserListener) ExitAndExpression(ctx *AndExpressionContext) {}

// EnterNotExpression is called when production notExpression is entered.
func (s *BaseCypherParserListener) EnterNotExpression(ctx *NotExpressionContext) {}

// ExitNotExpression is called when production notExpression is exited.
func (s *BaseCypherParserListener) ExitNotExpression(ctx *NotExpressionContext) {}

// EnterComparisonExpression is called when production comparisonExpression is entered.
func (s *BaseCypherParserListener) EnterComparisonExpression(ctx *ComparisonExpressionContext) {}

// ExitComparisonExpression is called when production comparisonExpression is exited.
func (s *BaseCypherParserListener) ExitComparisonExpression(ctx *ComparisonExpressionContext) {}

// EnterComparisonSigns is called when production comparisonSigns is entered.
func (s *BaseCypherParserListener) EnterComparisonSigns(ctx *ComparisonSignsContext) {}

// ExitComparisonSigns is called when production comparisonSigns is exited.
func (s *BaseCypherParserListener) ExitComparisonSigns(ctx *ComparisonSignsContext) {}

// EnterAddSubExpression is called when production addSubExpression is entered.
func (s *BaseCypherParserListener) EnterAddSubExpression(ctx *AddSubExpressionContext) {}

// ExitAddSubExpression is called when production addSubExpression is exited.
func (s *BaseCypherParserListener) ExitAddSubExpression(ctx *AddSubExpressionContext) {}

// EnterMultDivExpression is called when production multDivExpression is entered.
func (s *BaseCypherParserListener) EnterMultDivExpression(ctx *MultDivExpressionContext) {}

// ExitMultDivExpression is called when production multDivExpression is exited.
func (s *BaseCypherParserListener) ExitMultDivExpression(ctx *MultDivExpressionContext) {}

// EnterPowerExpression is called when production powerExpression is entered.
func (s *BaseCypherParserListener) EnterPowerExpression(ctx *PowerExpressionContext) {}

// ExitPowerExpression is called when production powerExpression is exited.
func (s *BaseCypherParserListener) ExitPowerExpression(ctx *PowerExpressionContext) {}

// EnterUnaryAddSubExpression is called when production unaryAddSubExpression is entered.
func (s *BaseCypherParserListener) EnterUnaryAddSubExpression(ctx *UnaryAddSubExpressionContext) {}

// ExitUnaryAddSubExpression is called when production unaryAddSubExpression is exited.
func (s *BaseCypherParserListener) ExitUnaryAddSubExpression(ctx *UnaryAddSubExpressionContext) {}

// EnterAtomicExpression is called when production atomicExpression is entered.
func (s *BaseCypherParserListener) EnterAtomicExpression(ctx *AtomicExpressionContext) {}

// ExitAtomicExpression is called when production atomicExpression is exited.
func (s *BaseCypherParserListener) ExitAtomicExpression(ctx *AtomicExpressionContext) {}

// EnterNormalizationPredicate is called when production normalizationPredicate is entered.
func (s *BaseCypherParserListener) EnterNormalizationPredicate(ctx *NormalizationPredicateContext) {}

// ExitNormalizationPredicate is called when production normalizationPredicate is exited.
func (s *BaseCypherParserListener) ExitNormalizationPredicate(ctx *NormalizationPredicateContext) {}

// EnterLabelPredicate is called when production labelPredicate is entered.
func (s *BaseCypherParserListener) EnterLabelPredicate(ctx *LabelPredicateContext) {}

// ExitLabelPredicate is called when production labelPredicate is exited.
func (s *BaseCypherParserListener) ExitLabelPredicate(ctx *LabelPredicateContext) {}

// EnterListExpression is called when production listExpression is entered.
func (s *BaseCypherParserListener) EnterListExpression(ctx *ListExpressionContext) {}

// ExitListExpression is called when production listExpression is exited.
func (s *BaseCypherParserListener) ExitListExpression(ctx *ListExpressionContext) {}

// EnterStringExpression is called when production stringExpression is entered.
func (s *BaseCypherParserListener) EnterStringExpression(ctx *StringExpressionContext) {}

// ExitStringExpression is called when production stringExpression is exited.
func (s *BaseCypherParserListener) ExitStringExpression(ctx *StringExpressionContext) {}

// EnterStringExpPrefix is called when production stringExpPrefix is entered.
func (s *BaseCypherParserListener) EnterStringExpPrefix(ctx *StringExpPrefixContext) {}

// ExitStringExpPrefix is called when production stringExpPrefix is exited.
func (s *BaseCypherParserListener) ExitStringExpPrefix(ctx *StringExpPrefixContext) {}

// EnterNullExpression is called when production nullExpression is entered.
func (s *BaseCypherParserListener) EnterNullExpression(ctx *NullExpressionContext) {}

// ExitNullExpression is called when production nullExpression is exited.
func (s *BaseCypherParserListener) ExitNullExpression(ctx *NullExpressionContext) {}

// EnterTypePredicate is called when production typePredicate is entered.
func (s *BaseCypherParserListener) EnterTypePredicate(ctx *TypePredicateContext) {}

// ExitTypePredicate is called when production typePredicate is exited.
func (s *BaseCypherParserListener) ExitTypePredicate(ctx *TypePredicateContext) {}

// EnterExpressionType is called when production expressionType is entered.
func (s *BaseCypherParserListener) EnterExpressionType(ctx *ExpressionTypeContext) {}

// ExitExpressionType is called when production expressionType is exited.
func (s *BaseCypherParserListener) ExitExpressionType(ctx *ExpressionTypeContext) {}

// EnterExpressionTypePart is called when production expressionTypePart is entered.
func (s *BaseCypherParserListener) EnterExpressionTypePart(ctx *ExpressionTypePartContext) {}

// ExitExpressionTypePart is called when production expressionTypePart is exited.
func (s *BaseCypherParserListener) ExitExpressionTypePart(ctx *ExpressionTypePartContext) {}

// EnterPropertyOrLabelExpression is called when production propertyOrLabelExpression is entered.
func (s *BaseCypherParserListener) EnterPropertyOrLabelExpression(ctx *PropertyOrLabelExpressionContext) {
}

// ExitPropertyOrLabelExpression is called when production propertyOrLabelExpression is exited.
func (s *BaseCypherParserListener) ExitPropertyOrLabelExpression(ctx *PropertyOrLabelExpressionContext) {
}

// EnterPropertyExpression is called when production propertyExpression is entered.
func (s *BaseCypherParserListener) EnterPropertyExpression(ctx *PropertyExpressionContext) {}

// ExitPropertyExpression is called when production propertyExpression is exited.
func (s *BaseCypherParserListener) ExitPropertyExpression(ctx *PropertyExpressionContext) {}

// EnterPatternPart is called when production patternPart is entered.
func (s *BaseCypherParserListener) EnterPatternPart(ctx *PatternPartContext) {}

// ExitPatternPart is called when production patternPart is exited.
func (s *BaseCypherParserListener) ExitPatternPart(ctx *PatternPartContext) {}

// EnterPathFunction is called when production pathFunction is entered.
func (s *BaseCypherParserListener) EnterPathFunction(ctx *PathFunctionContext) {}

// ExitPathFunction is called when production pathFunction is exited.
func (s *BaseCypherParserListener) ExitPathFunction(ctx *PathFunctionContext) {}

// EnterPatternElem is called when production patternElem is entered.
func (s *BaseCypherParserListener) EnterPatternElem(ctx *PatternElemContext) {}

// ExitPatternElem is called when production patternElem is exited.
func (s *BaseCypherParserListener) ExitPatternElem(ctx *PatternElemContext) {}

// EnterPatternElemChain is called when production patternElemChain is entered.
func (s *BaseCypherParserListener) EnterPatternElemChain(ctx *PatternElemChainContext) {}

// ExitPatternElemChain is called when production patternElemChain is exited.
func (s *BaseCypherParserListener) ExitPatternElemChain(ctx *PatternElemChainContext) {}

// EnterRelationshipQuantifier is called when production relationshipQuantifier is entered.
func (s *BaseCypherParserListener) EnterRelationshipQuantifier(ctx *RelationshipQuantifierContext) {}

// ExitRelationshipQuantifier is called when production relationshipQuantifier is exited.
func (s *BaseCypherParserListener) ExitRelationshipQuantifier(ctx *RelationshipQuantifierContext) {}

// EnterProperties is called when production properties is entered.
func (s *BaseCypherParserListener) EnterProperties(ctx *PropertiesContext) {}

// ExitProperties is called when production properties is exited.
func (s *BaseCypherParserListener) ExitProperties(ctx *PropertiesContext) {}

// EnterNodePattern is called when production nodePattern is entered.
func (s *BaseCypherParserListener) EnterNodePattern(ctx *NodePatternContext) {}

// ExitNodePattern is called when production nodePattern is exited.
func (s *BaseCypherParserListener) ExitNodePattern(ctx *NodePatternContext) {}

// EnterAtom is called when production atom is entered.
func (s *BaseCypherParserListener) EnterAtom(ctx *AtomContext) {}

// ExitAtom is called when production atom is exited.
func (s *BaseCypherParserListener) ExitAtom(ctx *AtomContext) {}

// EnterMapProjection is called when production mapProjection is entered.
func (s *BaseCypherParserListener) EnterMapProjection(ctx *MapProjectionContext) {}

// ExitMapProjection is called when production mapProjection is exited.
func (s *BaseCypherParserListener) ExitMapProjection(ctx *MapProjectionContext) {}

// EnterMapProjectionItem is called when production mapProjectionItem is entered.
func (s *BaseCypherParserListener) EnterMapProjectionItem(ctx *MapProjectionItemContext) {}

// ExitMapProjectionItem is called when production mapProjectionItem is exited.
func (s *BaseCypherParserListener) ExitMapProjectionItem(ctx *MapProjectionItemContext) {}

// EnterLhs is called when production lhs is entered.
func (s *BaseCypherParserListener) EnterLhs(ctx *LhsContext) {}

// ExitLhs is called when production lhs is exited.
func (s *BaseCypherParserListener) ExitLhs(ctx *LhsContext) {}

// EnterRelationshipPattern is called when production relationshipPattern is entered.
func (s *BaseCypherParserListener) EnterRelationshipPattern(ctx *RelationshipPatternContext) {}

// ExitRelationshipPattern is called when production relationshipPattern is exited.
func (s *BaseCypherParserListener) ExitRelationshipPattern(ctx *RelationshipPatternContext) {}

// EnterRelationDetail is called when production relationDetail is entered.
func (s *BaseCypherParserListener) EnterRelationDetail(ctx *RelationDetailContext) {}

// ExitRelationDetail is called when production relationDetail is exited.
func (s *BaseCypherParserListener) ExitRelationDetail(ctx *RelationDetailContext) {}

// EnterRelationshipTypes is called when production relationshipTypes is entered.
func (s *BaseCypherParserListener) EnterRelationshipTypes(ctx *RelationshipTypesContext) {}

// ExitRelationshipTypes is called when production relationshipTypes is exited.
func (s *BaseCypherParserListener) ExitRelationshipTypes(ctx *RelationshipTypesContext) {}

// EnterUnionSt is called when production unionSt is entered.
func (s *BaseCypherParserListener) EnterUnionSt(ctx *UnionStContext) {}

// ExitUnionSt is called when production unionSt is exited.
func (s *BaseCypherParserListener) ExitUnionSt(ctx *UnionStContext) {}

// EnterSubqueryExist is called when production subqueryExist is entered.
func (s *BaseCypherParserListener) EnterSubqueryExist(ctx *SubqueryExistContext) {}

// ExitSubqueryExist is called when production subqueryExist is exited.
func (s *BaseCypherParserListener) ExitSubqueryExist(ctx *SubqueryExistContext) {}

// EnterInvocationName is called when production invocationName is entered.
func (s *BaseCypherParserListener) EnterInvocationName(ctx *InvocationNameContext) {}

// ExitInvocationName is called when production invocationName is exited.
func (s *BaseCypherParserListener) ExitInvocationName(ctx *InvocationNameContext) {}

// EnterFunctionInvocation is called when production functionInvocation is entered.
func (s *BaseCypherParserListener) EnterFunctionInvocation(ctx *FunctionInvocationContext) {}

// ExitFunctionInvocation is called when production functionInvocation is exited.
func (s *BaseCypherParserListener) ExitFunctionInvocation(ctx *FunctionInvocationContext) {}

// EnterParenthesizedExpression is called when production parenthesizedExpression is entered.
func (s *BaseCypherParserListener) EnterParenthesizedExpression(ctx *ParenthesizedExpressionContext) {
}

// ExitParenthesizedExpression is called when production parenthesizedExpression is exited.
func (s *BaseCypherParserListener) ExitParenthesizedExpression(ctx *ParenthesizedExpressionContext) {}

// EnterFilterWith is called when production filterWith is entered.
func (s *BaseCypherParserListener) EnterFilterWith(ctx *FilterWithContext) {}

// ExitFilterWith is called when production filterWith is exited.
func (s *BaseCypherParserListener) ExitFilterWith(ctx *FilterWithContext) {}

// EnterPatternComprehension is called when production patternComprehension is entered.
func (s *BaseCypherParserListener) EnterPatternComprehension(ctx *PatternComprehensionContext) {}

// ExitPatternComprehension is called when production patternComprehension is exited.
func (s *BaseCypherParserListener) ExitPatternComprehension(ctx *PatternComprehensionContext) {}

// EnterRelationshipsChainPattern is called when production relationshipsChainPattern is entered.
func (s *BaseCypherParserListener) EnterRelationshipsChainPattern(ctx *RelationshipsChainPatternContext) {
}

// ExitRelationshipsChainPattern is called when production relationshipsChainPattern is exited.
func (s *BaseCypherParserListener) ExitRelationshipsChainPattern(ctx *RelationshipsChainPatternContext) {
}

// EnterListComprehension is called when production listComprehension is entered.
func (s *BaseCypherParserListener) EnterListComprehension(ctx *ListComprehensionContext) {}

// ExitListComprehension is called when production listComprehension is exited.
func (s *BaseCypherParserListener) ExitListComprehension(ctx *ListComprehensionContext) {}

// EnterFilterExpression is called when production filterExpression is entered.
func (s *BaseCypherParserListener) EnterFilterExpression(ctx *FilterExpressionContext) {}

// ExitFilterExpression is called when production filterExpression is exited.
func (s *BaseCypherParserListener) ExitFilterExpression(ctx *FilterExpressionContext) {}

// EnterCountAll is called when production countAll is entered.
func (s *BaseCypherParserListener) EnterCountAll(ctx *CountAllContext) {}

// ExitCountAll is called when production countAll is exited.
func (s *BaseCypherParserListener) ExitCountAll(ctx *CountAllContext) {}

// EnterExpressionChain is called when production expressionChain is entered.
func (s *BaseCypherParserListener) EnterExpressionChain(ctx *ExpressionChainContext) {}

// ExitExpressionChain is called when production expressionChain is exited.
func (s *BaseCypherParserListener) ExitExpressionChain(ctx *ExpressionChainContext) {}

// EnterCaseExpression is called when production caseExpression is entered.
func (s *BaseCypherParserListener) EnterCaseExpression(ctx *CaseExpressionContext) {}

// ExitCaseExpression is called when production caseExpression is exited.
func (s *BaseCypherParserListener) ExitCaseExpression(ctx *CaseExpressionContext) {}

// EnterReduceExpression is called when production reduceExpression is entered.
func (s *BaseCypherParserListener) EnterReduceExpression(ctx *ReduceExpressionContext) {}

// ExitReduceExpression is called when production reduceExpression is exited.
func (s *BaseCypherParserListener) ExitReduceExpression(ctx *ReduceExpressionContext) {}

// EnterParameter is called when production parameter is entered.
func (s *BaseCypherParserListener) EnterParameter(ctx *ParameterContext) {}

// ExitParameter is called when production parameter is exited.
func (s *BaseCypherParserListener) ExitParameter(ctx *ParameterContext) {}

// EnterLiteral is called when production literal is entered.
func (s *BaseCypherParserListener) EnterLiteral(ctx *LiteralContext) {}

// ExitLiteral is called when production literal is exited.
func (s *BaseCypherParserListener) ExitLiteral(ctx *LiteralContext) {}

// EnterRangeLit is called when production rangeLit is entered.
func (s *BaseCypherParserListener) EnterRangeLit(ctx *RangeLitContext) {}

// ExitRangeLit is called when production rangeLit is exited.
func (s *BaseCypherParserListener) ExitRangeLit(ctx *RangeLitContext) {}

// EnterBoolLit is called when production boolLit is entered.
func (s *BaseCypherParserListener) EnterBoolLit(ctx *BoolLitContext) {}

// ExitBoolLit is called when production boolLit is exited.
func (s *BaseCypherParserListener) ExitBoolLit(ctx *BoolLitContext) {}

// EnterIntegerLit is called when production integerLit is entered.
func (s *BaseCypherParserListener) EnterIntegerLit(ctx *IntegerLitContext) {}

// ExitIntegerLit is called when production integerLit is exited.
func (s *BaseCypherParserListener) ExitIntegerLit(ctx *IntegerLitContext) {}

// EnterNumLit is called when production numLit is entered.
func (s *BaseCypherParserListener) EnterNumLit(ctx *NumLitContext) {}

// ExitNumLit is called when production numLit is exited.
func (s *BaseCypherParserListener) ExitNumLit(ctx *NumLitContext) {}

// EnterStringLit is called when production stringLit is entered.
func (s *BaseCypherParserListener) EnterStringLit(ctx *StringLitContext) {}

// ExitStringLit is called when production stringLit is exited.
func (s *BaseCypherParserListener) ExitStringLit(ctx *StringLitContext) {}

// EnterCharLit is called when production charLit is entered.
func (s *BaseCypherParserListener) EnterCharLit(ctx *CharLitContext) {}

// ExitCharLit is called when production charLit is exited.
func (s *BaseCypherParserListener) ExitCharLit(ctx *CharLitContext) {}

// EnterListLit is called when production listLit is entered.
func (s *BaseCypherParserListener) EnterListLit(ctx *ListLitContext) {}

// ExitListLit is called when production listLit is exited.
func (s *BaseCypherParserListener) ExitListLit(ctx *ListLitContext) {}

// EnterMapLit is called when production mapLit is entered.
func (s *BaseCypherParserListener) EnterMapLit(ctx *MapLitContext) {}

// ExitMapLit is called when production mapLit is exited.
func (s *BaseCypherParserListener) ExitMapLit(ctx *MapLitContext) {}

// EnterMapPair is called when production mapPair is entered.
func (s *BaseCypherParserListener) EnterMapPair(ctx *MapPairContext) {}

// ExitMapPair is called when production mapPair is exited.
func (s *BaseCypherParserListener) ExitMapPair(ctx *MapPairContext) {}

// EnterName is called when production name is entered.
func (s *BaseCypherParserListener) EnterName(ctx *NameContext) {}

// ExitName is called when production name is exited.
func (s *BaseCypherParserListener) ExitName(ctx *NameContext) {}

// EnterSymbol is called when production symbol is entered.
func (s *BaseCypherParserListener) EnterSymbol(ctx *SymbolContext) {}

// ExitSymbol is called when production symbol is exited.
func (s *BaseCypherParserListener) ExitSymbol(ctx *SymbolContext) {}

// EnterReservedWord is called when production reservedWord is entered.
func (s *BaseCypherParserListener) EnterReservedWord(ctx *ReservedWordContext) {}

// ExitReservedWord is called when production reservedWord is exited.
func (s *BaseCypherParserListener) ExitReservedWord(ctx *ReservedWordContext) {}
