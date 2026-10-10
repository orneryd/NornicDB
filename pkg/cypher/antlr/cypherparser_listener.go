// Code generated from CypherParser.g4 by ANTLR 4.13.1. DO NOT EDIT.

package antlr // CypherParser
import "github.com/antlr4-go/antlr/v4"

// CypherParserListener is a complete listener for a parse tree produced by CypherParser.
type CypherParserListener interface {
	antlr.ParseTreeListener

	// EnterScript is called when entering the script production.
	EnterScript(c *ScriptContext)

	// EnterCypherPreamble is called when entering the cypherPreamble production.
	EnterCypherPreamble(c *CypherPreambleContext)

	// EnterCypherGroup is called when entering the cypherGroup production.
	EnterCypherGroup(c *CypherGroupContext)

	// EnterCypherOption is called when entering the cypherOption production.
	EnterCypherOption(c *CypherOptionContext)

	// EnterShellCommand is called when entering the shellCommand production.
	EnterShellCommand(c *ShellCommandContext)

	// EnterShellCommandElement is called when entering the shellCommandElement production.
	EnterShellCommandElement(c *ShellCommandElementContext)

	// EnterTransactionStatement is called when entering the transactionStatement production.
	EnterTransactionStatement(c *TransactionStatementContext)

	// EnterQuery is called when entering the query production.
	EnterQuery(c *QueryContext)

	// EnterUseClause is called when entering the useClause production.
	EnterUseClause(c *UseClauseContext)

	// EnterQueryPrefix is called when entering the queryPrefix production.
	EnterQueryPrefix(c *QueryPrefixContext)

	// EnterShowCommand is called when entering the showCommand production.
	EnterShowCommand(c *ShowCommandContext)

	// EnterShowConstraintKind is called when entering the showConstraintKind production.
	EnterShowConstraintKind(c *ShowConstraintKindContext)

	// EnterShowTail is called when entering the showTail production.
	EnterShowTail(c *ShowTailContext)

	// EnterTerminateCommand is called when entering the terminateCommand production.
	EnterTerminateCommand(c *TerminateCommandContext)

	// EnterAdministrationCommand is called when entering the administrationCommand production.
	EnterAdministrationCommand(c *AdministrationCommandContext)

	// EnterQualifiedName is called when entering the qualifiedName production.
	EnterQualifiedName(c *QualifiedNameContext)

	// EnterSchemaCommand is called when entering the schemaCommand production.
	EnterSchemaCommand(c *SchemaCommandContext)

	// EnterConstraintRequirement is called when entering the constraintRequirement production.
	EnterConstraintRequirement(c *ConstraintRequirementContext)

	// EnterConstraintBlock is called when entering the constraintBlock production.
	EnterConstraintBlock(c *ConstraintBlockContext)

	// EnterPropertyTypeName is called when entering the propertyTypeName production.
	EnterPropertyTypeName(c *PropertyTypeNameContext)

	// EnterRegularQuery is called when entering the regularQuery production.
	EnterRegularQuery(c *RegularQueryContext)

	// EnterSingleQuery is called when entering the singleQuery production.
	EnterSingleQuery(c *SingleQueryContext)

	// EnterStandaloneCall is called when entering the standaloneCall production.
	EnterStandaloneCall(c *StandaloneCallContext)

	// EnterExistsSubquery is called when entering the existsSubquery production.
	EnterExistsSubquery(c *ExistsSubqueryContext)

	// EnterCountSubquery is called when entering the countSubquery production.
	EnterCountSubquery(c *CountSubqueryContext)

	// EnterCollectSubquery is called when entering the collectSubquery production.
	EnterCollectSubquery(c *CollectSubqueryContext)

	// EnterCallSubquery is called when entering the callSubquery production.
	EnterCallSubquery(c *CallSubqueryContext)

	// EnterSubqueryBody is called when entering the subqueryBody production.
	EnterSubqueryBody(c *SubqueryBodyContext)

	// EnterReturnSt is called when entering the returnSt production.
	EnterReturnSt(c *ReturnStContext)

	// EnterWithSt is called when entering the withSt production.
	EnterWithSt(c *WithStContext)

	// EnterEmbeddingSt is called when entering the embeddingSt production.
	EnterEmbeddingSt(c *EmbeddingStContext)

	// EnterSkipSt is called when entering the skipSt production.
	EnterSkipSt(c *SkipStContext)

	// EnterLimitSt is called when entering the limitSt production.
	EnterLimitSt(c *LimitStContext)

	// EnterProjectionBody is called when entering the projectionBody production.
	EnterProjectionBody(c *ProjectionBodyContext)

	// EnterProjectionItems is called when entering the projectionItems production.
	EnterProjectionItems(c *ProjectionItemsContext)

	// EnterProjectionItem is called when entering the projectionItem production.
	EnterProjectionItem(c *ProjectionItemContext)

	// EnterOrderItem is called when entering the orderItem production.
	EnterOrderItem(c *OrderItemContext)

	// EnterOrderSt is called when entering the orderSt production.
	EnterOrderSt(c *OrderStContext)

	// EnterSinglePartQ is called when entering the singlePartQ production.
	EnterSinglePartQ(c *SinglePartQContext)

	// EnterMultiPartQ is called when entering the multiPartQ production.
	EnterMultiPartQ(c *MultiPartQContext)

	// EnterMatchSt is called when entering the matchSt production.
	EnterMatchSt(c *MatchStContext)

	// EnterUnwindSt is called when entering the unwindSt production.
	EnterUnwindSt(c *UnwindStContext)

	// EnterLetSt is called when entering the letSt production.
	EnterLetSt(c *LetStContext)

	// EnterLetItem is called when entering the letItem production.
	EnterLetItem(c *LetItemContext)

	// EnterFilterSt is called when entering the filterSt production.
	EnterFilterSt(c *FilterStContext)

	// EnterForSt is called when entering the forSt production.
	EnterForSt(c *ForStContext)

	// EnterReadingStatement is called when entering the readingStatement production.
	EnterReadingStatement(c *ReadingStatementContext)

	// EnterUpdatingStatement is called when entering the updatingStatement production.
	EnterUpdatingStatement(c *UpdatingStatementContext)

	// EnterDeleteSt is called when entering the deleteSt production.
	EnterDeleteSt(c *DeleteStContext)

	// EnterRemoveSt is called when entering the removeSt production.
	EnterRemoveSt(c *RemoveStContext)

	// EnterRemoveItem is called when entering the removeItem production.
	EnterRemoveItem(c *RemoveItemContext)

	// EnterForeachSt is called when entering the foreachSt production.
	EnterForeachSt(c *ForeachStContext)

	// EnterQueryCallSt is called when entering the queryCallSt production.
	EnterQueryCallSt(c *QueryCallStContext)

	// EnterParenExpressionChain is called when entering the parenExpressionChain production.
	EnterParenExpressionChain(c *ParenExpressionChainContext)

	// EnterYieldItems is called when entering the yieldItems production.
	EnterYieldItems(c *YieldItemsContext)

	// EnterYieldItem is called when entering the yieldItem production.
	EnterYieldItem(c *YieldItemContext)

	// EnterMergeSt is called when entering the mergeSt production.
	EnterMergeSt(c *MergeStContext)

	// EnterMergeAction is called when entering the mergeAction production.
	EnterMergeAction(c *MergeActionContext)

	// EnterSetSt is called when entering the setSt production.
	EnterSetSt(c *SetStContext)

	// EnterSetItem is called when entering the setItem production.
	EnterSetItem(c *SetItemContext)

	// EnterDynamicPropertyExpression is called when entering the dynamicPropertyExpression production.
	EnterDynamicPropertyExpression(c *DynamicPropertyExpressionContext)

	// EnterNodeLabels is called when entering the nodeLabels production.
	EnterNodeLabels(c *NodeLabelsContext)

	// EnterLabelExpression is called when entering the labelExpression production.
	EnterLabelExpression(c *LabelExpressionContext)

	// EnterLabelConjunction is called when entering the labelConjunction production.
	EnterLabelConjunction(c *LabelConjunctionContext)

	// EnterLabelNegation is called when entering the labelNegation production.
	EnterLabelNegation(c *LabelNegationContext)

	// EnterDynamicLabel is called when entering the dynamicLabel production.
	EnterDynamicLabel(c *DynamicLabelContext)

	// EnterCreateSt is called when entering the createSt production.
	EnterCreateSt(c *CreateStContext)

	// EnterPatternWhere is called when entering the patternWhere production.
	EnterPatternWhere(c *PatternWhereContext)

	// EnterWhere is called when entering the where production.
	EnterWhere(c *WhereContext)

	// EnterPattern is called when entering the pattern production.
	EnterPattern(c *PatternContext)

	// EnterExpression is called when entering the expression production.
	EnterExpression(c *ExpressionContext)

	// EnterXorExpression is called when entering the xorExpression production.
	EnterXorExpression(c *XorExpressionContext)

	// EnterAndExpression is called when entering the andExpression production.
	EnterAndExpression(c *AndExpressionContext)

	// EnterNotExpression is called when entering the notExpression production.
	EnterNotExpression(c *NotExpressionContext)

	// EnterComparisonExpression is called when entering the comparisonExpression production.
	EnterComparisonExpression(c *ComparisonExpressionContext)

	// EnterComparisonSigns is called when entering the comparisonSigns production.
	EnterComparisonSigns(c *ComparisonSignsContext)

	// EnterAddSubExpression is called when entering the addSubExpression production.
	EnterAddSubExpression(c *AddSubExpressionContext)

	// EnterMultDivExpression is called when entering the multDivExpression production.
	EnterMultDivExpression(c *MultDivExpressionContext)

	// EnterPowerExpression is called when entering the powerExpression production.
	EnterPowerExpression(c *PowerExpressionContext)

	// EnterUnaryAddSubExpression is called when entering the unaryAddSubExpression production.
	EnterUnaryAddSubExpression(c *UnaryAddSubExpressionContext)

	// EnterAtomicExpression is called when entering the atomicExpression production.
	EnterAtomicExpression(c *AtomicExpressionContext)

	// EnterNormalizationPredicate is called when entering the normalizationPredicate production.
	EnterNormalizationPredicate(c *NormalizationPredicateContext)

	// EnterLabelPredicate is called when entering the labelPredicate production.
	EnterLabelPredicate(c *LabelPredicateContext)

	// EnterListExpression is called when entering the listExpression production.
	EnterListExpression(c *ListExpressionContext)

	// EnterStringExpression is called when entering the stringExpression production.
	EnterStringExpression(c *StringExpressionContext)

	// EnterStringExpPrefix is called when entering the stringExpPrefix production.
	EnterStringExpPrefix(c *StringExpPrefixContext)

	// EnterNullExpression is called when entering the nullExpression production.
	EnterNullExpression(c *NullExpressionContext)

	// EnterTypePredicate is called when entering the typePredicate production.
	EnterTypePredicate(c *TypePredicateContext)

	// EnterExpressionType is called when entering the expressionType production.
	EnterExpressionType(c *ExpressionTypeContext)

	// EnterExpressionTypePart is called when entering the expressionTypePart production.
	EnterExpressionTypePart(c *ExpressionTypePartContext)

	// EnterPropertyOrLabelExpression is called when entering the propertyOrLabelExpression production.
	EnterPropertyOrLabelExpression(c *PropertyOrLabelExpressionContext)

	// EnterPropertyExpression is called when entering the propertyExpression production.
	EnterPropertyExpression(c *PropertyExpressionContext)

	// EnterPatternPart is called when entering the patternPart production.
	EnterPatternPart(c *PatternPartContext)

	// EnterPathFunction is called when entering the pathFunction production.
	EnterPathFunction(c *PathFunctionContext)

	// EnterPatternElem is called when entering the patternElem production.
	EnterPatternElem(c *PatternElemContext)

	// EnterPatternElemStart is called when entering the patternElemStart production.
	EnterPatternElemStart(c *PatternElemStartContext)

	// EnterPatternElemPart is called when entering the patternElemPart production.
	EnterPatternElemPart(c *PatternElemPartContext)

	// EnterQuantifiedPath is called when entering the quantifiedPath production.
	EnterQuantifiedPath(c *QuantifiedPathContext)

	// EnterPatternElemChain is called when entering the patternElemChain production.
	EnterPatternElemChain(c *PatternElemChainContext)

	// EnterRelationshipQuantifier is called when entering the relationshipQuantifier production.
	EnterRelationshipQuantifier(c *RelationshipQuantifierContext)

	// EnterProperties is called when entering the properties production.
	EnterProperties(c *PropertiesContext)

	// EnterNodePattern is called when entering the nodePattern production.
	EnterNodePattern(c *NodePatternContext)

	// EnterAtom is called when entering the atom production.
	EnterAtom(c *AtomContext)

	// EnterMapProjection is called when entering the mapProjection production.
	EnterMapProjection(c *MapProjectionContext)

	// EnterMapProjectionItem is called when entering the mapProjectionItem production.
	EnterMapProjectionItem(c *MapProjectionItemContext)

	// EnterLhs is called when entering the lhs production.
	EnterLhs(c *LhsContext)

	// EnterRelationshipPattern is called when entering the relationshipPattern production.
	EnterRelationshipPattern(c *RelationshipPatternContext)

	// EnterRelationDetail is called when entering the relationDetail production.
	EnterRelationDetail(c *RelationDetailContext)

	// EnterRelationshipTypes is called when entering the relationshipTypes production.
	EnterRelationshipTypes(c *RelationshipTypesContext)

	// EnterUnionSt is called when entering the unionSt production.
	EnterUnionSt(c *UnionStContext)

	// EnterSubqueryExist is called when entering the subqueryExist production.
	EnterSubqueryExist(c *SubqueryExistContext)

	// EnterInvocationName is called when entering the invocationName production.
	EnterInvocationName(c *InvocationNameContext)

	// EnterFunctionInvocation is called when entering the functionInvocation production.
	EnterFunctionInvocation(c *FunctionInvocationContext)

	// EnterParenthesizedExpression is called when entering the parenthesizedExpression production.
	EnterParenthesizedExpression(c *ParenthesizedExpressionContext)

	// EnterFilterWith is called when entering the filterWith production.
	EnterFilterWith(c *FilterWithContext)

	// EnterPatternComprehension is called when entering the patternComprehension production.
	EnterPatternComprehension(c *PatternComprehensionContext)

	// EnterRelationshipsChainPattern is called when entering the relationshipsChainPattern production.
	EnterRelationshipsChainPattern(c *RelationshipsChainPatternContext)

	// EnterListComprehension is called when entering the listComprehension production.
	EnterListComprehension(c *ListComprehensionContext)

	// EnterFilterExpression is called when entering the filterExpression production.
	EnterFilterExpression(c *FilterExpressionContext)

	// EnterCountAll is called when entering the countAll production.
	EnterCountAll(c *CountAllContext)

	// EnterExpressionChain is called when entering the expressionChain production.
	EnterExpressionChain(c *ExpressionChainContext)

	// EnterCaseExpression is called when entering the caseExpression production.
	EnterCaseExpression(c *CaseExpressionContext)

	// EnterReduceExpression is called when entering the reduceExpression production.
	EnterReduceExpression(c *ReduceExpressionContext)

	// EnterParameter is called when entering the parameter production.
	EnterParameter(c *ParameterContext)

	// EnterLiteral is called when entering the literal production.
	EnterLiteral(c *LiteralContext)

	// EnterRangeLit is called when entering the rangeLit production.
	EnterRangeLit(c *RangeLitContext)

	// EnterBoolLit is called when entering the boolLit production.
	EnterBoolLit(c *BoolLitContext)

	// EnterIntegerLit is called when entering the integerLit production.
	EnterIntegerLit(c *IntegerLitContext)

	// EnterNumLit is called when entering the numLit production.
	EnterNumLit(c *NumLitContext)

	// EnterStringLit is called when entering the stringLit production.
	EnterStringLit(c *StringLitContext)

	// EnterCharLit is called when entering the charLit production.
	EnterCharLit(c *CharLitContext)

	// EnterListLit is called when entering the listLit production.
	EnterListLit(c *ListLitContext)

	// EnterMapLit is called when entering the mapLit production.
	EnterMapLit(c *MapLitContext)

	// EnterMapPair is called when entering the mapPair production.
	EnterMapPair(c *MapPairContext)

	// EnterName is called when entering the name production.
	EnterName(c *NameContext)

	// EnterSymbol is called when entering the symbol production.
	EnterSymbol(c *SymbolContext)

	// EnterReservedWord is called when entering the reservedWord production.
	EnterReservedWord(c *ReservedWordContext)

	// ExitScript is called when exiting the script production.
	ExitScript(c *ScriptContext)

	// ExitCypherPreamble is called when exiting the cypherPreamble production.
	ExitCypherPreamble(c *CypherPreambleContext)

	// ExitCypherGroup is called when exiting the cypherGroup production.
	ExitCypherGroup(c *CypherGroupContext)

	// ExitCypherOption is called when exiting the cypherOption production.
	ExitCypherOption(c *CypherOptionContext)

	// ExitShellCommand is called when exiting the shellCommand production.
	ExitShellCommand(c *ShellCommandContext)

	// ExitShellCommandElement is called when exiting the shellCommandElement production.
	ExitShellCommandElement(c *ShellCommandElementContext)

	// ExitTransactionStatement is called when exiting the transactionStatement production.
	ExitTransactionStatement(c *TransactionStatementContext)

	// ExitQuery is called when exiting the query production.
	ExitQuery(c *QueryContext)

	// ExitUseClause is called when exiting the useClause production.
	ExitUseClause(c *UseClauseContext)

	// ExitQueryPrefix is called when exiting the queryPrefix production.
	ExitQueryPrefix(c *QueryPrefixContext)

	// ExitShowCommand is called when exiting the showCommand production.
	ExitShowCommand(c *ShowCommandContext)

	// ExitShowConstraintKind is called when exiting the showConstraintKind production.
	ExitShowConstraintKind(c *ShowConstraintKindContext)

	// ExitShowTail is called when exiting the showTail production.
	ExitShowTail(c *ShowTailContext)

	// ExitTerminateCommand is called when exiting the terminateCommand production.
	ExitTerminateCommand(c *TerminateCommandContext)

	// ExitAdministrationCommand is called when exiting the administrationCommand production.
	ExitAdministrationCommand(c *AdministrationCommandContext)

	// ExitQualifiedName is called when exiting the qualifiedName production.
	ExitQualifiedName(c *QualifiedNameContext)

	// ExitSchemaCommand is called when exiting the schemaCommand production.
	ExitSchemaCommand(c *SchemaCommandContext)

	// ExitConstraintRequirement is called when exiting the constraintRequirement production.
	ExitConstraintRequirement(c *ConstraintRequirementContext)

	// ExitConstraintBlock is called when exiting the constraintBlock production.
	ExitConstraintBlock(c *ConstraintBlockContext)

	// ExitPropertyTypeName is called when exiting the propertyTypeName production.
	ExitPropertyTypeName(c *PropertyTypeNameContext)

	// ExitRegularQuery is called when exiting the regularQuery production.
	ExitRegularQuery(c *RegularQueryContext)

	// ExitSingleQuery is called when exiting the singleQuery production.
	ExitSingleQuery(c *SingleQueryContext)

	// ExitStandaloneCall is called when exiting the standaloneCall production.
	ExitStandaloneCall(c *StandaloneCallContext)

	// ExitExistsSubquery is called when exiting the existsSubquery production.
	ExitExistsSubquery(c *ExistsSubqueryContext)

	// ExitCountSubquery is called when exiting the countSubquery production.
	ExitCountSubquery(c *CountSubqueryContext)

	// ExitCollectSubquery is called when exiting the collectSubquery production.
	ExitCollectSubquery(c *CollectSubqueryContext)

	// ExitCallSubquery is called when exiting the callSubquery production.
	ExitCallSubquery(c *CallSubqueryContext)

	// ExitSubqueryBody is called when exiting the subqueryBody production.
	ExitSubqueryBody(c *SubqueryBodyContext)

	// ExitReturnSt is called when exiting the returnSt production.
	ExitReturnSt(c *ReturnStContext)

	// ExitWithSt is called when exiting the withSt production.
	ExitWithSt(c *WithStContext)

	// ExitEmbeddingSt is called when exiting the embeddingSt production.
	ExitEmbeddingSt(c *EmbeddingStContext)

	// ExitSkipSt is called when exiting the skipSt production.
	ExitSkipSt(c *SkipStContext)

	// ExitLimitSt is called when exiting the limitSt production.
	ExitLimitSt(c *LimitStContext)

	// ExitProjectionBody is called when exiting the projectionBody production.
	ExitProjectionBody(c *ProjectionBodyContext)

	// ExitProjectionItems is called when exiting the projectionItems production.
	ExitProjectionItems(c *ProjectionItemsContext)

	// ExitProjectionItem is called when exiting the projectionItem production.
	ExitProjectionItem(c *ProjectionItemContext)

	// ExitOrderItem is called when exiting the orderItem production.
	ExitOrderItem(c *OrderItemContext)

	// ExitOrderSt is called when exiting the orderSt production.
	ExitOrderSt(c *OrderStContext)

	// ExitSinglePartQ is called when exiting the singlePartQ production.
	ExitSinglePartQ(c *SinglePartQContext)

	// ExitMultiPartQ is called when exiting the multiPartQ production.
	ExitMultiPartQ(c *MultiPartQContext)

	// ExitMatchSt is called when exiting the matchSt production.
	ExitMatchSt(c *MatchStContext)

	// ExitUnwindSt is called when exiting the unwindSt production.
	ExitUnwindSt(c *UnwindStContext)

	// ExitLetSt is called when exiting the letSt production.
	ExitLetSt(c *LetStContext)

	// ExitLetItem is called when exiting the letItem production.
	ExitLetItem(c *LetItemContext)

	// ExitFilterSt is called when exiting the filterSt production.
	ExitFilterSt(c *FilterStContext)

	// ExitForSt is called when exiting the forSt production.
	ExitForSt(c *ForStContext)

	// ExitReadingStatement is called when exiting the readingStatement production.
	ExitReadingStatement(c *ReadingStatementContext)

	// ExitUpdatingStatement is called when exiting the updatingStatement production.
	ExitUpdatingStatement(c *UpdatingStatementContext)

	// ExitDeleteSt is called when exiting the deleteSt production.
	ExitDeleteSt(c *DeleteStContext)

	// ExitRemoveSt is called when exiting the removeSt production.
	ExitRemoveSt(c *RemoveStContext)

	// ExitRemoveItem is called when exiting the removeItem production.
	ExitRemoveItem(c *RemoveItemContext)

	// ExitForeachSt is called when exiting the foreachSt production.
	ExitForeachSt(c *ForeachStContext)

	// ExitQueryCallSt is called when exiting the queryCallSt production.
	ExitQueryCallSt(c *QueryCallStContext)

	// ExitParenExpressionChain is called when exiting the parenExpressionChain production.
	ExitParenExpressionChain(c *ParenExpressionChainContext)

	// ExitYieldItems is called when exiting the yieldItems production.
	ExitYieldItems(c *YieldItemsContext)

	// ExitYieldItem is called when exiting the yieldItem production.
	ExitYieldItem(c *YieldItemContext)

	// ExitMergeSt is called when exiting the mergeSt production.
	ExitMergeSt(c *MergeStContext)

	// ExitMergeAction is called when exiting the mergeAction production.
	ExitMergeAction(c *MergeActionContext)

	// ExitSetSt is called when exiting the setSt production.
	ExitSetSt(c *SetStContext)

	// ExitSetItem is called when exiting the setItem production.
	ExitSetItem(c *SetItemContext)

	// ExitDynamicPropertyExpression is called when exiting the dynamicPropertyExpression production.
	ExitDynamicPropertyExpression(c *DynamicPropertyExpressionContext)

	// ExitNodeLabels is called when exiting the nodeLabels production.
	ExitNodeLabels(c *NodeLabelsContext)

	// ExitLabelExpression is called when exiting the labelExpression production.
	ExitLabelExpression(c *LabelExpressionContext)

	// ExitLabelConjunction is called when exiting the labelConjunction production.
	ExitLabelConjunction(c *LabelConjunctionContext)

	// ExitLabelNegation is called when exiting the labelNegation production.
	ExitLabelNegation(c *LabelNegationContext)

	// ExitDynamicLabel is called when exiting the dynamicLabel production.
	ExitDynamicLabel(c *DynamicLabelContext)

	// ExitCreateSt is called when exiting the createSt production.
	ExitCreateSt(c *CreateStContext)

	// ExitPatternWhere is called when exiting the patternWhere production.
	ExitPatternWhere(c *PatternWhereContext)

	// ExitWhere is called when exiting the where production.
	ExitWhere(c *WhereContext)

	// ExitPattern is called when exiting the pattern production.
	ExitPattern(c *PatternContext)

	// ExitExpression is called when exiting the expression production.
	ExitExpression(c *ExpressionContext)

	// ExitXorExpression is called when exiting the xorExpression production.
	ExitXorExpression(c *XorExpressionContext)

	// ExitAndExpression is called when exiting the andExpression production.
	ExitAndExpression(c *AndExpressionContext)

	// ExitNotExpression is called when exiting the notExpression production.
	ExitNotExpression(c *NotExpressionContext)

	// ExitComparisonExpression is called when exiting the comparisonExpression production.
	ExitComparisonExpression(c *ComparisonExpressionContext)

	// ExitComparisonSigns is called when exiting the comparisonSigns production.
	ExitComparisonSigns(c *ComparisonSignsContext)

	// ExitAddSubExpression is called when exiting the addSubExpression production.
	ExitAddSubExpression(c *AddSubExpressionContext)

	// ExitMultDivExpression is called when exiting the multDivExpression production.
	ExitMultDivExpression(c *MultDivExpressionContext)

	// ExitPowerExpression is called when exiting the powerExpression production.
	ExitPowerExpression(c *PowerExpressionContext)

	// ExitUnaryAddSubExpression is called when exiting the unaryAddSubExpression production.
	ExitUnaryAddSubExpression(c *UnaryAddSubExpressionContext)

	// ExitAtomicExpression is called when exiting the atomicExpression production.
	ExitAtomicExpression(c *AtomicExpressionContext)

	// ExitNormalizationPredicate is called when exiting the normalizationPredicate production.
	ExitNormalizationPredicate(c *NormalizationPredicateContext)

	// ExitLabelPredicate is called when exiting the labelPredicate production.
	ExitLabelPredicate(c *LabelPredicateContext)

	// ExitListExpression is called when exiting the listExpression production.
	ExitListExpression(c *ListExpressionContext)

	// ExitStringExpression is called when exiting the stringExpression production.
	ExitStringExpression(c *StringExpressionContext)

	// ExitStringExpPrefix is called when exiting the stringExpPrefix production.
	ExitStringExpPrefix(c *StringExpPrefixContext)

	// ExitNullExpression is called when exiting the nullExpression production.
	ExitNullExpression(c *NullExpressionContext)

	// ExitTypePredicate is called when exiting the typePredicate production.
	ExitTypePredicate(c *TypePredicateContext)

	// ExitExpressionType is called when exiting the expressionType production.
	ExitExpressionType(c *ExpressionTypeContext)

	// ExitExpressionTypePart is called when exiting the expressionTypePart production.
	ExitExpressionTypePart(c *ExpressionTypePartContext)

	// ExitPropertyOrLabelExpression is called when exiting the propertyOrLabelExpression production.
	ExitPropertyOrLabelExpression(c *PropertyOrLabelExpressionContext)

	// ExitPropertyExpression is called when exiting the propertyExpression production.
	ExitPropertyExpression(c *PropertyExpressionContext)

	// ExitPatternPart is called when exiting the patternPart production.
	ExitPatternPart(c *PatternPartContext)

	// ExitPathFunction is called when exiting the pathFunction production.
	ExitPathFunction(c *PathFunctionContext)

	// ExitPatternElem is called when exiting the patternElem production.
	ExitPatternElem(c *PatternElemContext)

	// ExitPatternElemStart is called when exiting the patternElemStart production.
	ExitPatternElemStart(c *PatternElemStartContext)

	// ExitPatternElemPart is called when exiting the patternElemPart production.
	ExitPatternElemPart(c *PatternElemPartContext)

	// ExitQuantifiedPath is called when exiting the quantifiedPath production.
	ExitQuantifiedPath(c *QuantifiedPathContext)

	// ExitPatternElemChain is called when exiting the patternElemChain production.
	ExitPatternElemChain(c *PatternElemChainContext)

	// ExitRelationshipQuantifier is called when exiting the relationshipQuantifier production.
	ExitRelationshipQuantifier(c *RelationshipQuantifierContext)

	// ExitProperties is called when exiting the properties production.
	ExitProperties(c *PropertiesContext)

	// ExitNodePattern is called when exiting the nodePattern production.
	ExitNodePattern(c *NodePatternContext)

	// ExitAtom is called when exiting the atom production.
	ExitAtom(c *AtomContext)

	// ExitMapProjection is called when exiting the mapProjection production.
	ExitMapProjection(c *MapProjectionContext)

	// ExitMapProjectionItem is called when exiting the mapProjectionItem production.
	ExitMapProjectionItem(c *MapProjectionItemContext)

	// ExitLhs is called when exiting the lhs production.
	ExitLhs(c *LhsContext)

	// ExitRelationshipPattern is called when exiting the relationshipPattern production.
	ExitRelationshipPattern(c *RelationshipPatternContext)

	// ExitRelationDetail is called when exiting the relationDetail production.
	ExitRelationDetail(c *RelationDetailContext)

	// ExitRelationshipTypes is called when exiting the relationshipTypes production.
	ExitRelationshipTypes(c *RelationshipTypesContext)

	// ExitUnionSt is called when exiting the unionSt production.
	ExitUnionSt(c *UnionStContext)

	// ExitSubqueryExist is called when exiting the subqueryExist production.
	ExitSubqueryExist(c *SubqueryExistContext)

	// ExitInvocationName is called when exiting the invocationName production.
	ExitInvocationName(c *InvocationNameContext)

	// ExitFunctionInvocation is called when exiting the functionInvocation production.
	ExitFunctionInvocation(c *FunctionInvocationContext)

	// ExitParenthesizedExpression is called when exiting the parenthesizedExpression production.
	ExitParenthesizedExpression(c *ParenthesizedExpressionContext)

	// ExitFilterWith is called when exiting the filterWith production.
	ExitFilterWith(c *FilterWithContext)

	// ExitPatternComprehension is called when exiting the patternComprehension production.
	ExitPatternComprehension(c *PatternComprehensionContext)

	// ExitRelationshipsChainPattern is called when exiting the relationshipsChainPattern production.
	ExitRelationshipsChainPattern(c *RelationshipsChainPatternContext)

	// ExitListComprehension is called when exiting the listComprehension production.
	ExitListComprehension(c *ListComprehensionContext)

	// ExitFilterExpression is called when exiting the filterExpression production.
	ExitFilterExpression(c *FilterExpressionContext)

	// ExitCountAll is called when exiting the countAll production.
	ExitCountAll(c *CountAllContext)

	// ExitExpressionChain is called when exiting the expressionChain production.
	ExitExpressionChain(c *ExpressionChainContext)

	// ExitCaseExpression is called when exiting the caseExpression production.
	ExitCaseExpression(c *CaseExpressionContext)

	// ExitReduceExpression is called when exiting the reduceExpression production.
	ExitReduceExpression(c *ReduceExpressionContext)

	// ExitParameter is called when exiting the parameter production.
	ExitParameter(c *ParameterContext)

	// ExitLiteral is called when exiting the literal production.
	ExitLiteral(c *LiteralContext)

	// ExitRangeLit is called when exiting the rangeLit production.
	ExitRangeLit(c *RangeLitContext)

	// ExitBoolLit is called when exiting the boolLit production.
	ExitBoolLit(c *BoolLitContext)

	// ExitIntegerLit is called when exiting the integerLit production.
	ExitIntegerLit(c *IntegerLitContext)

	// ExitNumLit is called when exiting the numLit production.
	ExitNumLit(c *NumLitContext)

	// ExitStringLit is called when exiting the stringLit production.
	ExitStringLit(c *StringLitContext)

	// ExitCharLit is called when exiting the charLit production.
	ExitCharLit(c *CharLitContext)

	// ExitListLit is called when exiting the listLit production.
	ExitListLit(c *ListLitContext)

	// ExitMapLit is called when exiting the mapLit production.
	ExitMapLit(c *MapLitContext)

	// ExitMapPair is called when exiting the mapPair production.
	ExitMapPair(c *MapPairContext)

	// ExitName is called when exiting the name production.
	ExitName(c *NameContext)

	// ExitSymbol is called when exiting the symbol production.
	ExitSymbol(c *SymbolContext)

	// ExitReservedWord is called when exiting the reservedWord production.
	ExitReservedWord(c *ReservedWordContext)
}
