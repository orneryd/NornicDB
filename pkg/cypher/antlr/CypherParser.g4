/*
 [The "BSD licence"]
 Copyright (c) 2022 Boris Zhguchev
 All rights reserved.

 Redistribution and use in source and binary forms with or without
 modification are permitted provided that the following conditions
 are met:
 1. Redistributions of source code must retain the above copyright
    notice this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright
    notice this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
 3. The name of the author may not be used to endorse or promote products
    derived from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 IMPLIED WARRANTIES INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
 OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT INDIRECT
 INCIDENTAL SPECIAL EXEMPLARY OR CONSEQUENTIAL DAMAGES (INCLUDING BUT
 NOT LIMITED TO PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE
 DATA OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 THEORY OF LIABILITY WHETHER IN CONTRACT STRICT LIABILITY OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// $antlr-format alignTrailingComments true, columnLimit 150, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine false, allowShortBlocksOnASingleLine true, alignSemicolons hanging, alignColons hanging

parser grammar CypherParser;

options {
    tokenVocab = CypherLexer;
}

script
    : shellCommand EOF
    | transactionStatement EOF
    | query (SEMI query)* SEMI? EOF
    ;

shellCommand
    : COLON name shellCommandElement*
    ;

shellCommandElement
    : mapLit
    | expression
    | ASSIGN
    | GT
    | COLON
    ;

transactionStatement
    : BEGIN TRANSACTION?
    | COMMIT TRANSACTION?
    | ROLLBACK TRANSACTION?
    ;

// statements
query
    : queryPrefix* useClause? (regularQuery | standaloneCall | schemaCommand | administrationCommand | showCommand)
    ;

useClause
    : USE (symbol (DOT symbol)* | functionInvocation)
    ;

// EXPLAIN/PROFILE prefix
queryPrefix
    : EXPLAIN
    | PROFILE
    ;

// SHOW commands
showCommand
        : SHOW ((ALL | FULLTEXT | RANGE_INDEX | TEXT | POINT | VECTOR | LOOKUP)? (INDEXES | INDEX)
            | CONSTRAINTS | CONSTRAINT | PROCEDURES | FUNCTIONS | COMPOSITE? (DATABASE | DATABASES)
            | ALIASES (FOR (DATABASE qualifiedName | DATABASES))? | ALL)
            (YIELD (MULT | yieldItems) returnSt?)?
    ;

administrationCommand
    : CREATE COMPOSITE? DATABASE name (IF NOT EXISTS)?
    | DROP COMPOSITE? DATABASE name (IF EXISTS)?
    | CREATE ALIAS qualifiedName (IF NOT EXISTS)? FOR DATABASE qualifiedName
    | DROP ALIAS qualifiedName (IF EXISTS)? (FOR DATABASE)?
    | ALTER COMPOSITE DATABASE name (ADD ALIAS qualifiedName FOR DATABASE qualifiedName | DROP ALIAS qualifiedName)
    ;

qualifiedName
    : name (DOT name)*
    ;

// Schema commands (DROP INDEX, CREATE INDEX, etc.)
schemaCommand
    : DROP INDEX name? (IF EXISTS)?
    | CREATE (RANGE_INDEX | TEXT | POINT)? INDEX name? (IF NOT EXISTS)? (FOR (nodePattern | relationshipsChainPattern))? ON? parenExpressionChain? (OPTIONS mapLit)?
    | CREATE FULLTEXT INDEX name? (IF NOT EXISTS)? (FOR (nodePattern | relationshipsChainPattern))? ON? EACH? LBRACK expressionChain RBRACK (OPTIONS mapLit)?
    | CREATE VECTOR INDEX name? (IF NOT EXISTS)? (FOR (nodePattern | relationshipsChainPattern))? ON? parenExpressionChain? (OPTIONS mapLit)?
    | CREATE LOOKUP INDEX name? (IF NOT EXISTS)? FOR (nodePattern | relationshipsChainPattern) ON EACH functionInvocation
    | DROP CONSTRAINT name? (IF EXISTS)?
    | CREATE CONSTRAINT name? (IF NOT EXISTS)? (FOR (nodePattern | relationshipsChainPattern))? REQUIRE (expression | parenExpressionChain) (IS UNIQUE | IS NOT NULL_W | IS NODE KEY | IS COLON COLON propertyTypeName | IS TYPED propertyTypeName) (OPTIONS mapLit)?
    | CREATE CONSTRAINT name? (IF NOT EXISTS)? ON? nodePattern? ASSERT (expression | parenExpressionChain) IS (UNIQUE | NOT NULL_W | NODE KEY | COLON COLON propertyTypeName | TYPED propertyTypeName) (OPTIONS mapLit)?
    ;

propertyTypeName
    : name (name)?
    ;

regularQuery
    : singleQuery unionSt*
    ;

singleQuery
    : singlePartQ
    | multiPartQ
    ;

standaloneCall
    : CALL invocationName parenExpressionChain? (YIELD (MULT | yieldItems) orderSt? skipSt? limitSt?)?
    ;

// Subqueries
existsSubquery
    : EXISTS LBRACE (matchSt | subqueryBody | pattern where?) RBRACE
    ;

countSubquery
    : COUNT LBRACE (matchSt | subqueryBody | pattern where?) RBRACE
    ;

collectSubquery
    : COLLECT LBRACE regularQuery RBRACE
    ;

callSubquery
    : CALL (LPAREN (MULT | symbol (COMMA symbol)*)? RPAREN)? LBRACE subqueryBody RBRACE (IN TRANSACTIONS (OF (numLit | parameter) (ROW | ROWS))?)?
    ;

// Subquery body can start with WITH (to import variables) or have statements
subqueryBody
    : useClause? ((readingStatement | updatingStatement)* withSt)* (readingStatement | updatingStatement)* (returnSt | FINISH)? (UNION ALL? subqueryBody)?
    ;

returnSt
    : RETURN projectionBody
    ;

withSt
    : WITH projectionBody where?
    ;

embeddingSt
    : WITH EMBEDDING
    ;

skipSt
    : SKIP_W expression
    ;

limitSt
    : LIMIT expression
    ;

projectionBody
    : DISTINCT? projectionItems orderSt? skipSt? limitSt?
    ;

projectionItems
    : (MULT | projectionItem) (COMMA projectionItem)*
    ;

projectionItem
    : expression (AS symbol)?
    ;

orderItem
    : expression (ASCENDING | ASC | DESCENDING | DESC)?
    ;

orderSt
    : ORDER BY orderItem (COMMA orderItem)*
    ;

singlePartQ
    : readingStatement* (returnSt | updatingStatement+ embeddingSt? returnSt?)?
    | callSubquery orderSt?
    ;

multiPartQ
    : ((readingStatement | updatingStatement)* withSt)+ singlePartQ
    ;

matchSt
    : OPTIONAL? MATCH patternWhere
    ;

unwindSt
    : UNWIND expression AS symbol where?
    ;

readingStatement
    : matchSt
    | unwindSt
    | queryCallSt
    | callSubquery
    ;

updatingStatement
    : createSt
    | mergeSt
    | deleteSt
    | setSt
    | removeSt
    | foreachSt
    ;

deleteSt
    : DETACH? DELETE expressionChain
    ;

removeSt
    : REMOVE removeItem (COMMA removeItem)*
    ;

removeItem
    : symbol nodeLabels
    | propertyExpression
    ;

foreachSt
    : FOREACH LPAREN symbol IN expression STICK updatingStatement+ RPAREN
    ;

queryCallSt
    : CALL invocationName parenExpressionChain? (YIELD (MULT | yieldItems) orderSt? skipSt? limitSt?)?
    ;

parenExpressionChain
    : LPAREN expressionChain? RPAREN
    ;

yieldItems
    : yieldItem (COMMA yieldItem)* where?
    ;

yieldItem
    : (symbol AS)? symbol
    ;

mergeSt
    : MERGE patternPart mergeAction*
    ;

mergeAction
    : ON (MATCH | CREATE) setSt
    ;

setSt
    : SET setItem (COMMA setItem)*
    ;

setItem
    : propertyExpression ASSIGN expression
    | symbol (ASSIGN | ADD_ASSIGN) expression
    | symbol nodeLabels
    ;

nodeLabels
    : (COLON (name (STICK name)* | DOLLAR LPAREN expression RPAREN))+
    ;

createSt
    : CREATE pattern
    ;

patternWhere
    : pattern where?
    ;

where
    : WHERE expression
    ;

pattern
    : patternPart (COMMA patternPart)*
    ;

expression
    : xorExpression (OR xorExpression)*
    ;

xorExpression
    : andExpression (XOR andExpression)*
    ;

andExpression
    : notExpression (AND notExpression)*
    ;

notExpression
    : NOT* comparisonExpression
    | NOT* existsSubquery
    ;

comparisonExpression
    : addSubExpression (comparisonSigns addSubExpression)*
    ;

comparisonSigns
    : ASSIGN
    | LE
    | GE
    | GT
    | LT
    | NOT_EQUAL
    | REGEX_MATCH
    ;

addSubExpression
    : multDivExpression ((PLUS | SUB) multDivExpression)*
    ;

multDivExpression
    : powerExpression ((MULT | DIV | MOD) powerExpression)*
    ;

powerExpression
    : unaryAddSubExpression (CARET unaryAddSubExpression)*
    ;

unaryAddSubExpression
    : (PLUS | SUB)? atomicExpression
    ;

atomicExpression
    : propertyOrLabelExpression (stringExpression | listExpression | nullExpression | typePredicate)*
    ;

listExpression
    : IN propertyOrLabelExpression
    | LBRACK (expression? RANGE expression? | expression) RBRACK (DOT name)*
    ;

stringExpression
    : stringExpPrefix propertyOrLabelExpression
    ;

stringExpPrefix
    : STARTS WITH
    | ENDS WITH
    | CONTAINS
    ;

nullExpression
    : IS NOT? NULL_W
    ;

typePredicate
    : IS NOT? (COLON COLON | TYPED) expressionType
    | COLON COLON expressionType
    ;

expressionType
    : expressionTypePart (STICK expressionTypePart)*
    ;

expressionTypePart
    : (ID | ANY | NODE | POINT | NULL_W) (ID | WITH)* (LT expressionType GT)? (NOT NULL_W)?
    ;

propertyOrLabelExpression
    : propertyExpression nodeLabels?
    ;

propertyExpression
    : atom (DOT name)*
    ;

patternPart
    : (symbol ASSIGN)? patternElem
    | (symbol ASSIGN)? pathFunction
    ;

pathFunction
    : (SHORTESTPATH | ALLSHORTESTPATHS) LPAREN patternElem RPAREN
    ;

patternElem
    : nodePattern patternElemChain*
    | LPAREN patternElem RPAREN
    ;

patternElemChain
    : relationshipPattern nodePattern
    ;

properties
    : mapLit
    | parameter
    ;

nodePattern
    : LPAREN symbol? nodeLabels? properties? RPAREN
    ;

atom
    : literal
    | parameter
    | mapProjection
    | caseExpression
    | reduceExpression
    | countAll
    | countSubquery
    | collectSubquery
    | listComprehension
    | patternComprehension
    | filterWith
    | relationshipsChainPattern
    | parenthesizedExpression
    | functionInvocation
    | symbol
    | subqueryExist
    ;

mapProjection
    : symbol LBRACE (mapProjectionItem (COMMA mapProjectionItem)*)? RBRACE
    ;

mapProjectionItem
    : DOT (name | MULT)
    | name COLON expression
    | symbol
    ;

lhs
    : symbol ASSIGN
    ;

relationshipPattern
    : LT SUB relationDetail? SUB GT?
    | SUB relationDetail? SUB GT?
    ;

relationDetail
    : LBRACK symbol? relationshipTypes? rangeLit? properties? RBRACK
    ;

relationshipTypes
    : COLON name (STICK COLON? name)*
    ;

unionSt
    : UNION ALL? singleQuery
    ;

subqueryExist
    : EXISTS LBRACE (regularQuery | patternWhere) RBRACE
    ;

invocationName
    : symbol (DOT symbol)*
    ;

functionInvocation
    : invocationName LPAREN DISTINCT? expressionChain? RPAREN
    | TRIM LPAREN (LEADING | TRAILING | BOTH)? expression? FROM expression RPAREN
    ;

parenthesizedExpression
    : LPAREN expression RPAREN
    ;

filterWith
    : (ALL | ANY | NONE | SINGLE) LPAREN filterExpression RPAREN
    ;

patternComprehension
    : LBRACK lhs? relationshipsChainPattern where? STICK expression RBRACK
    ;

relationshipsChainPattern
    : nodePattern patternElemChain+
    ;

listComprehension
    : LBRACK filterExpression (STICK expression)? RBRACK
    ;

filterExpression
    : symbol IN expression where?
    ;

countAll
    : COUNT LPAREN MULT RPAREN
    ;

expressionChain
    : expression (COMMA expression)*
    ;

caseExpression
    : CASE expression? (WHEN expression THEN expression)+ (ELSE expression)? END
    ;

reduceExpression
    : REDUCE LPAREN symbol ASSIGN expression COMMA symbol IN expression STICK expression RPAREN
    ;

parameter
    : DOLLAR (name | numLit)
    ;

// literals
literal
    : boolLit
    | numLit
    | NULL_W
    | stringLit
    | charLit
    | listLit
    | mapLit
    ;

rangeLit
    : MULT integerLit? (RANGE integerLit?)?
    ;

boolLit
    : TRUE
    | FALSE
    ;

integerLit
    : (PLUS | SUB)? (INTEGER | DIGIT)
    ;

numLit
    : (PLUS | SUB)? FLOAT
    | integerLit
    ;

stringLit
    : STRING_LITERAL
    ;

charLit
    : CHAR_LITERAL
    ;

listLit
    : LBRACK expressionChain? RBRACK
    ;

mapLit
    : LBRACE (mapPair (COMMA mapPair)*)? RBRACE
    ;

mapPair
    : name COLON expression
    ;

// primitive ids
name
    : symbol
    | reservedWord
    ;

symbol
    : ESC_LITERAL
    | ID
    | COUNT
    | SUM
    | AVG
    | MIN
    | MAX
    | COLLECT
    | FILTER
    | EXTRACT
    | REDUCE
    | FOREACH
    | ANY
    | NONE
    | SINGLE
    | INDEX
    | INDEXES
    | CONSTRAINT
    | CONSTRAINTS
    | BEGIN
    | COMMIT
    | ROLLBACK
    | TRANSACTION
    | DROP
    | CREATE
    | VECTOR
    | LOOKUP
    | USE
    | ALIAS
    | ALIASES
    | COMPOSITE
    | ALTER
    | ADD
    | RANGE_INDEX
    | TEXT
    | POINT
    | ROW
    | TRIM
    | FROM
    | LEADING
    | TRAILING
    | BOTH
    | DELETE
    | ADD
    | REMOVE
    | SET
    | MATCH
    | MERGE
    | FULLTEXT
    | PROCEDURES
    | FUNCTIONS
    | DATABASE
    | DATABASES
    | CALL
    | EXPLAIN
    | PROFILE
    | EXISTS
    | SHOW
    | OPTIONS
    | NODE
    | KEY
    | ASSERT
    | ROWS
    | TRANSACTIONS
    | END
    | CASE
    | WHEN
    | THEN
    | ELSE
    | TRUE
    | FALSE
    | NULL_W
    | UNIQUE
    | REQUIRE
    | TYPED
    | EMBEDDING
    | IF
    | EACH
    | ALL
    | SHORTESTPATH
    | ALLSHORTESTPATHS
    | reservedWord
    ;

reservedWord
    : ALL
    | ASC
    | ASCENDING
    | BY
    | CREATE
    | DELETE
    | DESC
    | DESCENDING
    | DETACH
    | EXISTS
    | LIMIT
    | MATCH
    | MERGE
    | ON
    | OPTIONAL
    | ORDER
    | REMOVE
    | RETURN
    | SET
    | SKIP_W
    | WHERE
    | WITH
    | UNION
    | FINISH
    | UNWIND
    | AND
    | AS
    | CONTAINS
    | DISTINCT
    | ENDS
    | IN
    | IS
    | NOT
    | OR
    | STARTS
    | XOR
    | FALSE
    | TRUE
    | NULL_W
    | CONSTRAINT
    | DO
    | FOR
    | REQUIRE
    | TYPED
    | EMBEDDING
    | UNIQUE
    | CASE
    | WHEN
    | THEN
    | ELSE
    | END
    | MANDATORY
    | SCALAR
    | OF
    | ADD
    | DROP
    | FOREACH
    | REDUCE
    ;