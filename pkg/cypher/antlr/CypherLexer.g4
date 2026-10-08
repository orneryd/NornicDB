/*
 [The "BSD licence"]
 Copyright (c) 2022 Boris Zhguchev
 All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:
 1. Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
 3. The name of the author may not be used to endorse or promote products
    derived from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// $antlr-format alignTrailingComments true, columnLimit 150, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine true, allowShortBlocksOnASingleLine true, minEmptyLines 0, alignSemicolons ownLine
// $antlr-format alignColons trailing, singleLineOverrulesHangingColon true, alignLexerCommands true, alignLabels true, alignTrailers true

lexer grammar CypherLexer;
channels {
    COMMENTS
}
options {
    caseInsensitive = true;
}

ASSIGN     : '=';
ADD_ASSIGN : '+=';
LE         : '<=';
GE         : '>=';
GT         : '>';
LT         : '<';
NOT_EQUAL  : '<>' | '!=';
REGEX_MATCH: '=~';
RANGE      : '..';
SEMI       : ';';
DOT        : '.';
COMMA      : ',';
LPAREN     : '(';
RPAREN     : ')';
LBRACE     : '{';
RBRACE     : '}';
LBRACK     : '[';
RBRACK     : ']';
SUB        : '-';
PLUS       : '+';
DIV        : '/';
MOD        : '%';
CARET      : '^';
MULT       : '*';
ESC        : '`';
COLON      : ':';
STICK      : '|';
AMPERSAND  : '&';
BANG       : '!';
DOLLAR     : '$';

CALL       : 'CALL';
BEGIN      : 'BEGIN';
COMMIT     : 'COMMIT';
ROLLBACK   : 'ROLLBACK';
TRANSACTION: 'TRANSACTION';
YIELD      : 'YIELD';
FILTER     : 'FILTER';
EXTRACT    : 'EXTRACT';
REDUCE     : 'REDUCE';
FOREACH    : 'FOREACH';
COUNT      : 'COUNT';
SUM        : 'SUM';
AVG        : 'AVG';
MIN        : 'MIN';
MAX        : 'MAX';
COLLECT    : 'COLLECT';
ANY        : 'ANY';
NONE       : 'NONE';
SINGLE     : 'SINGLE';
ALL        : 'ALL';
ASC        : 'ASC';
ASCENDING  : 'ASCENDING';
BY         : 'BY';
CREATE     : 'CREATE';
DELETE     : 'DELETE';
DESC       : 'DESC';
DESCENDING : 'DESCENDING';
DETACH     : 'DETACH';
EXISTS     : 'EXISTS';
LIMIT      : 'LIMIT';
MATCH      : 'MATCH';
MERGE      : 'MERGE';
ON         : 'ON';
OPTIONAL   : 'OPTIONAL';
ORDER      : 'ORDER';
REMOVE     : 'REMOVE';
RETURN     : 'RETURN';
FINISH     : 'FINISH';
SET        : 'SET';
SKIP_W     : 'SKIP';
WHERE      : 'WHERE';
WITH       : 'WITH';
UNION      : 'UNION';
UNWIND     : 'UNWIND';
AND        : 'AND';
AS         : 'AS';
CONTAINS   : 'CONTAINS';
DISTINCT   : 'DISTINCT';
ENDS       : 'ENDS';
IN         : 'IN';
IS         : 'IS';
NOT        : 'NOT';
OR         : 'OR';
STARTS     : 'STARTS';
XOR        : 'XOR';
FALSE      : 'FALSE';
TRUE       : 'TRUE';
NULL_W     : 'NULL';
CONSTRAINT : 'CONSTRAINT';
DO         : 'DO';
FOR        : 'FOR';
REQUIRE    : 'REQUIRE';
UNIQUE     : 'UNIQUE';
CASE       : 'CASE';
WHEN       : 'WHEN';
THEN       : 'THEN';
ELSE       : 'ELSE';
END        : 'END';
MANDATORY  : 'MANDATORY';
SCALAR     : 'SCALAR';
OF         : 'OF';
ADD        : 'ADD';
DROP       : 'DROP';
INDEX      : 'INDEX';
INDEXES    : 'INDEXES';
VECTOR     : 'VECTOR';
EXPLAIN    : 'EXPLAIN';
PROFILE    : 'PROFILE';
SHOW       : 'SHOW';
USERS      : 'USERS';
USER       : 'USER';
CURRENT    : 'CURRENT';
REPLACE    : 'REPLACE';
CONSTRAINTS: 'CONSTRAINTS';
CONTRACTS  : 'CONTRACTS';
PROCEDURES : 'PROCEDURES';
FUNCTIONS  : 'FUNCTIONS';
DATABASE   : 'DATABASE';
DATABASES  : 'DATABASES';
FULLTEXT   : 'FULLTEXT';
LOOKUP     : 'LOOKUP';
USE        : 'USE';
ALIAS      : 'ALIAS';
ALIASES    : 'ALIASES';
COMPOSITE  : 'COMPOSITE';
ALTER      : 'ALTER';
RANGE_INDEX: 'RANGE';
TEXT       : 'TEXT';
POINT      : 'POINT';
OPTIONS    : 'OPTIONS';
EACH       : 'EACH';
EMBEDDING  : 'EMBEDDING';
IF         : 'IF';
TRANSACTIONS: 'TRANSACTIONS';
ROWS       : 'ROWS';
ROW        : 'ROW';
TRIM       : 'TRIM';
FROM       : 'FROM';
LEADING    : 'LEADING';
TRAILING   : 'TRAILING';
BOTH       : 'BOTH';
ASSERT     : 'ASSERT';
TYPED      : 'TYPED';
DEFAULT    : 'DEFAULT';
HOME       : 'HOME';
SETTING    : 'SETTING';
SETTINGS   : 'SETTINGS';
ROLES      : 'ROLES';
PRIVILEGES : 'PRIVILEGES';
SERVERS    : 'SERVERS';
TERMINATE  : 'TERMINATE';
NORMALIZED : 'NORMALIZED';
NFC        : 'NFC';
NFD        : 'NFD';
NFKC       : 'NFKC';
NFKD       : 'NFKD';
KEY        : 'KEY';
NODE       : 'NODE';
RELATIONSHIP: 'RELATIONSHIP';
TEMPORAL   : 'TEMPORAL';
NO         : 'NO';
OVERLAP    : 'OVERLAP';
ALLOWED    : 'ALLOWED';
DISALLOWED : 'DISALLOWED';
SHORTESTPATH: 'shortestPath';
ALLSHORTESTPATHS: 'allShortestPaths';

// FLOAT must come before INTEGER so 2.00 isn't matched as INTEGER
// Note: We use [0-9]+ for fractional part to allow numbers like 0.05
FLOAT : (([0-9]+ '.' [0-9]+ | '.' [0-9]+) ExponentPart? [fd]? | [0-9]+ (ExponentPart [fd]? | [fd]));

// Integer literal - must be before ID to have priority
INTEGER : DecimalInteger;

// DIGIT - hex, octal, or single digit (for array indices, etc)
DIGIT : HexInteger | OctalInteger | [0-9];

// ID must come after numbers so they aren't matched as IDs
ID: Letter LetterOrDigit*;

ESC_LITERAL    : '`' ('``' | ~'`')* '`';
// Single-quoted strings can contain any characters except unescaped quotes
CHAR_LITERAL   : '\'' (~['\\] | EscapeSequence)* '\'';
// Double-quoted strings can contain any characters except unescaped quotes
STRING_LITERAL : '"' (~["\\] | EscapeSequence)* '"';

WS           : [ \t\r\n\u000C]+ -> channel(HIDDEN);
COMMENT      : '/*' .*? '*/'    -> channel(COMMENTS);
LINE_COMMENT : '//' ~[\r\n]*    -> channel(COMMENTS);
// ERRCHAR catches unrecognized characters - keeps them visible to trigger errors
ERRCHAR      : .;

fragment EscapeSequence:
    '\\' [btnfr"'\\.u/]
    | '\\' ([0-3]? [0-7])? [0-7]
    | '\\' 'u'+ HexDigit HexDigit HexDigit HexDigit
;

fragment ExponentPart: [e] [+-]? [0-9]+;

fragment HexInteger  : '0' [xX] HexDigit+;
fragment HexDigit    : [0-9a-fA-F];
fragment OctalInteger: '0' [oO]? [0-7]+;
fragment DecimalInteger: '0' | [1-9] [0-9]*;

fragment LetterOrDigit: Letter | [0-9];

Letter:
    [a-z_]
    | ~[\u0000-\u007F\uD800-\uDBFF] // covers all characters above 0x7F which are not a surrogate
    | [\uD800-\uDBFF] [\uDC00-\uDFFF]
; // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF