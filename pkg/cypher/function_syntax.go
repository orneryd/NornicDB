package cypher

import "strings"

// parseFunctionCallWS parses a standalone function call expression, tolerating
// whitespace between the function name and the opening parenthesis.
//
// Examples:
//   - "toLower(n.name)"   -> ("toLower", "n.name", true)
//   - "toLower ( n.name)" -> ("toLower", " n.name", true)
//
// Returns ok=false if the expression is not a standalone function call.
func parseFunctionCallWS(expr string) (name string, inner string, ok bool) {
	call, ok := functionCallAt(expr, queryGapEnd(expr, 0))
	if !ok {
		return "", "", false
	}
	close := call.close(expr)
	if close < 0 || queryGapEnd(expr, close+1) != len(expr) {
		return "", "", false
	}
	return call.name, expr[call.open+1 : close], true
}

// functionCallSpan is the shared lexical identity of a call. Delimiter matching
// is lazy: name-only validation does not rescan every nested argument list.
type functionCallSpan struct {
	name        string
	start, open int
	syntax *functionSyntax
	group int
 next int
 procedure bool
}

func (call functionCallSpan) close(text string) int {
	if call.syntax == nil || call.group<0 { return -1 }; return call.syntax.groups[call.group].close
}

// functionCallAt reads a name at a known code position, including quoted and
// qualified components and trivia before "(". Whole-text consumers must use
// nextFunctionCall so comments and string literals cannot become code.
func readFunctionIdentity(text string, at int) (functionCallSpan, bool) {
	if at < 0 || at >= len(text) || !functionNameStartsAt(text, at) ||
		at > 0 && (text[at-1] == '$' || text[at-1] == ':' && !colonIsMapKeySeparator(text,at-1)) {
		return functionCallSpan{}, false
	}
	name, next, ok := scanIdentifierToken(text, at)
	if !ok {
		return functionCallSpan{}, false
	}
	parts, end := dottedNameParts(text, next, true)
	open := queryGapEnd(text, end)
	if open >= len(text) || text[open] != '(' {
		return functionCallSpan{}, false
	}
	return functionCallSpan{name: name + parts, start: at, open: open, next:open+1}, true
}

// nextFunctionCall is the lexical owner for static and runtime call readers.
// It advances into arguments so nested calls are visited, and excludes syntax,
// procedure/graph names, type annotations, comments and string literals.
func nextFunctionCall(text string, index int) (functionCallSpan, bool) {
	return nextFunctionCallIn(text, index, true)
}

func nextFunctionCallIn(text string, index int, descendSubqueries bool) (functionCallSpan, bool) {
	return nextFunctionCallMode(text, index, descendSubqueries, false)
}

func nextFunctionCallMode(text string, index int, descendSubqueries, syntaxNames bool) (functionCallSpan, bool) {
 return nextCallSpanMode(text,index,descendSubqueries,false,syntaxNames)
}

func nextCallSpan(text string,index int,descendSubqueries,includeProcedures bool) (functionCallSpan,bool) {
 return nextCallSpanMode(text,index,descendSubqueries,includeProcedures,false)
}

func nextCallSpanMode(text string, index int, descendSubqueries, includeProcedures, syntaxNames bool) (functionCallSpan, bool) {
 syntax:=readFunctionSyntax(text)
 for _,call:=range syntax.calls {
  if call.start<index || call.procedure && !includeProcedures {continue}
  if !call.procedure && (precededByTypeAnnotation(text,call.start) || !syntaxNames && isNonFunctionSemanticKeyword(lowerASCII(call.name))) {continue}
  if !descendSubqueries {
   nested:=false
   for _,span:=range syntax.subqueries {if call.start>=span.start && call.start<span.end {nested=true;break}}
   if nested {continue}
  }
  return call,true
 }
 return functionCallSpan{},false
}
// functionCallBeforeParen reads a call at an opening already classified as
// code. Ordinary names use a backward token boundary plus functionCallAt;
// quoted or qualified names fall back to the same whole-text lexical reader.
func functionCallBeforeParen(text string, open int) (functionCallSpan, bool) {
 for _,call:=range readFunctionSyntax(text).calls {if call.open==open && !call.procedure {return call,true}}
 return functionCallSpan{},false
}

func functionCallAt(text string, at int) (functionCallSpan, bool) {
 for _,call:=range readFunctionSyntax(text).calls {if call.start==at && !call.procedure {return call,true}}
 return functionCallSpan{},false
}
// functionNameStartsAt prevents a builtin scanner from matching only the last
// component of a qualified function name (custom.range is not range), and is
// the one test for a word that follows a dot: a property key (x . and) or a
// name component is never a keyword or a function of its own. Whitespace and
// comments may stand around the dot.
func functionNameStartsAt(text string, at int) bool {
	if at == 0 {
		return true
	}
	if isIdentByte(text[at-1]) {
		return false
	}
	previous := queryGapStart(text, at) - 1
	return previous < 0 || text[previous] != '.'
}

// canonicalCallableName shares the qualified identifier reader used by semantic
// validation, preserving component case while removing trivia around separators.
func canonicalCallableName(text string) (string, bool) {
	text = strings.TrimSpace(text)
	// Ordinary unquoted names need no normalization. Validate their lexical
	// components in one pass before using the trivia/quoted-token reader.
	if len(text) > 0 && isIdentStartByte(text[0]) {
		compact := true
		for index := 1; index < len(text); index++ {
			if isIdentByte(text[index]) {
				continue
			}
			if text[index] == '.' && index+1 < len(text) && isIdentStartByte(text[index+1]) {
				index++
				continue
			}
			compact = false
			break
		}
		if compact {
			return text, true
		}
	}
	name, end, ok := scanIdentifierToken(text, 0)
	if !ok {
		return "", false
	}
	if end == len(text) {
		return name, true
	}
	parts, end := dottedNameParts(text, end, true)
	if end != len(text) && queryGapEnd(text, end) != len(text) {
		return "", false
	}
	if len(name)+len(parts) == len(text) {
		return text, true
	}
	return name + parts, true
}

// syntaxSpan uses byte offsets into immutable source text.
type syntaxSpan struct { start, end int }
type syntaxGroup struct {
 open, close int
 separators []int
 punctuation []int
}
type functionSyntax struct {
 text string
 calls []functionCallSpan
 groups []syntaxGroup
 comments []syntaxSpan
 subqueries []syntaxSpan
 leading int
}

var functionSyntaxCache = newBoundedCache[string, *functionSyntax](512, boundedCacheStringShardIndex)

func readFunctionSyntax(text string) *functionSyntax {
 if len(text) <= 16384 {
  if syntax, ok := functionSyntaxCache.get(text); ok { return syntax }
 }
 syntax := &functionSyntax{text:text, leading:queryGapEnd(text, 0)}
 // Group zero represents the whole input, including argument-only inputs.
 syntax.groups = append(syntax.groups, syntaxGroup{open:-1, close:len(text)})
 stack := []int{0}
 pending := make(map[int]functionCallSpan)
 subqueryOpen := make(map[int]int)
 for index:=0; index<len(text); {
  if end:=queryCommentEnd(text,index); end>=0 {
   syntax.comments=append(syntax.comments,syntaxSpan{index,end}); index=end; continue
  }
  if text[index]=='\'' || text[index]=='"' {
   index=skipCypherQuotedText(text,index,text[index]); continue
  }
  if name,next,ok:=scanIdentifierToken(text,index); ok {
   if text[index]!='`' {
    if _,open,ok:=subqueryExpressionKeywordAt(text,index); ok {subqueryOpen[open]=index}
    if strings.EqualFold(name,"CALL") || strings.EqualFold(name,"USE") {
     start:=queryGapEnd(text,next)
     called,end,named:=scanIdentifierToken(text,start)
     if !named {index=next;continue}
     parts,end:=dottedNameParts(text,end,true)
     if strings.EqualFold(name,"CALL") {
      cursor:=queryGapEnd(text,end)
      call:=functionCallSpan{name:called+parts,start:start,open:-1,next:cursor,procedure:true,syntax:syntax,group:-1}
      if cursor<len(text) && text[cursor]=='(' {call.open=cursor;call.next=cursor+1;pending[cursor]=call} else {syntax.calls=append(syntax.calls,call)}
     }
     index=end;continue
    }
   }
   if call,ok:=readFunctionIdentity(text,index); ok {
    pending[call.open]=call
    // A qualified name is one token; its components are never separate calls.
    index=next; continue
   }
   index=next; continue
  }
  switch text[index] {
  case '(', '[', '{':
   group:=len(syntax.groups)
   syntax.groups=append(syntax.groups,syntaxGroup{open:index,close:-1})
   stack=append(stack,group)
   if call,ok:=pending[index]; ok {
    call.syntax=syntax; call.group=group
    syntax.calls=append(syntax.calls,call)
   }
  case ')', ']', '}':
   if len(stack)>1 {
    group:=stack[len(stack)-1]
    opening:=text[syntax.groups[group].open]
    if opening=='(' && text[index]==')' || opening=='[' && text[index]==']' || opening=='{' && text[index]=='}' {
     syntax.groups[group].close=index; stack=stack[:len(stack)-1]
     if start,ok:=subqueryOpen[syntax.groups[group].open]; ok {syntax.subqueries=append(syntax.subqueries,syntaxSpan{start,index+1})}
    }
   }
  case ',':
   group:=stack[len(stack)-1]
   syntax.groups[group].separators=append(syntax.groups[group].separators,index)
   syntax.groups[group].punctuation=append(syntax.groups[group].punctuation,index)
  default:
   group:=stack[len(stack)-1]
   syntax.groups[group].punctuation=append(syntax.groups[group].punctuation,index)
  }
  index++
 }
 if len(text)<=16384 {functionSyntaxCache.put(text,syntax)}
 return syntax
}

// render removes comments recorded by the lexer, never quoted data. A space
// prevents tokens on either side of a removed comment from being concatenated.
func (syntax *functionSyntax) render(span syntaxSpan) string {
 var out strings.Builder
 start:=span.start
 for _,comment:=range syntax.comments {
  if comment.end<=start {continue}; if comment.start>=span.end {break}
  if out.Cap()==0 {out.Grow(span.end-span.start)}
  out.WriteString(syntax.text[start:comment.start]); out.WriteByte(' ')
  start=comment.end
 }
 if out.Cap()==0 {return strings.TrimSpace(syntax.text[span.start:span.end])}
 out.WriteString(syntax.text[start:span.end])
 return strings.TrimSpace(out.String())
}

func (syntax *functionSyntax) argumentSpans(group int) []syntaxSpan {
 shape:=syntax.groups[group]
 if shape.close<0 {return nil}
 start:=shape.open+1
 spans:=make([]syntaxSpan,0,len(shape.separators)+1)
 for _,comma:=range shape.separators {spans=append(spans,syntaxSpan{start,comma}); start=comma+1}
 return append(spans,syntaxSpan{start,shape.close})
}

// appendArguments preserves strict empty entries when requested. Legacy
// callers historically drop only the last empty entry, not interior ones.
func (syntax *functionSyntax) appendArguments(parts []string, group int, keepEmpty bool) []string {
 spans:=syntax.argumentSpans(group)
 if len(spans)==1 && syntax.render(spans[0])=="" {return parts}
 for index,span:=range spans {
  value:=syntax.render(span)
  if index==len(spans)-1 && value=="" && !keepEmpty {continue}
  parts=append(parts,value)
 }
 return parts
}

func (call functionCallSpan) arguments(parts []string) []string {
 return call.syntax.appendArguments(parts,call.group,false)
}

// splitDelimiter is the compatibility policy for generic AST separators. The
// lexer has already excluded quoted/comment text and nested groups.
func (syntax *functionSyntax) splitDelimiter(delimiter rune) []string {
 if delimiter==',' {return syntax.appendArguments(nil,0,false)}
 var parts []string
 start:=0
 for _,at:=range syntax.groups[0].punctuation {
  if rune(syntax.text[at])==delimiter {parts=append(parts,syntax.render(syntaxSpan{start,at})); start=at+1}
 }
 if value:=syntax.render(syntaxSpan{start,len(syntax.text)}); value!="" {parts=append(parts,value)}
 return parts
}

func parseFunctionCallArguments(text string) (string, []string, bool) {
 call,ok:=functionCallAt(text,readFunctionSyntax(text).leading)
 if !ok || call.close(text)<0 || queryGapEnd(text,call.close(text)+1)!=len(text) {return "",nil,false}
 return call.name,call.arguments(nil),true
}

func singleFunctionArgument(text string) string {
 _,arguments,ok:=parseFunctionCallArguments(text)
 if !ok || len(arguments)!=1 {return ""}
 return arguments[0]
}
