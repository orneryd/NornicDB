package cypher

import (
 "testing"
 "github.com/stretchr/testify/require"
)

func TestFunctionSyntaxStructure(t *testing.T) {
 text:=`substring(/* don't split , ) */ toLower('OKAY'), {k:range(1,2), `+"`a,b`"+`: [1,2]}, 2 /* , ' */)`
 syntax:=readFunctionSyntax(text)
 require.Len(t,syntax.calls,3)
 call,ok:=functionCallAt(text,0);require.True(t,ok)
 require.Equal(t,len(text)-1,call.close(text))
 require.Equal(t,[]string{`toLower('OKAY')`,"{k:range(1,2), `a,b`: [1,2]}","2"},call.arguments(nil))
 require.Equal(t,[]string{"'a,b'","[1,2]","{x:1,y:2}",""},readFunctionSyntax("'a,b', [1,2], {x:1,y:2}, /* ' , */").appendArguments(nil,0,true))
 require.Equal(t,[]string{"'a,b'","[1,2]","{x:1,y:2}"},readFunctionSyntax("'a,b', [1,2], {x:1,y:2}, /* ' , */").appendArguments(nil,0,false))
 require.True(t,hasValidStartKeyword("/* } { ' \" */ RETURN 'ok' AS v"))
}
