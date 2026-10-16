package fn

import (
	"strings"
	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/transform"
)

// CharacterCount counts Unicode code points, borrowing the caller's work
// budget for a large string. Invalid UTF-8 retains RuneCountInString semantics.
func CharacterCount(text string, cancellation Cancellation) (int, error) {
	length, ok := cyphertext.LengthWithWork(text, cancellation)
	if !ok { return 0, cancellation.Err() }
	return length, nil
}

// MapStringCase is the Unicode case mapping shared by the two spellings of
// upper/lower. One transformer preserves context across input chunks.
func MapStringCase(text string, upper bool, cancellation Cancellation) (string,error) {
	if cancellation == nil || len(text) <= 4096 {
		if upper { return upperCaseText(text),nil }
		return lowerCaseText(text),nil
	}
	caser := cases.Lower(language.Und)
	if upper { caser = cases.Upper(language.Und) }
	return TransformString(text,caser,cancellation)
}

// TransformString applies a stateful Unicode transform in bounded input
// chunks. The supplied cancellation budget remains the only polling owner.
func TransformString(text string, transformer transform.Transformer, cancellation Cancellation) (string,error) {
	if cancellation == nil {
		result,_,err := transform.String(transformer,text)
		return result,err
	}
	var result strings.Builder
	writer := transform.NewWriter(&result,transformer)
	for offset:=0;offset<len(text);offset+=4096 {
		if !cancellation.Continue() { return "",cancellation.Err() }
		if _,err:=writer.Write([]byte(text[offset:min(offset+4096,len(text))]));err!=nil{return "",err}
	}
	if err:=writer.Close();err!=nil{return "",err}
	return result.String(),nil
}

func labelValues(labels []string,cancellation Cancellation)([]interface{},error){
	capacity:=len(labels)
	if cancellation!=nil{capacity=min(capacity,128)}
	result:=make([]interface{},0,capacity)
	for _,label:=range labels{
		if cancellation!=nil && !cancellation.Continue(){return nil,cancellation.Err()}
		result=append(result,label)
	}
	return result,nil
}
