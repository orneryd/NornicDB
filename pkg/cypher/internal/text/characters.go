// Package text implements Cypher string operations in Unicode code points.
package text

import (
	"strings"
	"unicode/utf8"
)

// Work borrows a caller's existing sampled cancellation budget.
type Work interface { Continue() bool }

func runeValues(value string, work Work) ([]rune, bool) {
	if work == nil { return []rune(value), true }
	result := make([]rune,0,min(len(value),128))
	for _, character := range value {
		if !work.Continue() { return nil,false }
		result = append(result,character)
	}
	return result,true
}

func runeText(value []rune, work Work) (string,bool) {
	if work == nil { return string(value),true }
	var result strings.Builder
	for _,character := range value {
		if !work.Continue() { return "",false }
		result.WriteRune(character)
	}
	return result.String(),true
}

// Length returns the number of Unicode code points in value.
func Length(value string) int {
	length,_ := LengthWithWork(value,nil)
	return length
}

// LengthWithWork counts a large string through the borrowed budget.
func LengthWithWork(value string, work Work)(int,bool){
	if work == nil || len(value)<=4096 { return utf8.RuneCountInString(value),true }
	length:=0
	for range value { if !work.Continue(){return 0,false};length++ }
	return length,true
}

// Substring returns at most length Unicode code points beginning at start.
// Negative starts retain the evaluators' historical clamp-to-zero behavior;
// callers that expose Cypher argument errors validate before this helper.
func Substring(value string, start, length int) string {
	result,_ := SubstringWithWork(value,start,length,nil)
	return result
}

// SubstringWithWork is Substring with the caller's existing work budget.
func SubstringWithWork(value string,start,length int,work Work)(string,bool){
	if length < 0 {
		return "",true
	}
	if start < 0 {
		start = 0
	}
	runes,ok := runeValues(value,work)
	if !ok {return "",false}
	if start >= len(runes) {
		return "",true
	}
	end := start + length
	if end < start || end > len(runes) {
		end = len(runes)
	}
	return runeText(runes[start:end],work)
}

// From returns the suffix beginning at the zero-based Unicode code-point index.
func From(value string, start int) string {
	result,_ := FromWithWork(value,start,nil)
	return result
}

// FromWithWork is From with the caller's existing work budget.
func FromWithWork(value string,start int,work Work)(string,bool){
	if start < 0 {
		start = 0
	}
	runes,ok := runeValues(value,work)
	if !ok{return "",false}
	if start >= len(runes) {
		return "",true
	}
	return runeText(runes[start:],work)
}

// Left returns at most length Unicode code points from the start of value.
func Left(value string, length int) string {
	result,_ := LeftWithWork(value,length,nil)
	return result
}

// LeftWithWork is Left with the caller's existing work budget.
func LeftWithWork(value string,length int,work Work)(string,bool){
	if length < 0 {
		return "",true
	}
	runes,ok := runeValues(value,work)
	if !ok{return "",false}
	if length >= len(runes) {
		return value,true
	}
	return runeText(runes[:length],work)
}

// Right returns at most length Unicode code points from the end of value.
func Right(value string, length int) string {
	result,_ := RightWithWork(value,length,nil)
	return result
}

// RightWithWork is Right with the caller's existing work budget.
func RightWithWork(value string,length int,work Work)(string,bool){
	if length < 0 {
		return "",true
	}
	runes,ok := runeValues(value,work)
	if !ok{return "",false}
	if length >= len(runes) {
		return value,true
	}
	return runeText(runes[len(runes)-length:],work)
}

// At returns the Unicode character at index. Negative indexes count from the
// end, matching Cypher list/string indexing semantics.
func At(value string, index int) (string, bool) {
	return AtWithWork(value,index,nil)
}

// AtWithWork is At with the caller's existing work budget.
func AtWithWork(value string,index int,work Work)(string,bool){
	runes,ok := runeValues(value,work)
	if !ok{return "",false}
	if index < 0 {
		index += len(runes)
	}
	if index < 0 || index >= len(runes) {
		return "", false
	}
	return string(runes[index]), true
}
