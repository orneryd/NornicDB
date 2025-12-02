// Package text provides APOC text processing functions.
//
// This package implements all apoc.text.* functions for string
// manipulation and text processing in Cypher queries.
package text

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Join joins a list of strings with a delimiter.
//
// Example:
//   apoc.text.join(['Hello', 'World'], ' ') => 'Hello World'
func Join(strs []string, delimiter string) string {
	return strings.Join(strs, delimiter)
}

// Split splits a string by a delimiter.
//
// Example:
//   apoc.text.split('Hello World', ' ') => ['Hello', 'World']
func Split(text, delimiter string) []string {
	if delimiter == "" {
		return []string{text}
	}
	return strings.Split(text, delimiter)
}

// Replace replaces all occurrences of a substring.
//
// Example:
//   apoc.text.replace('Hello World', 'World', 'Universe') 
//   => 'Hello Universe'
func Replace(text, old, new string) string {
	return strings.ReplaceAll(text, old, new)
}

// RegexGroups extracts regex capture groups.
//
// Example:
//   apoc.text.regexGroups('abc123def', '([a-z]+)([0-9]+)([a-z]+)')
//   => [['abc123def', 'abc', '123', 'def']]
func RegexGroups(text, pattern string) [][]string {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return [][]string{}
	}
	return re.FindAllStringSubmatch(text, -1)
}

// Capitalize capitalizes the first letter of each word.
//
// Example:
//   apoc.text.capitalize('hello world') => 'Hello World'
func Capitalize(text string) string {
	return strings.Title(text)
}

// CapitalizeAll capitalizes all letters.
//
// Example:
//   apoc.text.capitalizeAll('hello world') => 'HELLO WORLD'
func CapitalizeAll(text string) string {
	return strings.ToUpper(text)
}

// Decapitalize makes the first letter lowercase.
//
// Example:
//   apoc.text.decapitalize('Hello World') => 'hello World'
func Decapitalize(text string) string {
	if len(text) == 0 {
		return text
	}
	r, size := utf8.DecodeRuneInString(text)
	return string(unicode.ToLower(r)) + text[size:]
}

// DecapitalizeAll makes all letters lowercase.
//
// Example:
//   apoc.text.decapitalizeAll('HELLO WORLD') => 'hello world'
func DecapitalizeAll(text string) string {
	return strings.ToLower(text)
}

// SwapCase swaps the case of all letters.
//
// Example:
//   apoc.text.swapCase('Hello World') => 'hELLO wORLD'
func SwapCase(text string) string {
	var result strings.Builder
	for _, r := range text {
		if unicode.IsUpper(r) {
			result.WriteRune(unicode.ToLower(r))
		} else if unicode.IsLower(r) {
			result.WriteRune(unicode.ToUpper(r))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// CamelCase converts text to camelCase.
//
// Example:
//   apoc.text.camelCase('hello world') => 'helloWorld'
//   apoc.text.camelCase('hello_world') => 'helloWorld'
func CamelCase(text string) string {
	words := splitWords(text)
	if len(words) == 0 {
		return ""
	}
	
	var result strings.Builder
	result.WriteString(strings.ToLower(words[0]))
	for i := 1; i < len(words); i++ {
		result.WriteString(Capitalize(strings.ToLower(words[i])))
	}
	return result.String()
}

// SnakeCase converts text to snake_case.
//
// Example:
//   apoc.text.snakeCase('HelloWorld') => 'hello_world'
//   apoc.text.snakeCase('hello world') => 'hello_world'
func SnakeCase(text string) string {
	words := splitWords(text)
	for i := range words {
		words[i] = strings.ToLower(words[i])
	}
	return strings.Join(words, "_")
}

// UpperCamelCase converts text to UpperCamelCase (PascalCase).
//
// Example:
//   apoc.text.upperCamelCase('hello world') => 'HelloWorld'
func UpperCamelCase(text string) string {
	words := splitWords(text)
	var result strings.Builder
	for _, word := range words {
		result.WriteString(Capitalize(strings.ToLower(word)))
	}
	return result.String()
}

// Clean removes extra whitespace and trims.
//
// Example:
//   apoc.text.clean('  hello   world  ') => 'hello world'
func Clean(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

// Comparecleanstrings compares two strings after cleaning.
//
// Example:
//   apoc.text.compareCleaned('  Hello  ', 'hello') => true
func CompareCleaned(text1, text2 string) bool {
	return Clean(strings.ToLower(text1)) == Clean(strings.ToLower(text2))
}

// Distance calculates Levenshtein distance between two strings.
//
// Example:
//   apoc.text.distance('kitten', 'sitting') => 3
func Distance(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}
	
	// Create matrix
	matrix := make([][]int, len(s1)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(s2)+1)
	}
	
	// Initialize first row and column
	for i := 0; i <= len(s1); i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len(s2); j++ {
		matrix[0][j] = j
	}
	
	// Fill matrix
	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}
			matrix[i][j] = min(
				matrix[i-1][j]+1,      // deletion
				min(matrix[i][j-1]+1,  // insertion
					matrix[i-1][j-1]+cost), // substitution
			)
		}
	}
	
	return matrix[len(s1)][len(s2)]
}

// FuzzyMatch checks if strings are similar within a threshold.
//
// Example:
//   apoc.text.fuzzyMatch('hello', 'helo') => true
func FuzzyMatch(s1, s2 string, threshold float64) bool {
	maxLen := max(len(s1), len(s2))
	if maxLen == 0 {
		return true
	}
	distance := Distance(s1, s2)
	similarity := 1.0 - float64(distance)/float64(maxLen)
	return similarity >= threshold
}

// Hammingdistance calculates Hamming distance (for equal-length strings).
//
// Example:
//   apoc.text.hammingDistance('karolin', 'kathrin') => 3
func HammingDistance(s1, s2 string) int {
	if len(s1) != len(s2) {
		return -1 // Invalid for different lengths
	}
	
	distance := 0
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			distance++
		}
	}
	return distance
}

// JaroWinklerDistance calculates Jaro-Winkler similarity.
//
// Example:
//   apoc.text.jaroWinklerDistance('martha', 'marhta') => 0.96
func JaroWinklerDistance(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}
	if len(s1) == 0 || len(s2) == 0 {
		return 0.0
	}
	
	// Calculate Jaro similarity
	matchWindow := max(len(s1), len(s2))/2 - 1
	if matchWindow < 1 {
		matchWindow = 1
	}
	
	s1Matches := make([]bool, len(s1))
	s2Matches := make([]bool, len(s2))
	matches := 0
	transpositions := 0
	
	// Find matches
	for i := 0; i < len(s1); i++ {
		start := max(0, i-matchWindow)
		end := min(i+matchWindow+1, len(s2))
		
		for j := start; j < end; j++ {
			if s2Matches[j] || s1[i] != s2[j] {
				continue
			}
			s1Matches[i] = true
			s2Matches[j] = true
			matches++
			break
		}
	}
	
	if matches == 0 {
		return 0.0
	}
	
	// Count transpositions
	k := 0
	for i := 0; i < len(s1); i++ {
		if !s1Matches[i] {
			continue
		}
		for !s2Matches[k] {
			k++
		}
		if s1[i] != s2[k] {
			transpositions++
		}
		k++
	}
	
	jaro := (float64(matches)/float64(len(s1)) +
		float64(matches)/float64(len(s2)) +
		float64(matches-transpositions/2)/float64(matches)) / 3.0
	
	// Calculate Jaro-Winkler
	prefix := 0
	for i := 0; i < min(len(s1), len(s2)) && i < 4; i++ {
		if s1[i] == s2[i] {
			prefix++
		} else {
			break
		}
	}
	
	return jaro + float64(prefix)*0.1*(1.0-jaro)
}

// Lpad pads a string on the left to a given length.
//
// Example:
//   apoc.text.lpad('5', 3, '0') => '005'
func Lpad(text string, length int, pad string) string {
	if len(text) >= length {
		return text
	}
	padding := strings.Repeat(pad, (length-len(text)+len(pad)-1)/len(pad))
	return padding[:length-len(text)] + text
}

// Rpad pads a string on the right to a given length.
//
// Example:
//   apoc.text.rpad('5', 3, '0') => '500'
func Rpad(text string, length int, pad string) string {
	if len(text) >= length {
		return text
	}
	padding := strings.Repeat(pad, (length-len(text)+len(pad)-1)/len(pad))
	return text + padding[:length-len(text)]
}

// Format formats a string using placeholders.
//
// Example:
//   apoc.text.format('Hello %s, you are %d years old', ['Alice', 30])
//   => 'Hello Alice, you are 30 years old'
func Format(format string, args []interface{}) string {
	return fmt.Sprintf(format, args...)
}

// Repeat repeats a string n times.
//
// Example:
//   apoc.text.repeat('ab', 3) => 'ababab'
func Repeat(text string, count int) string {
	return strings.Repeat(text, count)
}

// Reverse reverses a string.
//
// Example:
//   apoc.text.reverse('hello') => 'olleh'
func Reverse(text string) string {
	runes := []rune(text)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// Slug converts text to a URL-friendly slug.
//
// Example:
//   apoc.text.slug('Hello World!') => 'hello-world'
func Slug(text string) string {
	// Convert to lowercase
	text = strings.ToLower(text)
	
	// Replace non-alphanumeric with hyphens
	re := regexp.MustCompile(`[^a-z0-9]+`)
	text = re.ReplaceAllString(text, "-")
	
	// Remove leading/trailing hyphens
	text = strings.Trim(text, "-")
	
	return text
}

// SorensenDiceSimilarity calculates Sørensen-Dice coefficient.
//
// Example:
//   apoc.text.sorensenDiceSimilarity('night', 'nacht') => 0.25
func SorensenDiceSimilarity(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}
	if len(s1) < 2 || len(s2) < 2 {
		return 0.0
	}
	
	// Get bigrams
	bigrams1 := getBigrams(s1)
	bigrams2 := getBigrams(s2)
	
	// Count intersections
	intersection := 0
	for bigram := range bigrams1 {
		if bigrams2[bigram] {
			intersection++
		}
	}
	
	return 2.0 * float64(intersection) / float64(len(bigrams1)+len(bigrams2))
}

// Trim removes leading and trailing whitespace.
//
// Example:
//   apoc.text.trim('  hello  ') => 'hello'
func Trim(text string) string {
	return strings.TrimSpace(text)
}

// Ltrim removes leading whitespace.
//
// Example:
//   apoc.text.ltrim('  hello  ') => 'hello  '
func Ltrim(text string) string {
	return strings.TrimLeft(text, " \t\n\r")
}

// Rtrim removes trailing whitespace.
//
// Example:
//   apoc.text.rtrim('  hello  ') => '  hello'
func Rtrim(text string) string {
	return strings.TrimRight(text, " \t\n\r")
}

// Urlencode encodes a string for use in URLs.
//
// Example:
//   apoc.text.urlencode('hello world') => 'hello+world'
func Urlencode(text string) string {
	return strings.ReplaceAll(text, " ", "+")
}

// Urldecode decodes a URL-encoded string.
//
// Example:
//   apoc.text.urldecode('hello+world') => 'hello world'
func Urldecode(text string) string {
	return strings.ReplaceAll(text, "+", " ")
}

// Base64Encode encodes a string to base64.
//
// Example:
//   apoc.text.base64Encode('hello') => 'aGVsbG8='
func Base64Encode(text string) string {
	// Note: In production, use encoding/base64
	return text // Placeholder
}

// Base64Decode decodes a base64 string.
//
// Example:
//   apoc.text.base64Decode('aGVsbG8=') => 'hello'
func Base64Decode(text string) string {
	// Note: In production, use encoding/base64
	return text // Placeholder
}

// IndexOf returns the index of the first occurrence of a substring.
//
// Example:
//   apoc.text.indexOf('hello world', 'world') => 6
func IndexOf(text, substring string) int {
	return strings.Index(text, substring)
}

// IndexesOf returns all indexes of a substring.
//
// Example:
//   apoc.text.indexesOf('hello hello', 'hello') => [0, 6]
func IndexesOf(text, substring string) []int {
	indexes := make([]int, 0)
	start := 0
	for {
		index := strings.Index(text[start:], substring)
		if index == -1 {
			break
		}
		indexes = append(indexes, start+index)
		start += index + len(substring)
	}
	return indexes
}

// Code returns the Unicode code point of the first character.
//
// Example:
//   apoc.text.code('A') => 65
func Code(text string) int {
	if len(text) == 0 {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(text)
	return int(r)
}

// FromCodePoint returns a string from a Unicode code point.
//
// Example:
//   apoc.text.fromCodePoint(65) => 'A'
func FromCodePoint(code int) string {
	return string(rune(code))
}

// Bytes returns the byte representation of a string.
//
// Example:
//   apoc.text.bytes('hello') => [104, 101, 108, 108, 111]
func Bytes(text string) []byte {
	return []byte(text)
}

// BytesToString converts bytes to a string.
//
// Example:
//   apoc.text.bytesToString([104, 101, 108, 108, 111]) => 'hello'
func BytesToString(bytes []byte) string {
	return string(bytes)
}

// Phonetic returns a phonetic encoding (Soundex).
//
// Example:
//   apoc.text.phonetic('Smith') => 'S530'
func Phonetic(text string) string {
	return soundex(text)
}

// PhoneticDelta calculates phonetic similarity.
//
// Example:
//   apoc.text.phoneticDelta('Smith', 'Smythe') => 0 (same soundex)
func PhoneticDelta(s1, s2 string) int {
	code1 := soundex(s1)
	code2 := soundex(s2)
	if code1 == code2 {
		return 0
	}
	return 4 // Maximum difference
}

// Doublemet aphonetic encoding (Double Metaphone).
//
// Example:
//   apoc.text.doubleMetaphone('Smith') => ['SM0', 'XMT']
func DoubleMetaphone(text string) []string {
	// Note: In production, implement full Double Metaphone algorithm
	return []string{soundex(text)} // Placeholder
}

// Helper functions

func splitWords(text string) []string {
	// Split on spaces, underscores, hyphens, and camelCase boundaries
	var words []string
	var current strings.Builder
	
	for i, r := range text {
		if unicode.IsSpace(r) || r == '_' || r == '-' {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
		} else if i > 0 && unicode.IsUpper(r) && unicode.IsLower(rune(text[i-1])) {
			// CamelCase boundary
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			current.WriteRune(r)
		} else {
			current.WriteRune(r)
		}
	}
	
	if current.Len() > 0 {
		words = append(words, current.String())
	}
	
	return words
}

func getBigrams(text string) map[string]bool {
	bigrams := make(map[string]bool)
	for i := 0; i < len(text)-1; i++ {
		bigrams[text[i:i+2]] = true
	}
	return bigrams
}

func soundex(text string) string {
	if len(text) == 0 {
		return ""
	}
	
	text = strings.ToUpper(text)
	
	// Soundex mapping
	mapping := map[rune]rune{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}
	
	var result strings.Builder
	result.WriteRune(rune(text[0]))
	
	prevCode := mapping[rune(text[0])]
	for i := 1; i < len(text) && result.Len() < 4; i++ {
		code := mapping[rune(text[i])]
		if code != 0 && code != prevCode {
			result.WriteRune(code)
			prevCode = code
		} else if code == 0 {
			prevCode = 0
		}
	}
	
	// Pad with zeros
	for result.Len() < 4 {
		result.WriteRune('0')
	}
	
	return result.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
