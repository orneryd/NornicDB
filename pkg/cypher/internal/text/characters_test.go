package text

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnicodeCharacterOperations(t *testing.T) {
	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{name: "length counts code points", got: Length("a界🙂"), want: 3},
		{name: "substring uses code point offsets", got: Substring("привет", 1, 3), want: "рив"},
		{name: "substring clamps end", got: Substring("東京", 1, 9), want: "京"},
		{name: "substring clamps negative start", got: Substring("abc", -1, 1), want: "a"},
		{name: "substring rejects negative length", got: Substring("abc", 0, -1), want: ""},
		{name: "suffix uses code point offset", got: From("a界🙂", 1), want: "界🙂"},
		{name: "suffix clamps negative start", got: From("a界🙂", -1), want: "a界🙂"},
		{name: "left uses code point length", got: Left("🙂🙃a", 2), want: "🙂🙃"},
		{name: "right uses code point length", got: Right("a界🙂", 2), want: "界🙂"},
		{name: "left clamps length", got: Left("界", 2), want: "界"},
		{name: "right rejects negative length", got: Right("界", -1), want: ""},
		{name: "index uses code point offset", got: mustCharacter(At("a界🙂", 1)), want: "界"},
		{name: "negative index counts from end", got: mustCharacter(At("a界🙂", -1)), want: "🙂"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

func mustCharacter(value string, ok bool) string {
	if !ok {
		return ""
	}
	return value
}
