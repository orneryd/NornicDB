package cyphertext

import "testing"

func TestPreviousLiveByteSourceWindows(t *testing.T) {
	cases := []struct {
		text           string
		position, want int
	}{
		{"a /* hidden */ b", 15, 0}, {"a /* hidden */ b", 16, 15},
		{"a 'xy' b", 3, 2}, {"a 'xy' b", 5, 2}, {"a 'xy' b", 6, 5},
		{"a // hidden\nb", 12, 0}, {"a // hidden\nb", 13, 12},
		{"a 'unterminated", 15, 2}, {"a  ", 3, 0}, {"", 0, -1},
	}
	for _, c := range cases {
		if got := PreviousLiveByte(c.text, c.position); got != c.want {
			t.Errorf("%q at%d: got%d want%d", c.text, c.position, got, c.want)
		}
	}
}
