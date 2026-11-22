package storage

// Head-only: skipUnselectedProperty does not exist on the base.

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// s1SkipPair runs the PR's skip and the decoder's own Skip (what the base did)
// over the same bytes and reports whether they differ in outcome or position.
func s1SkipPair(raw []byte) string {
	run := func(skip func(*msgpack.Decoder, *bytes.Reader) error) (consumed int, err error) {
		reader := bytes.NewReader(raw)
		dec := msgpack.NewDecoder(reader)
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("PANIC %v", p)
			}
		}()
		err = skip(dec, reader)
		return len(raw) - reader.Len(), err
	}
	oldN, oldErr := run(func(d *msgpack.Decoder, _ *bytes.Reader) error { return d.Skip() })
	newN, newErr := run(func(d *msgpack.Decoder, r *bytes.Reader) error { return skipUnselectedProperty(d, r, raw) })
	if (oldErr == nil) != (newErr == nil) {
		return fmt.Sprintf("outcome differs: old=%v new=%v raw=%x", oldErr, newErr, raw)
	}
	if oldErr == nil && oldN != newN {
		return fmt.Sprintf("position differs: old=%d new=%d raw=%x", oldN, newN, raw)
	}
	if newErr != nil && len(newErr.Error()) > 5 && newErr.Error()[:5] == "PANIC" {
		return newErr.Error()
	}
	return ""
}

func TestPR1826S1SkipMatchesDecoderSkipHead(t *testing.T) {
	tails := [][]byte{{}, {0}, {1}, {0, 0}, {0, 1}, {0, 0, 0, 0}, {0, 0, 0, 1}, {0xff, 0xff, 0xff, 0xff}, {0x7f, 0xff, 0xff, 0xff}, {0x80, 0, 0, 0}, {0, 0, 1, 0}, {3, 5, 'a', 'b', 'c'}, {0, 3, 5, 'a', 'b', 'c'}, {0, 0, 0, 3, 5, 'a', 'b', 'c'}}
	bad := 0
	for code := 0; code < 256; code++ {
		for _, tail := range tails {
			for _, pad := range []int{0, 1, 2, 15, 16, 17, 31, 32, 33, 300, 70000} {
				raw := append([]byte{byte(code)}, tail...)
				raw = append(raw, make([]byte, pad)...)
				if v := s1SkipPair(raw); v != "" {
					bad++
					if bad <= 20 {
						t.Errorf("%.300s", v)
					}
				}
			}
		}
	}
	t.Logf("S1COUNT skip mismatches: %d", bad)
}

func FuzzPR1826S1SkipHead(f *testing.F) {
	f.Add([]byte{0xd9, 3, 'a', 'b', 'c'})
	f.Add([]byte{0xc7, 0, 5})
	f.Add([]byte{0xc9, 0xff, 0xff, 0xff, 0xff, 5})
	f.Add([]byte{0x93, 0xa1, 'x', 0xc4, 1, 2, 0xc0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if v := s1SkipPair(raw); v != "" {
			t.Fatal(v)
		}
	})
}
