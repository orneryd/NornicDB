package queryprofile

import (
	"errors"
	"testing"
)

func TestEntityReadResultReportsActualPointOutcome(t *testing.T) {
	value := 1
	for _, tc := range []struct {
		name  string
		image *int
		err   error
		rows  int64
	}{
		{"found", &value, nil, 1}, {"absent", nil, nil, 0},
		{"failed", nil, errors.New("read failed"), 0},
		{"failed partial image", &value, errors.New("read incomplete"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := new(ReadCounter)
			EntityReadResult(counter, tc.image, tc.err)
			rows, hits := counter.Values()
			if rows != tc.rows || hits != 1 {
				t.Fatalf("got rows=%d hits=%d; want rows=%d hits=1", rows, hits, tc.rows)
			}
			EntityReadResult[int](nil, tc.image, tc.err)
		})
	}
}
