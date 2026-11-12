package queryprofile

import (
	"errors"
	"testing"
)

func TestRequestResultDistinguishesStagingOutcomeFromAttempt(t *testing.T) {
	counter := new(ReadCounter)
	RequestResult(counter, nil)
	RequestResult(counter, errors.New("staging rejected"))
	RequestResult(nil, nil)
	RequestResult(nil, errors.New("unobserved refusal"))
	rows, hits := counter.Values()
	if rows != 1 || hits != 2 {
		t.Fatalf("got acknowledgements=%d attempts=%d; want 1 and 2", rows, hits)
	}
}
