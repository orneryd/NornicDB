package kms

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWrappingDisplaySecret(t *testing.T) {
	const credential = "synthetic-kms-credential-314159"
	failure := errors.New("upstream reflected " + credential)
	a := newWrappingAdapter(&fakeWrapper{encryptErr: failure}, credential)
	_, err := a.GenerateDataKey(context.Background(), KeyGenOpts{})
	if !errors.Is(err, ErrEncryptFailed) || strings.Contains(err.Error(), credential) {
		t.Error("KMS display or error classification failed")
	}
}
