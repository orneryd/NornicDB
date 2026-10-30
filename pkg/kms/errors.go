package kms

import "errors"

var (
	ErrUnsupportedProvider = errors.New("kms: unsupported provider")
	ErrInvalidConfig       = errors.New("kms: invalid configuration")
	ErrKeyNotFound         = errors.New("kms: key not found")
	ErrDecryptFailed       = errors.New("kms: data key decrypt failed")
	ErrEncryptFailed       = errors.New("kms: data key encrypt failed")
	ErrClosed              = errors.New("kms: provider closed")
)
