package testfixture

// EncodingOwner is the fatal assertion boundary supplied by testing.T or B.
type EncodingOwner interface {
	Helper()
	Fatalf(string, ...any)
}

// Encode runs the explicitly selected fixture codec and fails its test on error.
// For example, Encode(t, msgpack.Marshal, historicalValue) retains a frozen format.
func Encode[T any](owner EncodingOwner, encode func(T) ([]byte, error), value T) []byte {
	owner.Helper()
	data, err := encode(value)
	if err != nil {
		owner.Fatalf("encoding test fixture: %v", err)
	}
	return data
}
