package queryprofile

// RequestResult records one actual native mutation request a statement issued:
// the staging call it made on its transaction, never a physical write, a final
// statistic or a row count derived from either. The commit decides what reaches
// storage, so the request's own outcome is the only fact the statement owns: a
// call that returned no error was acknowledged and contributes one row, and a
// call that failed contributes none. Either way it was one request, hence one
// hit.
//
// It takes the counter rather than the context because a batch resolves its
// request counter once and reports every call of the loop to it; the record
// carries no context, name or identity of its own. A nil counter - what a
// statement nobody profiles resolved - reports nothing and allocates nothing.
func RequestResult(counter *ReadCounter, err error) {
	if counter == nil {
		return
	}
	var acknowledged int64
	if err == nil {
		acknowledged = 1
	}
	counter.Add(acknowledged, 1)
}
