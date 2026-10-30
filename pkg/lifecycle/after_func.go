package lifecycle

import "context"

// AfterFuncJoined arranges fn to run once ctx is done, like context.AfterFunc,
// and returns a stop that settles it: stop either prevents fn from running or,
// when fn has already started, waits for it to return. After stop returns, fn
// is not running and never will be; repeated and concurrent stops join the
// same settlement. It is the one stop-callback-and-join mechanism for a
// callback whose resources its caller releases after stop.
func AfterFuncJoined(ctx context.Context, fn func()) (stop func()) {
	requireContext("AfterFuncJoined", ctx)
	var ran, settled OnceCompletion
	prevent := context.AfterFunc(ctx, func() { ran.Run(fn) })
	// Every stop caller, repeated or concurrent, joins the first settlement:
	// a later prevent reports false without fn ever having run.
	return func() {
		settled.Run(func() {
			if !prevent() {
				<-ran.Done()
			}
		})
	}
}
