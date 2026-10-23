package bulk

// Stopped is the cancellation owner's stop. A check that finds the
// statement canceled unwinds with it to the nearest boundary, where
// RecoverStop turns it into the context's error. The work it stops returns
// nothing: no partial result of a stopped pass reaches a caller, so none is
// returned, compared, written, committed or cached.
type Stopped struct {
	Err error
}

// RecoverStop, deferred at a boundary, turns the owner's stop into *err.
// Any other panic continues unchanged, with its value and its stack.
func RecoverStop(err *error) {
	if failure := recover(); failure != nil {
		stop, ok := failure.(Stopped)
		if !ok {
			panic(failure)
		}
		*err = stop.Err
	}
}
