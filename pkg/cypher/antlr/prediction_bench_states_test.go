package antlr

// benchPredictionStates is the DFA state count of every shared prediction
// generation.
func benchPredictionStates() int64 { return exactSharedStates() }
