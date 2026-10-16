package antlr

func PredictionModegetSingleViableAlt(altsets []*BitSet) int {
	return PredictionModegetSingleViableAltWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModehasStateAssociatedWithOneAlt(configs *ATNConfigSet) bool {
	return PredictionModehasStateAssociatedWithOneAltWithWork(configs, nil)
}
func PredictionModeGetStateToAltMap(configs *ATNConfigSet) *AltDict {
	return PredictionModeGetStateToAltMapWithWork(configs, nil)
}
func PredictionModegetConflictingAltSubsets(configs *ATNConfigSet) []*BitSet {
	return PredictionModegetConflictingAltSubsetsWithWork(configs, nil).materialize()
}
func PredictionModeGetAlts(altsets []*BitSet) *BitSet {
	return PredictionModeGetAltsWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModegetUniqueAlt(altsets []*BitSet) int {
	return PredictionModegetUniqueAltWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModeallSubsetsEqual(altsets []*BitSet) bool {
	return PredictionModeallSubsetsEqualWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModehasConflictingAltSet(altsets []*BitSet) bool {
	return PredictionModehasConflictingAltSetWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModehasNonConflictingAltSet(altsets []*BitSet) bool {
	return PredictionModehasNonConflictingAltSetWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModeallSubsetsConflict(altsets []*BitSet) bool {
	return PredictionModeallSubsetsConflictWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModeresolvesToJustOneViableAlt(altsets []*BitSet) int {
	return PredictionModeresolvesToJustOneViableAltWithWork(sequenceFromSlice(altsets), nil)
}
func PredictionModeallConfigsInRuleStopStates(configs *ATNConfigSet) bool {
	return PredictionModeallConfigsInRuleStopStatesWithWork(configs, nil)
}
func PredictionModehasConfigInRuleStopState(configs *ATNConfigSet) bool {
	return PredictionModehasConfigInRuleStopStateWithWork(configs, nil)
}
func PredictionModehasSLLConflictTerminatingPrediction(mode int, configs *ATNConfigSet) bool {
	return PredictionModehasSLLConflictTerminatingPredictionWithWork(mode, configs, nil)
}
