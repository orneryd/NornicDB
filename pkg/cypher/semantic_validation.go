package cypher

// validateSemanticScopes is the shared compile-time semantic chokepoint used
// by top-level statements and internally executed query branches.
func (e *StorageExecutor) validateSemanticScopes(cypher string) error {
	if err := e.validateMatchSemanticScopes(cypher); err != nil {
		return err
	}
	if err := e.validateCreateSemanticScopes(cypher); err != nil {
		return err
	}
	if err := e.validateMergeSemanticScopes(cypher); err != nil {
		return err
	}
	return e.validateSetSemanticScopes(cypher)
}
