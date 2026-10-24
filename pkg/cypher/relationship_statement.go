package cypher

// admittedRelationshipStatement is produced only by statement admission. It is
// an explicit kernel input, independent of cancellation and row context wrappers.
// Passing the existing immutable program pointer adds no allocation or cached
// row, parameter, storage-view or schema ownership.
// Statement adapters carry this input directly. Only a parsed access-site
// kernel requires the stronger relationshipAccessExecution capability; an
// adapter must never manufacture an empty access frame to forward admission.
type admittedRelationshipStatement struct {
	program *relationshipAccessProgram
}
