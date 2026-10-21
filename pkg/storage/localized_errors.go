package storage

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func localizedError(message localization.Message, cause error) error {
	return localization.NewLocalizedError(string(message.ID), message, cause)
}

type schemaAdmissionError struct {
	code  string
	cause error
}

func (err *schemaAdmissionError) Error() string         { return err.cause.Error() }
func (err *schemaAdmissionError) Unwrap() error         { return err.cause }
func (err *schemaAdmissionError) BoltErrorCode() string { return err.code }

func newSchemaAdmissionError(code string, message localization.Message) error {
	return &schemaAdmissionError{code: "Neo.ClientError.Schema." + code, cause: localizedError(message, nil)}
}

// HiddenEdgeIdentityConflictError is the permanent result of a CREATE whose
// physical ID belongs to a hidden relationship but whose final transaction
// image is not semantically identical. It unwraps to ErrAlreadyExists for
// storage callers while exposing a non-retryable client status to Bolt/HTTP.
type HiddenEdgeIdentityConflictError struct {
	EdgeID    EdgeID
	Namespace string
}

func (err *HiddenEdgeIdentityConflictError) Error() string {
	id := strings.TrimPrefix(string(err.EdgeID), err.Namespace+":")
	return localizedError(localization.StorageTransactionHiddenEdgeIdentityConflict(id), ErrAlreadyExists).Error()
}

func (err *HiddenEdgeIdentityConflictError) Unwrap() error { return ErrAlreadyExists }

func (err *HiddenEdgeIdentityConflictError) BoltErrorCode() string {
	return "Neo.ClientError.Schema.ConstraintValidationFailed"
}

func (err *HiddenEdgeIdentityConflictError) BoltErrorDetail() string {
	return "RelationshipIdentityConflict"
}

// NodeStillConnectedError fails the commit of a transaction that deleted a
// node with a non-DETACH DELETE while relationships still connected it at
// commit (BadgerTransaction.DeleteConnectedNode): Neo4j's DeleteConnectedNode
// constraint, a Neo.ClientError.Schema.ConstraintValidationFailed.
type NodeStillConnectedError struct {
	NodeID NodeID
	// Namespace is the transaction's database: the message names the node
	// as its clients do, without the namespace prefix.
	Namespace string
}

func (err *NodeStillConnectedError) Error() string {
	id := strings.TrimPrefix(string(err.NodeID), err.Namespace+":")
	return localizedError(localization.CypherTransactionsDeleteResidualRelationships(id), nil).Error()
}

// BoltErrorCode is the status Bolt reports for the error.
func (err *NodeStillConnectedError) BoltErrorCode() string {
	return "Neo.ClientError.Schema.ConstraintValidationFailed"
}

// BoltErrorDetail names the constraint, as the DELETE-time error does.
func (err *NodeStillConnectedError) BoltErrorDetail() string { return "DeleteConnectedNode" }

// ConnectedNodeDeleter is a store that can delete a node that relationships
// still connect, failing the commit unless they are deleted first
// (BadgerTransaction.DeleteConnectedNode).
type ConnectedNodeDeleter interface {
	DeleteConnectedNode(id NodeID) error
}

// IncidentEdgeLister is a store that lists every relationship connecting a
// node, those the visibility layer hides from reads included
// (BadgerTransaction.IncidentEdges). A DELETE reads them: a hidden
// relationship still connects its endpoints, so it keeps a non-DETACH DELETE
// from removing the node, and DETACH DELETE removes and counts it.
type IncidentEdgeLister interface {
	IncidentEdges(nodeID NodeID) ([]*Edge, error)
}
