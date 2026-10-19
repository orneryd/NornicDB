package storage

// Reuse the schema persistence and index-cache owners when replaying a schema
// transaction. The ordinary schema commit validates the namespace snapshot,
// excluding a data writer that overtook this rebuild before publication.
func (tx *BadgerTransaction) stageReplaySchema(definition *SchemaDefinition, ordinary bool) error {
	view, err := tx.KnowledgePolicySchema()
	if err != nil {
		return err
	}
	if err := view.ReplaceFromDefinition(definition); err != nil {
		return err
	}
	if ordinary {
		if err := tx.engine.rebuildReplaySchemaValues(tx.namespace, view); err != nil {
			return err
		}
	}
	if err := view.persist(definition); err != nil {
		return err
	}
	if ordinary {
		return tx.StageSchemaChanges()
	}
	return nil
}

type WALSchemaData struct {
	Schema            *SchemaDefinition `json:"schema"`
	SchemaTransaction bool              `json:"schema_transaction,omitempty"`
	TxID              string            `json:"tx_id,omitempty"`
}
