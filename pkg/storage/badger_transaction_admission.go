package storage

// SetWriteAdmission makes admit the transaction's write admission: the
// decision, taken before the transaction's first write, whether it may write
// at all. Every data write and the first schema change ask it (AdmitWrite);
// once it admits, later writes don't ask again, and while it refuses, the
// write fails with its error and changes nothing. A knowledge-policy change
// asks it when it is persisted to the transaction. A composite statement's
// transaction on one of its databases admits a write only when that database
// can be the statement's one write database.
func (tx *BadgerTransaction) SetWriteAdmission(admit func() error) {
	tx.mu.Lock()
	tx.writeAdmission = admit
	tx.mu.Unlock()
}

// AdmitWrite asks the transaction's write admission (SetWriteAdmission) for
// a write the transaction is about to make that is no data or schema write
// of its own: a change applied when it commits.
func (tx *BadgerTransaction) AdmitWrite() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.admitWriteLocked()
}

func (tx *BadgerTransaction) admitWriteLocked() error {
	if tx.writeAdmission == nil {
		return nil
	}
	if err := tx.writeAdmission(); err != nil {
		return err
	}
	tx.writeAdmission = nil
	return nil
}
