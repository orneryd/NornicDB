package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

var _ storage.TransactionEqualityLookup = (*transactionStorageWrapper)(nil)

// LookupNodesByEquality keeps candidates in the transaction's database and
// converts only the matching results back to the caller's namespace view.
// A participant's storage that has begun no transaction is a
// read-committed view: the caller uses its normal scan.
func (w *transactionStorageWrapper) LookupNodesByEquality(label string, properties map[string]interface{}) ([]*storage.Node, bool, error) {
	tx := w.transaction()
	if tx == nil {
		return nil, false, nil
	}
	if w.namespace != "" {
		if err := tx.SetNamespace(w.namespace); err != nil {
			return nil, false, err
		}
	}
	nodes, complete, err := tx.LookupNodesByEquality(label, properties)
	if err != nil || !complete || w.namespace == "" {
		return nodes, complete, err
	}
	return w.toUserNamespacedNodes(nodes), true, nil
}
