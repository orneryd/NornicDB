package storage

import "context"

// CatalogUpgradeV4 identifies the one configured catalog record transformed by
// the V3-to-V4 storage upgrade. NodeID is fully namespace-qualified. Transform
// validates and transforms only the record's data string; storage owns native
// decoding, atomic publication and the version marker. It must not reenter the
// storage engine or mutate external state. For example, a catalog owner supplies
// its configured metadata ID and a pure credential-representation transformer.
type CatalogUpgradeV4 struct {
	NodeID    NodeID
	Transform func(context.Context, string) (string, bool, error)
}
