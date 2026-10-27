package multidb

// DatabaseCreateRequest describes one atomic standard-database catalog creation.
// Options are initial metadata, published with the name through the same storage
// receipt. The manager validates and copies them; later caller changes do not
// modify the database. For example:
//
//	manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: "tenant_a"})
//	manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: "audit", Options: map[string]string{OptionTxLogEnrichment: "FULL"}})
//
// Existing databases remain errors; query-level IF NOT EXISTS admission belongs
// to its caller. Request cancellation is governed by the catalog commit receipt.
type DatabaseCreateRequest struct {
	Name    string
	Options map[string]string
}
