package catalogadapter

import (
	"github.com/orneryd/nornicdb/pkg/multidb"
	"time"
)

// DatabaseInfoInterface is the shared catalog metadata view exposed to Cypher.
// It retains the exact names, types, and timestamps supplied by the manager.
type DatabaseInfoInterface interface {
	Name() string
	Type() string
	Status() string
	IsDefault() bool
	CreatedAt() time.Time
}

// InfoView presents native database metadata through the shared read contract.
// The manager owns the pointed-to record's snapshot lifetime.
type InfoView struct{ Info *multidb.DatabaseInfo }

func (v *InfoView) Name() string         { return v.Info.Name }
func (v *InfoView) Type() string         { return v.Info.Type }
func (v *InfoView) Status() string       { return v.Info.Status }
func (v *InfoView) IsDefault() bool      { return v.Info.IsDefault }
func (v *InfoView) CreatedAt() time.Time { return v.Info.CreatedAt }

func databaseViews(records []*multidb.DatabaseInfo) []DatabaseInfoInterface {
	out := make([]DatabaseInfoInterface, len(records))
	for i, record := range records {
		out[i] = &InfoView{Info: record}
	}
	return out
}

// ListDatabases returns the manager's ordered visible database snapshots.
func (a Catalog) ListDatabases() []DatabaseInfoInterface {
	return databaseViews(a.Manager.ListDatabases())
}

// ListCompositeDatabases returns the same metadata contract for composites.
func (a Catalog) ListCompositeDatabases() []DatabaseInfoInterface {
	return databaseViews(a.Manager.ListCompositeDatabases())
}

// GetCompositeConstituents projects authoritative stored references without
// interpreting or disclosing their sealed connection descriptions.
func (a Catalog) GetCompositeConstituents(name string) ([]interface{}, error) {
	refs, err := a.Manager.GetCompositeConstituents(name)
	if err != nil {
		return nil, err
	}
	out := make([]interface{}, len(refs))
	for i, ref := range refs {
		out[i] = ref
	}
	return out, nil
}

// RemoteConstituentForDisplay delegates display projection to the manager that
// owns the connection descriptor. All protocols use the same redaction policy.
func (a Catalog) RemoteConstituentForDisplay(ref multidb.ConstituentRef) (multidb.ConstituentRef, error) {
	return a.Manager.RemoteConstituentForDisplay(ref)
}
