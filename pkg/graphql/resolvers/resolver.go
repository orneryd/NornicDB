package resolvers

import (
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

// Resolver is the root resolver for the GraphQL schema.
type Resolver struct {
	DB        *nornicdb.DB
	StartTime time.Time
}

// NewResolver creates a new resolver with the given database.
func NewResolver(db *nornicdb.DB) *Resolver {
	return &Resolver{
		DB:        db,
		StartTime: time.Now(),
	}
}
