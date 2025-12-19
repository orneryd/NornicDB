// Package multidb provides multi-database support for NornicDB.
//
// This package implements Neo4j 4.x-style multi-database support, allowing
// multiple logical databases (tenants) to share a single physical storage backend
// while maintaining complete data isolation.
package multidb

import (
	"errors"
)

// Multi-database error types
var (
	ErrDatabaseNotFound        = errors.New("database not found")
	ErrDatabaseExists          = errors.New("database already exists")
	ErrInvalidDatabaseName     = errors.New("invalid database name")
	ErrMaxDatabasesReached     = errors.New("maximum number of databases reached")
	ErrCannotDropSystemDB      = errors.New("cannot drop system database")
	ErrCannotDropDefaultDB     = errors.New("cannot drop default database")
	ErrDatabaseOffline         = errors.New("database is offline")
	ErrAliasExists             = errors.New("alias already exists")
	ErrAliasNotFound           = errors.New("alias not found")
	ErrInvalidAliasName        = errors.New("invalid alias name")
	ErrAliasConflict           = errors.New("alias conflicts with existing database name")
	ErrDatabaseHasAliases      = errors.New("database has aliases - drop aliases first")
	ErrStorageLimitExceeded    = errors.New("storage limit exceeded")
	ErrQueryLimitExceeded      = errors.New("query limit exceeded")
	ErrConnectionLimitExceeded = errors.New("connection limit exceeded")
	ErrRateLimitExceeded       = errors.New("rate limit exceeded")
	ErrNotCompositeDatabase    = errors.New("database is not a composite database")
	ErrConstituentNotFound     = errors.New("constituent not found")
	ErrDuplicateConstituent    = errors.New("duplicate constituent alias")
)
