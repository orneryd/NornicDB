package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
)

func zz3bCreateAdmin(a *auth.Authenticator) error {
	_, err := a.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin})
	return err
}
