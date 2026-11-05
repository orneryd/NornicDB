package auth

import (
	"fmt"
	"time"
)

// clusterTokenPurpose selects the public entry point's error and audit policies.
type clusterTokenPurpose int

const (
	clusterTokenPurposeDefault clusterTokenPurpose = iota
	clusterTokenPurposeExplicit
)

// issueClusterToken owns virtual-user construction, signing via issueUserJWT,
// error propagation, and the single successful audit event.
func (a *Authenticator) issueClusterToken(nodeID string, role Role, expiry time.Duration, purpose clusterTokenPurpose) (string, error) {
	user := &User{
		ID:       clusterSubjectPrefix + nodeID,
		Username: nodeID,
		Roles:    []Role{role},
	}

	token, err := a.issueUserJWT(user, expiry)
	if err != nil {
		if purpose == clusterTokenPurposeDefault {
			if err == ErrMissingSecret {
				return "", err
			}
			return "", fmt.Errorf("failed to generate cluster token: %w", err)
		}
		return "", err
	}

	var details string
	switch purpose {
	case clusterTokenPurposeDefault:
		details = fmt.Sprintf("cluster token generated for node %s with role %s", nodeID, role)
	case clusterTokenPurposeExplicit:
		details = fmt.Sprintf("cluster token generated for node %s with role %s, expiry=%v", nodeID, role, expiry)
	}

	a.logAudit(AuditEvent{
		Timestamp: time.Now(),
		EventType: "cluster_token_generated",
		Username:  nodeID,
		UserID:    user.ID,
		Success:   true,
		Details:   details,
	})

	return token, nil
}
