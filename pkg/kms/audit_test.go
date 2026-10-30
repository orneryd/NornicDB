package kms

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuditSigner_SignAndVerify(t *testing.T) {
	t.Parallel()
	signer := NewAuditSigner([]byte("audit-signing-key-32-bytes-123456"))
	event := AuditEvent{
		EventType: "KEY_GENERATED",
		KeyURI:    "kms://local/test",
		Principal: "unit-test",
		Timestamp: time.Unix(1700000000, 0).UTC(),
		Status:    "SUCCESS",
		Metadata:  map[string]interface{}{"version": 1},
	}

	signed, err := signer.Sign(event)
	require.NoError(t, err)
	require.NotEmpty(t, signed.Signature)
	require.True(t, signer.Verify(signed))

	signed.Status = "FAILURE"
	require.False(t, signer.Verify(signed))
}
