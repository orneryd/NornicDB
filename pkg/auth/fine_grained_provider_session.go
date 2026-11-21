package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// The standard session and external-provider session use separate signing
// domains. Legacy RBAC token validation uses the standard signing key.
// The provider header encodes {"alg":"HS256","typ":"NORNICDB-FGAC"}.
const fineGrainedProviderTokenHeader = "eyJhbGciOiJIUzI1NiIsInR5cCI6Ik5PUk5JQ0RCLUZHQUMifQ"

type fineGrainedProviderToken struct {
	JWTClaims
	Provider   string         `json:"provider"`
	ExternalID string         `json:"externalId"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// FineGrainedProviderSession is a verified, principal-bound provider session.
// Its attributes are available to authorization rules in the same session.
type FineGrainedProviderSession struct {
	provider   string
	externalID string
	userID     string
	expiresAt  int64
	attributes map[string]any
}

type fineGrainedProviderSessionKey struct{}

// WithContext carries a verified external identity to FGAC admission. A nil
// session leaves the context untouched.
func (session *FineGrainedProviderSession) WithContext(ctx context.Context) context.Context {
	if session == nil {
		return ctx
	}
	return context.WithValue(ctx, fineGrainedProviderSessionKey{}, session)
}

func (a *Authenticator) fineGrainedProviderSignature(message string) string {
	domain := hmac.New(sha256.New, a.config.JWTSecret)
	domain.Write([]byte("nornicdb-fgac-provider-session-v1"))
	return signedJWTMessage(domain.Sum(nil), message)
}

func (a *Authenticator) issueFineGrainedProviderToken(user *User, provider, subject string, attributes map[string]any, expiry time.Time) (string, error) {
	if len(a.config.JWTSecret) == 0 {
		return "", ErrMissingSecret
	}
	now := time.Now()
	if !expiry.After(now) {
		return "", ErrSessionExpired
	}
	body, err := jsontext.Marshal(fineGrainedProviderToken{
		JWTClaims: JWTClaims{Sub: user.ID, Username: user.Username, Email: user.Email, Iat: now.Unix(), Exp: expiry.Unix()},
		Provider:  provider, ExternalID: subject, Attributes: attributes,
	})
	if err != nil {
		return "", err
	}
	message := fineGrainedProviderTokenHeader + "." + base64.RawURLEncoding.EncodeToString(body)
	return message + "." + a.fineGrainedProviderSignature(message), nil
}

func (a *Authenticator) validateFineGrainedProviderToken(ctx context.Context, token string) (*FineGrainedProviderSession, *JWTClaims, error) {
	message, signature, ok := strings.Cut(token[len(fineGrainedProviderTokenHeader)+1:], ".")
	if !ok || len(a.config.JWTSecret) == 0 {
		return nil, nil, ErrInvalidToken
	}
	if !SecureCompare(signature, a.fineGrainedProviderSignature(fineGrainedProviderTokenHeader+"."+message)) {
		return nil, nil, ErrInvalidToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(message)
	if err != nil {
		return nil, nil, ErrInvalidToken
	}
	var claims fineGrainedProviderToken
	if jsontext.Unmarshal(raw, &claims) != nil || claims.Exp <= time.Now().Unix() {
		return nil, nil, ErrInvalidToken
	}
	session := &FineGrainedProviderSession{
		provider: claims.Provider, externalID: claims.ExternalID, userID: claims.Sub, expiresAt: claims.Exp, attributes: claims.Attributes,
	}
	if err := session.validate(ctx, a); err != nil {
		return nil, nil, err
	}
	current, err := a.CurrentClaimsContext(ctx, &claims.JWTClaims)
	if err != nil {
		return nil, nil, err
	}
	return session, current, nil
}

func (session *FineGrainedProviderSession) validate(ctx context.Context, directory *Authenticator) error {
	if session.expiresAt <= time.Now().Unix() {
		return ErrSessionExpired
	}
	user, err := directory.fineGrainedUserForProvider(ctx, session.provider, session.externalID)
	if err != nil {
		return err
	}
	if user.ID != session.userID {
		return ErrInvalidToken
	}
	return nil
}
