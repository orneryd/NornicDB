package observability_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// The oracle marks the union of matches in the ORIGINAL input. Sequential
// replacement, even longest-first, cannot implement offset-overlap semantics.
func credentialUnionOracle(input string, values []string) string {
	covered := make([]bool, len(input))
	for _, value := range values {
		if value == "" {
			continue
		}
		for start := 0; start < len(input); {
			i := strings.Index(input[start:], value)
			if i < 0 {
				break
			}
			i += start
			for j := i; j < i+len(value); j++ {
				covered[j] = true
			}
			start = i + 1
		}
	}
	var out strings.Builder
	for i := 0; i < len(input); i++ {
		if !covered[i] {
			out.WriteByte(input[i])
			continue
		}
		out.WriteString("<REDACTED>")
		for i+1 < len(input) && covered[i+1] {
			i++
		}
	}
	return out.String()
}

func escapedEveryByte(value string, lower bool) string {
	var out strings.Builder
	for i := range len(value) {
		fmt.Fprintf(&out, "%%%02X", value[i])
	}
	if lower {
		return strings.ToLower(out.String())
	}
	return out.String()
}

func credentialRepresentations(value string) []string {
	jsonValue, _ := json.Marshal(value)
	goValue := strconv.Quote(value)
	return []string{value, string(jsonValue[1 : len(jsonValue)-1]), goValue[1 : len(goValue)-1],
		url.QueryEscape(value), url.PathEscape(value), escapedEveryByte(value, false), escapedEveryByte(value, true),
		base64.StdEncoding.EncodeToString([]byte(value))}
}

func TestCredentialRepresentationOverlapDifferential(t *testing.T) {
	// Enumerate bytes, rather than selecting punctuation examples. net/url is
	// the acceptance oracle for encoded userinfo, including escaped separators,
	// arbitrary UTF-8 bytes, absent passwords and explicitly empty passwords.
	for b := 0; b <= 256; b++ {
		username := "synthetic-user-" + string([]byte{byte(b)}) + "-871429"
		if b == 256 {
			username = ""
		}
		for mode := 0; mode < 3; mode++ {
			password := ""
			user := url.User(username)
			if mode > 0 {
				if mode == 2 {
					password = "synthetic-password-317/:%@" + string([]byte{byte(b)})
				}
				user = url.UserPassword(username, password)
			}
			for spelling, encoded := range []string{user.String(), strings.ReplaceAll(user.String(), "%2F", "%2f"), strings.ToLower(user.String()), escapedEveryByte(username, false), escapedEveryByte(username, true)} {
				if spelling >= 3 && mode > 0 {
					encoded += ":" + url.QueryEscape(password)
				}
				endpoint := "https://" + encoded + "@example.invalid/"
				parsed, err := url.Parse(endpoint)
				if err != nil {
					t.Fatalf("generated userinfo rejected byte=%d mode=%d spelling=%d; capture withheld", b, mode, spelling)
				}
				decoded := parsed.User.Username()
				owned := observability.ClientCredentials(endpoint)
				secrets := []string{}
				if decoded != "" {
					secrets = append(secrets, decoded)
				}
				if pass, present := parsed.User.Password(); present && pass != "" {
					secrets = append(secrets, pass, decoded+":"+pass)
				}
				for _, secret := range secrets {
					for representation, reflected := range credentialRepresentations(secret) {
						// Prefix, suffix, containment, duplicate, and offset overlap,
						// in BOTH caller orders. The expected union is independent of
						// the implementation's credential/representation expansion.
						for overlap := 0; overlap < 5; overlap++ {
							other, text := reflected, reflected
							switch overlap {
							case 0:
								other = reflected[:len(reflected)/2]
							case 1:
								other = reflected[len(reflected)/2:]
							case 2:
								other = "left-" + reflected
								text = "left-" + reflected
							case 3:
								other = reflected[len(reflected)/2:] + "-right"
								text = reflected + "-right"
							case 4:
								other = reflected
							}
							for order := 0; order < 2; order++ {
								credentials := append([]string(nil), owned...)
								if order == 0 {
									credentials = append([]string{other}, credentials...)
								} else {
									credentials = append(credentials, other)
								}
								// The outer sentinels contain no generated credential.
								input := "[" + text + "]"
								oracleValues := append(credentialRepresentations(other), reflected)
								// Include every exact spelling of all owned values in the oracle.
								for _, value := range secrets {
									oracleValues = append(oracleValues, credentialRepresentations(value)...)
								}
								want := credentialUnionOracle(input, oracleValues)
								if got := observability.RedactText(input, credentials...); got != want {
									t.Fatalf("representation/overlap mismatch byte=%d mode=%d spelling=%d representation=%d overlap=%d order=%d; captures withheld", b, mode, spelling, representation, overlap, order)
								}
							}
						}
					}
				}
			}
		}
	}
}
