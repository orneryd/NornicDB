package testfixture

// EncryptionPropertyCase keeps a synthetic property shared by configuration
// classification checks and the real database encryption round-trip. Class is
// explicit, PHI or plain for the custom field-config test; empty omits that
// test's classification cell. RoundTrip selects the original database inputs.
type EncryptionPropertyCase struct {
	Name      string
	Value     any
	Class     string
	RoundTrip bool
}

// EncryptionPropertyCases returns fresh, synthetic property cases without
// retaining a caller's edits. For example, a database test builds its input
// from cases with RoundTrip set; configuration tests select nonempty Class.
func EncryptionPropertyCases() []EncryptionPropertyCase {
	return []EncryptionPropertyCase{
		{Name: "name", Value: "John Doe", Class: "plain", RoundTrip: true},
		{Name: "ssn", Value: "123-45-6789", Class: "PHI", RoundTrip: true},
		{Name: "social_security_number", Value: "987-65-4321", RoundTrip: true},
		{Name: "email", Value: "john@example.com", Class: "PHI", RoundTrip: true},
		{Name: "phone", Value: "555-123-4567", RoundTrip: true},
		{Name: "dob", Value: "1990-01-15", RoundTrip: true},
		{Name: "diagnosis", Value: "Type 2 Diabetes", RoundTrip: true},
		{Name: "credit_card", Value: "4111-1111-1111-1111", RoundTrip: true},
		{Name: "api_key", Value: "sk-secret-key-123", Class: "explicit", RoundTrip: true},
		{Name: "regular_field", Value: "not sensitive", RoundTrip: true},
		{Name: "count", Value: 42, RoundTrip: true},
		{Name: "secret", Value: "synthetic-secret", Class: "explicit"},
		{Name: "created_at", Value: "synthetic-created-at", Class: "plain"},
	}
}
